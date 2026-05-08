import importlib
import os
import queue
import re
import shlex
import threading
import time
import traceback
from bisect import bisect_right
from datetime import datetime

from cryptography.fernet import Fernet

try:
    import paramiko
except ImportError:
    os.system('pip install paramiko')
    try:
        import paramiko
    except ImportError:
        paramiko = None

from .setup import *


# ── 상수 ───────────────────────────────────────────────────────
GIB                  = 1024 * 1024 * 1024
DEFAULT_BATCH_GB     = 10
DEFAULT_CONCURRENT   = 2          # 동시에 도는 NAS rclone 수
DEFAULT_MTSTREAMS    = 4          # 파일 1개를 받을 때 multi-thread-streams 수
COPY_DONE_STATUS     = 'completed'
COPY_FAIL_PREFIX     = 'fail'
LOG_KEEP             = 300
LOG_RETURN_TAIL      = 100

# rclone --stats 1s 출력 파싱.
# "Transferred:  1.234 GiB / 5.000 GiB, 24%, 56.7 MiB/s, ETA 1m20s" 형태.
RCLONE_STATS_RE = re.compile(
    r'Transferred:\s+'
    r'([\d.]+\s*[KMGT]?i?B)\s*/\s*'
    r'([\d.]+\s*[KMGT]?i?B),\s*'
    r'(\d+)%'
    r'(?:,\s*([\d.]+\s*[KMGT]?i?B/s))?'
    r'(?:,\s*ETA\s+(\S+))?'
)


class ModuleMain(PluginModuleBase):

    def __init__(self, P):
        super(ModuleMain, self).__init__(P, name='main', first_menu='setting')
        self.db_default = {
            'main_nas_ip':           '',
            'main_nas_port':         '22',
            'main_nas_user':         '',
            'main_nas_password':     '',                       # Fernet 암호화 저장
            'main_encrypt_key':      '',                       # Fernet key
            'main_script_path':         '/volume1/MK/rclone_move_one.sh',
            'main_gdrive_remote':       'GDG:/Downloads',
            'main_max_batch_gb':        str(DEFAULT_BATCH_GB),    # 동시 in-flight 최대 합계 (GB)
            'main_concurrent_moves':    str(DEFAULT_CONCURRENT),  # 동시 NAS rclone 개수
            'main_multi_thread_streams':str(DEFAULT_MTSTREAMS),   # 파일당 stream 수
            'main_poll_interval':       '15',                     # 도착 polling 간격 (초)
            'main_copy_timeout':        '7200',                   # 파일별 도착 타임아웃 (초)
            'main_recursive':           'False',                  # 하위 폴더 재귀 처리
            'main_last_source_id':      '',                       # 마지막으로 실행한 폴더 ID
        }
        self._history_id  = None
        self._lock        = threading.Lock()
        self._is_running  = False
        self._stop_flag   = False
        self._logs        = []
        self._progress    = self._fresh_progress()
        self._ssh_client  = None
        self._ssh_lock    = threading.Lock()
        self._pipeline_cv = None

    # ── 초기 진행 상태 ────────────────────────────────────────────
    @staticmethod
    def _fresh_progress():
        return {
            'total':           0,    # 전체 파일 수
            'arrived':         0,    # share→my drive 도착 누적
            'moved':           0,    # my→nas 이동 완료 누적 (성공)
            'failed':          0,    # 실패 누적
            'in_flight':       [],   # 현재 처리 중인 파일명들
            'in_flight_bytes': 0,    # 현재 in-flight 합계 (디버그용)
            'concurrent':      0,    # 설정된 동시 mover 수
            'streams':         0,    # multi-thread-streams 설정값
            'phase':           '',
        }

    # ── 암호화 ────────────────────────────────────────────────────
    def _fernet(self):
        key = P.ModelSetting.get('main_encrypt_key')
        if not key:
            key = Fernet.generate_key().decode()
            P.ModelSetting.set('main_encrypt_key', key)
        return Fernet(key.encode())

    def _encrypt(self, plain):
        return self._fernet().encrypt(plain.encode()).decode()

    def _decrypt(self, enc):
        if not enc:
            return ''
        try:
            return self._fernet().decrypt(enc.encode()).decode()
        except Exception:
            return ''

    # ── 로그 ──────────────────────────────────────────────────────
    def _log(self, msg, level='INFO'):
        ts    = datetime.now().strftime('%H:%M:%S')
        entry = {'ts': ts, 'level': level, 'msg': msg}
        P.logger.info(f'[{level}] {msg}')
        with self._lock:
            self._logs.append(entry)
            if len(self._logs) > LOG_KEEP:
                self._logs = self._logs[-LOG_KEEP:]
        try:
            F.socketio.emit('gds_tool2_log', entry, namespace='/framework')
        except Exception:
            pass

    def _emit_progress(self):
        try:
            F.socketio.emit('gds_tool2_progress', dict(self._progress), namespace='/framework')
        except Exception:
            pass

    # ── 설정 저장 후 훅 (프레임워크가 호출) ──────────────────────
    # 폼 필드 tmp_main_nas_password 는 setting_save 단계에서 'tmp_' 접두사로
    # 자동 스킵된다. 여기서 직접 읽어 암호화한 뒤 main_nas_password 에 저장.
    def setting_save_after(self, change_list):
        try:
            from flask import request
            plain = (request.form.get('tmp_main_nas_password') or '').strip()
            if plain:
                P.ModelSetting.set('main_nas_password', self._encrypt(plain))
                P.logger.info('NAS password encrypted & saved')
        except Exception as e:
            P.logger.error(f'setting_save_after exception: {e}')
            P.logger.error(traceback.format_exc())

    # ── 인터럽트 가능한 sleep ─────────────────────────────────────
    def _interruptible_sleep(self, seconds):
        end = time.time() + seconds
        while time.time() < end:
            if self._stop_flag:
                return False
            time.sleep(min(1.0, end - time.time()))
        return True

    # ── SSH (영속 세션 재사용) ────────────────────────────────────
    def _ssh_alive(self):
        c = self._ssh_client
        if c is None:
            return False
        try:
            t = c.get_transport()
            return bool(t and t.is_active())
        except Exception:
            return False

    def _ssh_connect(self):
        if paramiko is None:
            raise RuntimeError('paramiko 자동 설치 실패. 수동으로 pip install paramiko 후 SJVA를 재시작하세요.')
        ip       = P.ModelSetting.get('main_nas_ip')
        port     = int(P.ModelSetting.get('main_nas_port') or 22)
        user     = P.ModelSetting.get('main_nas_user')
        password = self._decrypt(P.ModelSetting.get('main_nas_password'))
        if not ip or not user:
            raise RuntimeError('NAS 접속 정보(IP/User)가 비어있습니다.')

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(ip, port=port, username=user, password=password, timeout=10)
        try:
            t = client.get_transport()
            if t is not None:
                # NAS sshd 가 끊지 않게 keepalive
                t.set_keepalive(30)
        except Exception:
            pass
        self._ssh_client = client

    def _ssh_close(self):
        with self._ssh_lock:
            c = self._ssh_client
            self._ssh_client = None
        if c is not None:
            try:
                c.close()
            except Exception:
                pass

    def _ssh_exec(self, command, timeout=600):
        # paramiko Transport는 동시에 여러 채널을 띄울 수 있어서
        # exec_command 자체는 락 없이 병렬 호출 가능. 락은 connect/reconnect만.
        self._ensure_ssh()
        try:
            return self._ssh_run(command, timeout)
        except (paramiko.SSHException, OSError, EOFError) as e:
            P.logger.warning(f'SSH 끊김 감지({e}). 재연결 후 재시도.')
            with self._ssh_lock:
                if not self._ssh_alive():
                    try:
                        if self._ssh_client is not None:
                            self._ssh_client.close()
                    except Exception:
                        pass
                    self._ssh_client = None
                    self._ssh_connect()
            return self._ssh_run(command, timeout)

    def _ensure_ssh(self):
        if self._ssh_alive():
            return
        with self._ssh_lock:
            if not self._ssh_alive():
                self._ssh_connect()

    def _ssh_run(self, command, timeout):
        client = self._ssh_client
        stdin, stdout, stderr = client.exec_command(command, timeout=timeout)
        exit_code = stdout.channel.recv_exit_status()
        out = stdout.read().decode(errors='replace').strip()
        err = stderr.read().decode(errors='replace').strip()
        return exit_code, out, err

    def _test_ssh(self):
        try:
            code, out, _ = self._ssh_exec('echo ok', timeout=10)
            return code == 0 and 'ok' in out
        except Exception as e:
            self._log(f'SSH 연결 실패: {e}', 'ERROR')
            return False

    # ── gds_tool 인스턴스 / 모델 ──────────────────────────────────
    def _get_gds(self):
        gds = F.PluginManager.get_plugin_instance('gds_tool')
        if gds is None:
            raise RuntimeError('gds_tool 플러그인이 설치/로드되지 않았습니다.')
        return gds

    def _get_request_model(self):
        # gds_tool setup.py 가 P.ModelRequestItem 으로 노출함.
        # 미노출 환경(구버전) 대비해 importlib 폴백.
        try:
            gds = F.PluginManager.get_plugin_instance('gds_tool')
            if gds is not None and hasattr(gds, 'ModelRequestItem'):
                return gds.ModelRequestItem
        except Exception:
            pass
        try:
            mod = importlib.import_module('gds_tool.mod_request')
            return mod.ModelRequestItem
        except Exception as e:
            self._log(f'ModelRequestItem 접근 실패 (lsjson 단독): {e}', 'WARN')
            return None

    # ── 공유드라이브 파일 목록 ────────────────────────────────────
    def _get_file_list(self, folder_id):
        try:
            gds       = self._get_gds()
            remote    = 'worker:{%s}' % folder_id
            recursive = (P.ModelSetting.get('main_recursive') or 'False') == 'True'

            try:
                if recursive:
                    result = gds.SupportRcloneWorker.lsjson(remote, option=['-R', '--files-only'])
                else:
                    result = gds.SupportRcloneWorker.lsjson(remote)
            except TypeError:
                # 구버전 SupportRcloneWorker.lsjson(remote)만 받는 경우 폴백
                result = gds.SupportRcloneWorker.lsjson(remote)
                if recursive:
                    self._log('SupportRcloneWorker.lsjson이 option 인자 미지원. 비재귀로 진행', 'WARN')
                    recursive = False

            if not result:
                self._log('lsjson 결과 없음 (폴더 비어있거나 접근 불가)', 'WARN')
                return []
            files = [
                f for f in result
                if not f.get('IsDir', False)
            ]

            # 동일 Name 충돌 감지 (재귀 시 서로 다른 하위폴더에 같은 파일명)
            if recursive:
                names = {}
                for f in files:
                    names.setdefault(f.get('Name'), []).append(f.get('Path', f.get('Name')))
                dups = {n: ps for n, ps in names.items() if len(ps) > 1}
                if dups:
                    self._log(
                        f'⚠ 재귀 모드: 동일 파일명 {len(dups)}건 충돌 — '
                        f'내 드라이브에서 덮어쓰일 수 있음',
                        'WARN',
                    )
                    for n, ps in list(dups.items())[:5]:
                        self._log(f'  - {n}: {ps}', 'WARN')

            mode = '재귀' if recursive else '단일 폴더'
            self._log(f'[{mode}] 전체 {len(result)}개 항목 중 파일 {len(files)}개')
            return files
        except Exception as e:
            self._log(f'파일 목록 오류: {e}', 'ERROR')
            P.logger.error(traceback.format_exc())
            return []

    # ── 이전 완료 기록 스킵 ───────────────────────────────────────
    def _filter_skip_completed(self, files):
        Model = self._get_request_model()
        if Model is None:
            return files
        kept    = []
        skipped = []
        for f in files:
            sid = f.get('ID')
            try:
                existing = Model.get_by_source_id(sid) if sid else None
            except Exception:
                existing = None
            if existing is not None and (existing.status or '') == COPY_DONE_STATUS:
                skipped.append((f.get('Name', '?'), existing.id))
            else:
                kept.append(f)
        if skipped:
            self._log(f'⏭ 이전 완료 기록으로 {len(skipped)}개 스킵')
            for name, eid in skipped[:10]:
                self._log(f'  · {name} (gds_tool id={eid})')
            if len(skipped) > 10:
                self._log(f'  · ... 외 {len(skipped) - 10}건')
        return kept

    # ── 배치 워커 (스레드) ────────────────────────────────────────
    def _batch_worker(self, folder_id, selected_ids=None):
        final_status = 'completed'
        final_note   = ''
        try:
            with self._lock:
                self._is_running = True
                self._stop_flag  = False
                self._logs       = []
                self._progress   = self._fresh_progress()

            try:
                max_gb = float(P.ModelSetting.get('main_max_batch_gb') or DEFAULT_BATCH_GB)
            except ValueError:
                max_gb = DEFAULT_BATCH_GB
            max_bytes = int(max_gb * GIB)
            try:
                n_consumers = max(1, int(P.ModelSetting.get('main_concurrent_moves') or DEFAULT_CONCURRENT))
            except ValueError:
                n_consumers = DEFAULT_CONCURRENT
            try:
                n_streams = max(1, int(P.ModelSetting.get('main_multi_thread_streams') or DEFAULT_MTSTREAMS))
            except ValueError:
                n_streams = DEFAULT_MTSTREAMS

            self._history_id = self._history_create(folder_id, max_gb)

            self._log('SSH 연결 테스트...')
            if not self._test_ssh():
                self._log('SSH 연결 실패. 중단.', 'ERROR')
                final_status = 'error'
                final_note   = 'SSH 연결 실패'
                return

            self._log(f'공유드라이브 파일 목록 추출: {folder_id}')
            files = self._get_file_list(folder_id)
            if not files:
                self._log('처리할 파일이 없습니다.')
                final_status = 'completed'
                final_note   = '파일 없음'
                return

            files = self._filter_skip_completed(files)
            if not files:
                self._log('모든 파일이 이미 완료 기록 있음. 처리할 항목 없음.')
                final_status = 'completed'
                final_note   = '전부 스킵'
                return

            if selected_ids:
                wanted = {s.strip() for s in selected_ids.split(',') if s.strip()}
                before = len(files)
                files  = [f for f in files if f.get('ID') in wanted]
                self._log(f'사용자 선택: {len(files)}개 (전체 가능 {before}개 중)')
                if not files:
                    self._log('선택한 파일이 모두 필터링됨. 처리할 항목 없음.', 'WARN')
                    final_status = 'completed'
                    final_note   = '선택 0개'
                    return

            self._progress['total']      = len(files)
            self._progress['concurrent'] = n_consumers
            self._progress['streams']    = n_streams
            self._progress['phase']      = 'pipeline'
            self._emit_progress()
            self._history_update(total_files=len(files), total_batches=0)

            self._log(
                f'파이프라인: 총 {len(files)}개 파일 / capa ≤ {max_gb:g} GB / '
                f'동시 mover {n_consumers}개 / streams={n_streams}'
            )

            self._pipeline_run(files, max_bytes, n_consumers, n_streams)

            if self._stop_flag:
                final_status = 'stopped'
            elif self._progress['failed'] > 0:
                final_note = f'{self._progress["failed"]}개 실패 포함'

            self._log(
                f'전체 종료 — 성공 {self._progress["moved"]}, '
                f'실패 {self._progress["failed"]} / 총 {self._progress["total"]}'
            )

        except Exception as e:
            self._log(f'예외: {e}', 'ERROR')
            P.logger.error(traceback.format_exc())
            final_status = 'error'
            final_note   = str(e)[:200]
        finally:
            self._history_finalize(final_status, final_note)
            with self._lock:
                self._is_running = False
            self._progress['phase']     = ''
            self._progress['in_flight'] = []
            self._ssh_close()
            try:
                F.socketio.emit('gds_tool2_done', dict(self._progress), namespace='/framework')
            except Exception:
                pass

    # ── 파이프라인 (Producer + Watcher + Consumer N) ──────────────
    def _pipeline_run(self, files, max_bytes, n_consumers, n_streams):
        from support.expand.rclone import SupportRclone

        cv             = threading.Condition()
        in_flight_b    = [0]                          # 현재 in-flight 합계
        pending_lock   = threading.Lock()
        pending        = {}                           # name -> (file, size, req_id, started_at)
        ready_q        = queue.PriorityQueue()        # (size, seq, (f, size)) — 작은 거 우선
        ready_seq      = [0]                          # tie-breaker (PriorityQueue는 size 동률 시 다음 키 비교)
        done_issue     = threading.Event()
        done_watch     = threading.Event()
        self._pipeline_cv = cv                        # stop 시 외부에서 깨우려고 노출

        gds      = self._get_gds()
        gdrive   = P.ModelSetting.get('main_gdrive_remote')
        Model    = self._get_request_model()
        interval = max(int(P.ModelSetting.get('main_poll_interval') or 15), 3)
        timeout  = int(P.ModelSetting.get('main_copy_timeout') or 7200)

        def add_inflight(name, size):
            with self._lock:
                self._progress['in_flight'].append({
                    'name':     name,
                    'size_gb':  size / GIB,
                    'phase':    'copy',     # copy → arrived → move
                    'percent':  0,
                    'speed':    '',
                    'eta':      '',
                })
                self._progress['in_flight_bytes'] = in_flight_b[0]
            self._emit_progress()

        def update_inflight(name, **fields):
            with self._lock:
                for entry in self._progress['in_flight']:
                    if entry['name'] == name:
                        entry.update(fields)
                        break
            self._emit_progress()

        def remove_inflight(name):
            with self._lock:
                self._progress['in_flight'] = [
                    e for e in self._progress['in_flight'] if e['name'] != name
                ]
                self._progress['in_flight_bytes'] = in_flight_b[0]
            self._emit_progress()

        def release_capacity(size):
            with cv:
                in_flight_b[0] -= size
                cv.notify_all()

        # ──── Producer (best-fit: 사이즈 기준 정렬 + bisect로 O(log n) 선택) ────
        # files 의 size 정보는 _get_file_list 시점에 lsjson 으로 이미 받아둠.
        # 그걸 size 오름차순으로 정렬해두면, 매 라운드마다 가용 용량 이하의
        # 가장 큰 파일을 bisect 로 한 번에 찾을 수 있다 (스캔 X).
        def producer():
            items = sorted(
                [((f.get('Size', 0) or 0), f) for f in files],
                key=lambda x: x[0],
            )
            sizes = [it[0] for it in items]
            last_wait_log = 0.0

            try:
                while items:
                    if self._stop_flag:
                        break

                    with cv:
                        available = max_bytes - in_flight_b[0]
                        idx = bisect_right(sizes, available) - 1

                        if idx < 0:
                            # 가용 용량 < 모든 잔여 파일
                            if in_flight_b[0] == 0:
                                # 단일 파일이 한도 초과 → 어쩔 수 없이 단독 처리
                                idx = 0     # 가장 작은 것
                                self._log(
                                    f'  ⚠ 한도 초과 단독 처리: {items[0][1].get("Name")} '
                                    f'({items[0][0]/GIB:.2f} GB > 한도 {max_bytes/GIB:.2f} GB)',
                                    'WARN',
                                )
                            else:
                                now = time.time()
                                if now - last_wait_log > 10:
                                    self._log(
                                        f'  · capa 대기: 남은 {len(items)}개 모두 한도 초과 '
                                        f'(in-flight {in_flight_b[0]/GIB:.2f} GB, '
                                        f'최소 {sizes[0]/GIB:.2f} GB 필요)'
                                    )
                                    last_wait_log = now
                                # capacity 변화는 consumer 의 release_capacity → cv.notify_all 또는
                                # stop 명령 시 외부에서 깨우는 경로뿐이므로 polling 안 함.
                                cv.wait()
                                continue

                        last_wait_log = 0
                        size, f = items.pop(idx)
                        sizes.pop(idx)
                        in_flight_b[0] += size

                    name = f.get('Name', '?')
                    add_inflight(name, size)

                    try:
                        ret = gds.add_copy(
                            source_id     = f['ID'],
                            folder_name   = '',
                            board_type    = 'direct',
                            category_type = '',
                            size          = size,
                            count         = 1,
                            copy_type     = 'folder',
                            remote_path   = gdrive,
                        ) or {}
                    except Exception as e:
                        self._log(f'  ✗ add_copy 예외: {name}: {e}', 'ERROR')
                        self._progress['failed'] += 1
                        release_capacity(size)
                        remove_inflight(name)
                        continue

                    status = ret.get('ret', 'fail')
                    req_id = ret.get('request_db_id')
                    if status not in ('success', 'already'):
                        self._log(f'  ✗ add_copy 실패 ({status}): {name} / {ret}', 'ERROR')
                        self._progress['failed'] += 1
                        release_capacity(size)
                        remove_inflight(name)
                        continue

                    with pending_lock:
                        pending[name] = (f, size, req_id, time.time())
                    self._log(
                        f'  → 복사 요청: {name} '
                        f'({size/GIB:.2f} GB, id={req_id}, ret={status})'
                    )
            except Exception as e:
                self._log(f'producer 예외: {e}', 'ERROR')
                P.logger.error(traceback.format_exc())
            finally:
                done_issue.set()

        # ──── Watcher ────
        def watcher():
            try:
                while True:
                    if self._stop_flag:
                        return
                    with pending_lock:
                        empty = (len(pending) == 0)
                    if done_issue.is_set() and empty:
                        return

                    try:
                        drive_files = SupportRclone.lsjson(gdrive) or []
                        by_name = {
                            df.get('Name'): (df.get('Size', 0) or 0)
                            for df in drive_files
                            if not df.get('IsDir', False)
                        }
                        lsjson_err = None
                    except Exception as e:
                        by_name    = None
                        lsjson_err = str(e)

                    arrived = []
                    fails   = []
                    timeouts = []
                    with pending_lock:
                        items = list(pending.items())
                    for name, (f, size, req_id, started) in items:
                        if by_name is not None:
                            actual = by_name.get(name)
                            if actual is not None and (size <= 0 or actual >= size):
                                arrived.append((name, f, size))
                                continue
                        if Model and req_id:
                            try:
                                rec = Model.get_by_id(int(req_id))
                                if rec and (rec.status or '').startswith(COPY_FAIL_PREFIX):
                                    fails.append((name, size, rec.status))
                                    continue
                            except Exception:
                                pass
                        if time.time() - started > timeout:
                            timeouts.append((name, size))

                    for name, f, size in arrived:
                        with pending_lock:
                            pending.pop(name, None)
                        self._progress['arrived'] += 1
                        update_inflight(name, phase='arrived', percent=100)
                        self._log(f'  ✓ 도착: {name}')
                        ready_seq[0] += 1
                        ready_q.put((size, ready_seq[0], (f, size)))

                    for name, size, st in fails:
                        with pending_lock:
                            pending.pop(name, None)
                        self._progress['failed'] += 1
                        release_capacity(size)
                        remove_inflight(name)
                        self._log(f'  ✗ 복사 실패 ({st}): {name}', 'ERROR')

                    for name, size in timeouts:
                        with pending_lock:
                            pending.pop(name, None)
                        self._progress['failed'] += 1
                        release_capacity(size)
                        remove_inflight(name)
                        self._log(f'  ✗ 도착 타임아웃: {name}', 'ERROR')

                    if lsjson_err is not None:
                        self._log(f'lsjson 오류 (재시도): {lsjson_err}', 'WARN')

                    if not self._interruptible_sleep(interval):
                        return
            except Exception as e:
                self._log(f'watcher 예외: {e}', 'ERROR')
                P.logger.error(traceback.format_exc())
            finally:
                done_watch.set()

        # ──── Consumer ────
        def consumer(idx):
            while True:
                if self._stop_flag:
                    return
                if done_issue.is_set() and done_watch.is_set() and ready_q.empty():
                    return
                try:
                    _, _, item = ready_q.get(timeout=1)
                except queue.Empty:
                    continue
                f, size = item
                name = f.get('Name', '?')
                update_inflight(name, phase='move', percent=0, speed='', eta='')
                try:
                    ok = self._run_nas_move_one(
                        name, n_streams,
                        progress_cb=lambda pct, sp, eta_, _n=name: update_inflight(
                            _n, percent=pct, speed=sp, eta=eta_
                        ),
                    )
                    if ok:
                        self._progress['moved'] += 1
                    else:
                        self._progress['failed'] += 1
                except Exception as e:
                    self._log(f'  ✗ NAS 이동 예외: {name}: {e}', 'ERROR')
                    self._progress['failed'] += 1
                release_capacity(size)
                remove_inflight(name)

        prod_t  = threading.Thread(target=producer, name='gds2-prod', daemon=True)
        watch_t = threading.Thread(target=watcher,  name='gds2-watch', daemon=True)
        cons_ts = [
            threading.Thread(target=consumer, args=(i,), name=f'gds2-cons-{i}', daemon=True)
            for i in range(n_consumers)
        ]

        prod_t.start()
        watch_t.start()
        for t in cons_ts:
            t.start()

        prod_t.join()
        watch_t.join()
        with cv:
            cv.notify_all()           # 깨워서 종료 검사하게
        for t in cons_ts:
            t.join()
        self._pipeline_cv = None

    # ── NAS 단일 파일 이동 ────────────────────────────────────────
    def _run_nas_move_one(self, name, streams, progress_cb=None):
        script = P.ModelSetting.get('main_script_path')
        # rclone --stats 1s 출력은 stderr 로 가므로 2>&1 로 stdout 에 합침.
        cmd = f'bash {shlex.quote(script)} {shlex.quote(name)} {int(streams)} 2>&1'
        self._log(f'  → NAS 이동 시작: {name} (streams={streams})')
        try:
            self._ensure_ssh()
        except Exception as e:
            self._log(f'  ✗ SSH 연결 실패: {name}: {e}', 'ERROR')
            return False
        try:
            return self._stream_move(name, cmd, progress_cb)
        except (paramiko.SSHException, OSError, EOFError) as e:
            P.logger.warning(f'SSH 끊김({e}). 재연결 후 재시도.')
            try:
                with self._ssh_lock:
                    if not self._ssh_alive():
                        try:
                            if self._ssh_client is not None:
                                self._ssh_client.close()
                        except Exception:
                            pass
                        self._ssh_client = None
                        self._ssh_connect()
                return self._stream_move(name, cmd, progress_cb)
            except Exception as e2:
                self._log(f'  ✗ SSH 재시도 실패: {name}: {e2}', 'ERROR')
                return False
        except Exception as e:
            self._log(f'  ✗ 이동 예외: {name}: {e}', 'ERROR')
            return False

    def _stream_move(self, name, cmd, progress_cb):
        client = self._ssh_client
        stdin, stdout, stderr = client.exec_command(cmd, timeout=14400)

        last_pct = -1
        last_pct_log = -10              # 진행 로그 throttle (10% 단위)
        tail_buf = []                   # 실패 시 마지막 출력 보존

        try:
            for raw in iter(stdout.readline, ''):
                if not raw:
                    break
                line = raw.rstrip()
                if not line:
                    continue
                tail_buf.append(line)
                if len(tail_buf) > 30:
                    tail_buf = tail_buf[-30:]

                m = RCLONE_STATS_RE.search(line)
                if m:
                    pct   = int(m.group(3))
                    speed = (m.group(4) or '').replace(' ', '')
                    eta   = m.group(5) or ''
                    if progress_cb is not None:
                        try:
                            progress_cb(pct, speed, eta)
                        except Exception:
                            pass
                    if pct >= last_pct_log + 10:
                        self._log(f'  · {name}: {pct}% ({speed}, ETA {eta})')
                        last_pct_log = pct - (pct % 10)
                    last_pct = pct
        except Exception:
            raise

        exit_code = stdout.channel.recv_exit_status()
        if exit_code == 0:
            self._log(f'  ✓ NAS 이동 완료: {name}')
            return True
        tail = '\n'.join(tail_buf[-5:])
        self._log(f'  ✗ NAS 이동 실패 ({exit_code}): {name}: {tail[:300]}', 'ERROR')
        return False

    def plugin_unload(self):
        self._ssh_close()

    # ── 이력 ──────────────────────────────────────────────────────
    def _history_model(self):
        try:
            return getattr(P, 'ModelBatchHistory', None)
        except Exception:
            return None

    def _history_create(self, folder_id, max_gb):
        Model = self._history_model()
        if Model is None:
            return None
        try:
            item = Model(folder_id=folder_id, max_batch_gb=str(max_gb))
            item.save()
            return item.id
        except Exception as e:
            P.logger.error(f'history create exception: {e}')
            return None

    def _history_update(self, **fields):
        Model = self._history_model()
        if Model is None or self._history_id is None:
            return
        try:
            item = Model.get_by_id(self._history_id)
            if item is None:
                return
            for k, v in fields.items():
                setattr(item, k, v)
            item.save()
        except Exception as e:
            P.logger.error(f'history update exception: {e}')

    def _history_finalize(self, status, note):
        Model = self._history_model()
        if Model is None or self._history_id is None:
            return
        try:
            item = Model.get_by_id(self._history_id)
            if item is None:
                return
            item.finished_time  = datetime.now()
            item.success_count  = self._progress.get('moved', 0)
            item.fail_count     = self._progress.get('failed', 0)
            item.status         = status
            if note:
                item.note = note
            item.save()
        except Exception as e:
            P.logger.error(f'history finalize exception: {e}')

    # ── SJVA command 핸들러 ───────────────────────────────────────
    # 설정 저장은 프레임워크의 globalSettingSaveBtn 이 처리한다.
    # 여기서는 커스텀 명령(SSH 테스트, 미리보기, 배치 시작/중단, 상태 조회)만 다룸.
    def process_command(self, command, arg1, arg2, arg3, req):
        ret = {'ret': 'success'}
        try:
            if command == 'test_ssh':
                ok = self._test_ssh()
                if not self._is_running:
                    self._ssh_close()    # 단발 테스트는 세션 남기지 않음
                if ok:
                    ret['msg'] = 'SSH 연결 성공!'
                else:
                    ret['ret'] = 'error'
                    ret['msg'] = 'SSH 연결 실패. IP/포트/계정/비밀번호 확인하세요.'

            elif command == 'preview_files':
                fid = (arg1 or '').strip()
                if not fid:
                    ret['ret'] = 'error'
                    ret['msg'] = '폴더 ID를 입력하세요.'
                else:
                    if arg2 in ('0', '1'):
                        P.ModelSetting.set('main_recursive', 'True' if arg2 == '1' else 'False')
                    files = self._get_file_list(fid)
                    files = self._filter_skip_completed(files)
                    try:
                        max_gb = float(P.ModelSetting.get('main_max_batch_gb') or DEFAULT_BATCH_GB)
                    except ValueError:
                        max_gb = DEFAULT_BATCH_GB
                    max_count = MAX_BATCH_FILES
                    batches = self._pack_batches(files, int(max_gb * GIB), max_count)
                    ret['files']         = files
                    ret['batches_count'] = len(batches)
                    ret['total_size']    = sum((f.get('Size', 0) or 0) for f in files)
                    cap_desc = f'≤{max_gb:g} GB' + (f', ≤{max_count}개' if max_count > 0 else '')
                    ret['msg'] = (
                        f'{len(files)}개 파일 / '
                        f'{ret["total_size"]/GIB:.2f} GB / '
                        f'{len(batches)}개 배치 (배치당 {cap_desc})'
                    )

            elif command == 'start_batch':
                fid = (arg1 or '').strip()
                selected_ids = (arg3 or '').strip()
                if not fid:
                    ret['ret'] = 'error'
                    ret['msg'] = '폴더 ID를 입력하세요.'
                else:
                    with self._lock:
                        if self._is_running:
                            ret['ret'] = 'error'
                            ret['msg'] = '이미 실행 중입니다.'
                        else:
                            self._is_running = True
                            self._stop_flag  = False
                    if ret['ret'] == 'success':
                        try:
                            P.ModelSetting.set('main_last_source_id', fid)
                            if arg2 in ('0', '1'):
                                P.ModelSetting.set('main_recursive', 'True' if arg2 == '1' else 'False')
                        except Exception:
                            pass
                        t = threading.Thread(
                            target=self._batch_worker,
                            args=(fid, selected_ids or None),
                            daemon=True,
                        )
                        t.start()
                        ret['msg'] = '배치 시작!'

            elif command == 'stop_batch':
                if self._is_running:
                    self._stop_flag = True
                    cv = self._pipeline_cv
                    if cv is not None:
                        with cv:
                            cv.notify_all()           # capacity 대기 중인 producer 깨우기
                    ret['msg'] = '중단 요청. 다음 안전 지점에서 정지합니다.'
                else:
                    ret['msg'] = '실행 중인 배치 없음.'

            elif command == 'get_status':
                ret['is_running'] = self._is_running
                ret['progress']   = dict(self._progress)
                with self._lock:
                    ret['logs']   = self._logs[-LOG_RETURN_TAIL:]

            elif command == 'list_history':
                Model = self._history_model()
                if Model is None:
                    ret['list'] = []
                else:
                    try:
                        items = Model.get_list() or []
                        items.sort(key=lambda x: x.id, reverse=True)
                        ret['list'] = [it.as_dict() for it in items[:200]]
                    except Exception as e:
                        ret['ret'] = 'error'
                        ret['msg'] = f'이력 조회 실패: {e}'
                        ret['list'] = []

            elif command == 'delete_history':
                Model = self._history_model()
                if Model is None or not arg1:
                    ret['ret'] = 'error'
                    ret['msg'] = '삭제 대상 없음'
                else:
                    if Model.delete_by_id(arg1):
                        ret['msg'] = '삭제 완료'
                    else:
                        ret['ret'] = 'error'
                        ret['msg'] = '삭제 실패'

            elif command == 'clear_history':
                Model = self._history_model()
                if Model is None:
                    ret['ret'] = 'error'
                    ret['msg'] = '모델 없음'
                else:
                    n = Model.delete_all(0)
                    ret['msg'] = f'{n}건 삭제'

            elif command == 'list_bookmarks':
                Model = getattr(P, 'ModelSourceBookmark', None)
                if Model is None:
                    ret['list'] = []
                else:
                    try:
                        items = Model.get_list() or []
                        items.sort(key=lambda x: x.id, reverse=True)
                        ret['list'] = [it.as_dict() for it in items]
                    except Exception as e:
                        ret['list'] = []
                        ret['ret'] = 'error'
                        ret['msg'] = f'즐겨찾기 조회 실패: {e}'

            elif command == 'add_bookmark':
                name = (arg1 or '').strip()
                sid  = (arg2 or '').strip()
                Model = getattr(P, 'ModelSourceBookmark', None)
                if not name or not sid:
                    ret['ret'] = 'error'
                    ret['msg'] = '이름과 ID가 모두 필요합니다.'
                elif Model is None:
                    ret['ret'] = 'error'
                    ret['msg'] = '모델 없음'
                else:
                    item = Model(name=name, source_id=sid)
                    item.save()
                    ret['msg'] = f'저장: {name}'

            elif command == 'delete_bookmark':
                bid = (arg1 or '').strip()
                Model = getattr(P, 'ModelSourceBookmark', None)
                if Model is None or not bid:
                    ret['ret'] = 'error'
                    ret['msg'] = '삭제 대상 없음'
                else:
                    if Model.delete_by_id(bid):
                        ret['msg'] = '삭제 완료'
                    else:
                        ret['ret'] = 'error'
                        ret['msg'] = '삭제 실패'

        except Exception as e:
            ret['ret'] = 'error'
            ret['msg'] = str(e)
            P.logger.error(traceback.format_exc())

        return jsonify(ret)
