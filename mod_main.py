import importlib
import os
import threading
import time
import traceback
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
MAX_BATCH_FILES      = 4         # NAS rclone_move.sh가 한 번에 처리하는 파일 수
COPY_DONE_STATUS     = 'completed'
COPY_FAIL_PREFIX     = 'fail'
LOG_KEEP             = 300
LOG_RETURN_TAIL      = 100


class ModuleMain(PluginModuleBase):

    def __init__(self, P):
        super(ModuleMain, self).__init__(P, name='main', first_menu='setting')
        self.db_default = {
            'main_nas_ip':           '',
            'main_nas_port':         '22',
            'main_nas_user':         '',
            'main_nas_password':     '',                       # Fernet 암호화 저장
            'main_encrypt_key':      '',                       # Fernet key
            'main_script_path':      '/volume1/MK/rclone_move.sh',
            'main_gdrive_remote':    'GDG:/Downloads',
            'main_max_batch_gb':     str(DEFAULT_BATCH_GB),    # 배치당 최대 용량 (GB)
            'main_poll_interval':    '15',                     # status polling 간격 (초)
            'main_copy_timeout':     '7200',                   # 배치 복사 타임아웃 (초)
            'main_recursive':        'False',                  # 하위 폴더 재귀 처리
            'main_last_source_id':   '',                       # 마지막으로 실행한 폴더 ID
        }
        self._history_id  = None
        self._lock        = threading.Lock()
        self._is_running  = False
        self._stop_flag   = False
        self._logs        = []
        self._progress    = self._fresh_progress()

    # ── 초기 진행 상태 ────────────────────────────────────────────
    @staticmethod
    def _fresh_progress():
        return {
            'total_files':   0,
            'total_batches': 0,
            'batch_index':   0,   # 1-based 현재 배치
            'batch_files':   0,   # 현재 배치 파일 수
            'batch_done':    0,   # 현재 배치 완료
            'batch_failed':  0,   # 현재 배치 실패
            'overall_done':  0,   # 전체 누적 완료
            'overall_failed':0,
            'current':       '',
            'phase':         '',  # 'copy' | 'nas' | 'wait'
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

    # ── SSH ───────────────────────────────────────────────────────
    def _ssh_exec(self, command, timeout=600):
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
        try:
            client.connect(ip, port=port, username=user, password=password, timeout=10)
            stdin, stdout, stderr = client.exec_command(command, timeout=timeout)
            exit_code = stdout.channel.recv_exit_status()
            out = stdout.read().decode(errors='replace').strip()
            err = stderr.read().decode(errors='replace').strip()
            return exit_code, out, err
        finally:
            try:
                client.close()
            except Exception:
                pass

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
                and (f.get('MimeType') or '').startswith('video/')
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
            self._log(f'[{mode}] 전체 {len(result)}개 항목 중 비디오 {len(files)}개')
            return files
        except Exception as e:
            self._log(f'파일 목록 오류: {e}', 'ERROR')
            P.logger.error(traceback.format_exc())
            return []

    # ── 배치 그룹핑 (greedy first-fit) ────────────────────────────
    @staticmethod
    def _pack_batches(files, max_bytes, max_count=0):
        """파일 리스트를 배치로 묶는다.
        - 단일 파일이 max_bytes를 초과하면 단독 배치.
        - 그 외는 합이 max_bytes를 넘지 않고 개수가 max_count를 넘지 않게 채움.
        - max_count <= 0 이면 개수 제한 없음.
        """
        batches = []
        current = []
        current_size = 0
        for f in files:
            size = f.get('Size', 0) or 0
            if size > max_bytes:
                if current:
                    batches.append(current)
                    current, current_size = [], 0
                batches.append([f])  # 단독
                continue
            count_full = max_count > 0 and len(current) >= max_count
            size_full  = current_size + size > max_bytes
            if (count_full or size_full) and current:
                batches.append(current)
                current, current_size = [], 0
            current.append(f)
            current_size += size
        if current:
            batches.append(current)
        return batches

    # ── 배치 워커 (스레드) ────────────────────────────────────────
    def _batch_worker(self, folder_id):
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
            max_count = MAX_BATCH_FILES
            max_bytes = int(max_gb * GIB)

            self._history_id = self._history_create(folder_id, max_gb)

            self._log('SSH 연결 테스트...')
            if not self._test_ssh():
                self._log('SSH 연결 실패. 배치 중단.', 'ERROR')
                final_status = 'error'
                final_note   = 'SSH 연결 실패'
                return

            self._log(f'공유드라이브 파일 목록 추출: {folder_id}')
            files = self._get_file_list(folder_id)
            if not files:
                self._log('처리할 비디오 파일이 없습니다.')
                final_status = 'completed'
                final_note   = '비디오 없음'
                return

            batches = self._pack_batches(files, max_bytes, max_count)
            total_files   = sum(len(b) for b in batches)
            total_batches = len(batches)

            self._progress['total_files']   = total_files
            self._progress['total_batches'] = total_batches
            self._emit_progress()
            self._history_update(
                total_files=total_files,
                total_batches=total_batches,
            )

            cap_desc = f'≤{max_gb:g} GB' + (f', ≤{max_count}개' if max_count > 0 else '')
            self._log(
                f'총 {total_files}개 파일 → {total_batches}개 배치 '
                f'(배치당 {cap_desc})'
            )

            for bi, batch in enumerate(batches, start=1):
                if self._stop_flag:
                    self._log('사용자 중단 (배치 시작 전)')
                    final_status = 'stopped'
                    break
                self._run_one_batch(bi, total_batches, batch)
                self._history_update(
                    success_count=self._progress['overall_done'],
                    fail_count=self._progress['overall_failed'],
                )
            else:
                if self._progress['overall_failed'] > 0:
                    final_status = 'completed'
                    final_note   = f'{self._progress["overall_failed"]}개 실패 포함'

            self._log(
                f'배치 종료 — 성공: {self._progress["overall_done"]}, '
                f'실패: {self._progress["overall_failed"]}, '
                f'전체: {total_files}'
            )

        except Exception as e:
            self._log(f'배치 예외: {e}', 'ERROR')
            P.logger.error(traceback.format_exc())
            final_status = 'error'
            final_note   = str(e)[:200]
        finally:
            self._history_finalize(final_status, final_note)
            with self._lock:
                self._is_running = False
            self._progress['current'] = ''
            self._progress['phase']   = ''
            try:
                F.socketio.emit('gds_tool2_done', dict(self._progress), namespace='/framework')
            except Exception:
                pass

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
            item.success_count  = self._progress.get('overall_done', 0)
            item.fail_count     = self._progress.get('overall_failed', 0)
            item.status         = status
            if note:
                item.note = note
            item.save()
        except Exception as e:
            P.logger.error(f'history finalize exception: {e}')

    # ── 단일 배치 처리 ────────────────────────────────────────────
    def _run_one_batch(self, bi, total_batches, batch):
        batch_size_gb = sum((f.get('Size', 0) or 0) for f in batch) / GIB
        self._progress['batch_index']  = bi
        self._progress['batch_files']  = len(batch)
        self._progress['batch_done']   = 0
        self._progress['batch_failed'] = 0
        self._progress['phase']        = 'copy'
        self._emit_progress()

        self._log(
            f'━━━ 배치 [{bi}/{total_batches}] 시작 — '
            f'{len(batch)}개 파일, {batch_size_gb:.2f} GB ━━━'
        )

        # ① 배치 내 모든 파일 add_copy 요청 (gds_tool이 백그라운드에서 병렬 처리)
        gds       = self._get_gds()
        gdrive    = P.ModelSetting.get('main_gdrive_remote')
        # 대기 대상: {filename: {'db_id': int|None, 'size': int}}
        # db_id는 빠른 실패 감지(상태 fail_*)에만 쓰고, 완료 판정은 lsjson 기준.
        pending = {}

        for f in batch:
            if self._stop_flag:
                return
            filename = f.get('Name', '?')
            fsize    = f.get('Size', 0) or 0
            self._progress['current'] = filename
            self._emit_progress()
            try:
                ret = gds.add_copy(
                    source_id     = f['ID'],
                    folder_name   = '',
                    board_type    = 'direct',
                    category_type = '',
                    size          = fsize,
                    count         = 1,
                    copy_type     = 'folder',
                    remote_path   = gdrive,
                ) or {}
                status = ret.get('ret', 'fail')
                req_id = ret.get('request_db_id')

                if status == 'success':
                    pending[filename] = {'db_id': req_id, 'size': fsize}
                    self._log(f'  ↳ 복사 요청: {filename} (id={req_id})')
                elif status == 'already':
                    # gds_tool DB는 'completed'라고 하지만 실제 파일이 없을 수 있음.
                    # → 무조건 lsjson 기준으로 다시 확인하도록 pending에 등록.
                    prev_status = ret.get('status', '')
                    pending[filename] = {'db_id': req_id, 'size': fsize}
                    self._log(f'  ↳ 기존 요청 재사용: {filename} (id={req_id}, prev={prev_status})')
                else:
                    self._log(f'  ↳ 복사 요청 실패 ({status}): {filename} / {ret}', 'ERROR')
                    self._mark_failed(filename)
            except Exception as e:
                self._log(f'  ↳ add_copy 예외: {filename}: {e}', 'ERROR')
                P.logger.error(traceback.format_exc())
                self._mark_failed(filename)

        if not pending:
            self._log(f'배치 [{bi}] 처리 가능한 항목 없음. 다음 배치로.', 'WARN')
            return

        # ② 내 드라이브에 모든 파일이 실제로 도착할 때까지 대기 (lsjson 기준)
        self._progress['phase'] = 'wait'
        self._emit_progress()
        self._wait_batch_present(bi, pending)
        if self._stop_flag:
            return

        # 한 파일도 도착 못했으면 NAS 단계 스킵
        if self._progress['batch_done'] == 0:
            self._log(f'배치 [{bi}] 도착한 파일 없음. NAS 단계 스킵.', 'WARN')
        else:
            # ③ NAS 이동 (이번 배치 파일들이 GDG:/Downloads에 있음)
            self._progress['phase']   = 'nas'
            self._progress['current'] = '(NAS rclone 이동중)'
            self._emit_progress()
            self._run_nas_move(bi)

        # ④ overall 통계 갱신
        self._progress['overall_done']   += self._progress['batch_done']
        self._progress['overall_failed'] += self._progress['batch_failed']
        self._emit_progress()

    # ── 배치 내 모든 파일이 내 드라이브에 도착할 때까지 대기 ────────
    # 완료 판정: lsjson에 동일 Name이 존재하고 Size가 원본 이상.
    # DB status는 보조 신호로만 사용 — 'fail_*'면 즉시 실패 처리.
    # 'completed'여도 파일이 없으면 계속 대기 (이게 사용자가 본 케이스).
    def _wait_batch_present(self, bi, pending):
        from support.expand.rclone import SupportRclone

        Model    = self._get_request_model()
        remote   = P.ModelSetting.get('main_gdrive_remote')
        interval = max(int(P.ModelSetting.get('main_poll_interval') or 15), 3)
        timeout  = int(P.ModelSetting.get('main_copy_timeout')  or 7200)
        start_t  = time.time()
        deadline = start_t + timeout
        total    = len(pending)

        self._log(
            f'배치 [{bi}] 내 드라이브 도착 대기 ({total}개, '
            f'타임아웃 {timeout}s, 기준 remote={remote})'
        )

        round_no = 0
        while pending and time.time() < deadline:
            if self._stop_flag:
                self._log(f'배치 [{bi}] 대기 중 사용자 중단', 'WARN')
                return
            round_no += 1
            elapsed = int(time.time() - start_t)

            # 한 라운드에 lsjson 1회 호출
            lsjson_err = None
            try:
                drive_files = SupportRclone.lsjson(remote) or []
            except Exception as e:
                lsjson_err = str(e)
                drive_files = None

            arrived_this_round = []
            if drive_files is not None:
                by_name = {
                    f.get('Name'): (f.get('Size', 0) or 0)
                    for f in drive_files
                    if not f.get('IsDir', False)
                }
                lsjson_count = len(by_name)
                for fname, info in list(pending.items()):
                    expected = info['size']
                    actual   = by_name.get(fname)
                    if actual is not None and (expected <= 0 or actual >= expected):
                        pending.pop(fname)
                        self._progress['batch_done'] += 1
                        self._progress['current']     = fname
                        arrived_this_round.append(fname)
                        self._log(f'  ✓ 도착 확인: {fname} ({actual} B)')
                        self._emit_progress()
                        continue
                    # 파일이 아직 없으면 — DB 상태로 빠른 실패만 체크
                    db_status = self._db_status(Model, info['db_id'])
                    if db_status and db_status.startswith(COPY_FAIL_PREFIX):
                        pending.pop(fname)
                        self._progress['batch_failed'] += 1
                        self._log(f'  ✗ 실패 ({db_status}): {fname}', 'ERROR')
                        self._emit_progress()
            else:
                lsjson_count = -1

            done    = self._progress['batch_done']
            failed  = self._progress['batch_failed']
            waiting = list(pending.keys())
            preview = ', '.join(waiting[:3]) + (f' 외 {len(waiting)-3}' if len(waiting) > 3 else '')

            if lsjson_err is not None:
                self._log(
                    f'  · 라운드 {round_no} ({elapsed}s) lsjson 오류: {lsjson_err} '
                    f'— 재시도 예정',
                    'WARN',
                )
            else:
                self._log(
                    f'  · 라운드 {round_no} ({elapsed}s) lsjson {lsjson_count}개 / '
                    f'완료 {done}/{total}, 실패 {failed}, 대기 {len(waiting)}'
                    + (f' [{preview}]' if waiting else '')
                )

            if not pending:
                break
            if not self._interruptible_sleep(interval):
                return

        if pending:
            for fname in pending:
                self._progress['batch_failed'] += 1
                self._log(f'  ✗ 타임아웃 (드라이브 미도착): {fname}', 'ERROR')
            self._emit_progress()

    # ── DB 상태 조회 (실패 감지용 보조) ───────────────────────────
    def _db_status(self, Model, db_id):
        if Model is None or db_id is None:
            return None
        try:
            item = Model.get_by_id(int(db_id))
            return item.status if item is not None else None
        except Exception:
            return None

    # ── NAS rclone 실행 ───────────────────────────────────────────
    def _run_nas_move(self, bi):
        script = P.ModelSetting.get('main_script_path')
        cmd    = f'bash {script} downloads'
        self._log(f'배치 [{bi}] NAS rclone 실행: {cmd}')
        try:
            code, out, err = self._ssh_exec(cmd, timeout=14400)
            if out:
                self._log(f'  stdout: {out[:300]}')
            if code == 0:
                self._log(f'배치 [{bi}] NAS 이동 완료')
                return True
            else:
                self._log(f'배치 [{bi}] NAS rclone 실패 (exit {code}): {err[:300]}', 'ERROR')
                return False
        except Exception as e:
            self._log(f'배치 [{bi}] SSH 실행 오류: {e}', 'ERROR')
            return False

    def _mark_failed(self, _filename):
        self._progress['batch_failed'] += 1
        self._emit_progress()

    # ── SJVA command 핸들러 ───────────────────────────────────────
    # 설정 저장은 프레임워크의 globalSettingSaveBtn 이 처리한다.
    # 여기서는 커스텀 명령(SSH 테스트, 미리보기, 배치 시작/중단, 상태 조회)만 다룸.
    def process_command(self, command, arg1, arg2, arg3, req):
        ret = {'ret': 'success'}
        try:
            if command == 'test_ssh':
                if self._test_ssh():
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
                    files = self._get_file_list(fid)
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
                        f'{len(files)}개 비디오 / '
                        f'{ret["total_size"]/GIB:.2f} GB / '
                        f'{len(batches)}개 배치 (배치당 {cap_desc})'
                    )

            elif command == 'start_batch':
                fid = (arg1 or '').strip()
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
                        except Exception:
                            pass
                        t = threading.Thread(target=self._batch_worker, args=(fid,), daemon=True)
                        t.start()
                        ret['msg'] = '배치 시작!'

            elif command == 'stop_batch':
                if self._is_running:
                    self._stop_flag = True
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

        except Exception as e:
            ret['ret'] = 'error'
            ret['msg'] = str(e)
            P.logger.error(traceback.format_exc())

        return jsonify(ret)
