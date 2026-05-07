import importlib
import threading
import time
import traceback
from datetime import datetime

from cryptography.fernet import Fernet

try:
    import paramiko
except ImportError:
    paramiko = None

from .setup import *


# ── 상수 ───────────────────────────────────────────────────────
GIB                  = 1024 * 1024 * 1024
DEFAULT_BATCH_GB     = 10
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
        }
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
            F.socketio.emit('gds_copy_log', entry, namespace='/framework')
        except Exception:
            pass

    def _emit_progress(self):
        try:
            F.socketio.emit('gds_copy_progress', dict(self._progress), namespace='/framework')
        except Exception:
            pass

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
            raise RuntimeError('paramiko 미설치. pip install paramiko 필요.')
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
        try:
            mod = importlib.import_module('gds_tool.mod_request')
            return mod.ModelRequestItem
        except Exception as e:
            self._log(f'gds_tool.mod_request 임포트 실패 (lsjson 폴백): {e}', 'WARN')
            return None

    # ── 공유드라이브 파일 목록 ────────────────────────────────────
    def _get_file_list(self, folder_id):
        try:
            gds    = self._get_gds()
            remote = 'worker:{%s}' % folder_id
            result = gds.SupportRcloneWorker.lsjson(remote)
            if not result:
                self._log('lsjson 결과 없음 (폴더 비어있거나 접근 불가)', 'WARN')
                return []
            files = [
                f for f in result
                if not f.get('IsDir', False)
                and (f.get('MimeType') or '').startswith('video/')
            ]
            self._log(f'전체 {len(result)}개 항목 중 비디오 {len(files)}개')
            return files
        except Exception as e:
            self._log(f'파일 목록 오류: {e}', 'ERROR')
            P.logger.error(traceback.format_exc())
            return []

    # ── 배치 그룹핑 (greedy first-fit) ────────────────────────────
    @staticmethod
    def _pack_batches(files, max_bytes):
        """파일 리스트를 배치로 묶는다.
        - 단일 파일이 max_bytes를 초과하면 단독 배치로 묶고 경고 마킹.
        - 그 외는 합이 max_bytes를 넘지 않는 선에서 순서대로 채움.
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
            if current_size + size > max_bytes and current:
                batches.append(current)
                current, current_size = [], 0
            current.append(f)
            current_size += size
        if current:
            batches.append(current)
        return batches

    # ── 배치 워커 (스레드) ────────────────────────────────────────
    def _batch_worker(self, folder_id):
        try:
            with self._lock:
                self._is_running = True
                self._stop_flag  = False
                self._logs       = []
                self._progress   = self._fresh_progress()

            self._log('SSH 연결 테스트...')
            if not self._test_ssh():
                self._log('SSH 연결 실패. 배치 중단.', 'ERROR')
                return

            self._log(f'공유드라이브 파일 목록 추출: {folder_id}')
            files = self._get_file_list(folder_id)
            if not files:
                self._log('처리할 비디오 파일이 없습니다.')
                return

            try:
                max_gb    = float(P.ModelSetting.get('main_max_batch_gb') or DEFAULT_BATCH_GB)
            except ValueError:
                max_gb    = DEFAULT_BATCH_GB
            max_bytes = int(max_gb * GIB)

            batches = self._pack_batches(files, max_bytes)
            total_files   = sum(len(b) for b in batches)
            total_batches = len(batches)

            self._progress['total_files']   = total_files
            self._progress['total_batches'] = total_batches
            self._emit_progress()

            self._log(
                f'총 {total_files}개 파일 → {total_batches}개 배치 '
                f'(배치당 최대 {max_gb:g} GB)'
            )

            for bi, batch in enumerate(batches, start=1):
                if self._stop_flag:
                    self._log('사용자 중단 (배치 시작 전)')
                    break
                self._run_one_batch(bi, total_batches, batch)

            self._log(
                f'배치 종료 — 성공: {self._progress["overall_done"]}, '
                f'실패: {self._progress["overall_failed"]}, '
                f'전체: {total_files}'
            )

        except Exception as e:
            self._log(f'배치 예외: {e}', 'ERROR')
            P.logger.error(traceback.format_exc())
        finally:
            with self._lock:
                self._is_running = False
            self._progress['current'] = ''
            self._progress['phase']   = ''
            try:
                F.socketio.emit('gds_copy_done', dict(self._progress), namespace='/framework')
            except Exception:
                pass

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
        gds          = self._get_gds()
        gdrive       = P.ModelSetting.get('main_gdrive_remote')
        request_ids  = []   # [(filename, request_db_id)]
        already_done = []   # 이미 완료된 파일명

        for f in batch:
            if self._stop_flag:
                return
            filename = f.get('Name', '?')
            self._progress['current'] = filename
            self._emit_progress()
            try:
                ret = gds.add_copy(
                    source_id     = f['ID'],
                    folder_name   = filename,
                    board_type    = 'direct',
                    category_type = '',
                    size          = f.get('Size', 0) or 0,
                    count         = 1,
                    copy_type     = 'file',
                    remote_path   = gdrive,
                ) or {}
                status = ret.get('ret', 'fail')
                req_id = ret.get('request_db_id')

                if status == 'success' and req_id is not None:
                    request_ids.append((filename, req_id))
                    self._log(f'  ↳ 복사 요청: {filename} (id={req_id})')
                elif status == 'already':
                    prev_status = ret.get('status', '')
                    if prev_status == COPY_DONE_STATUS:
                        already_done.append(filename)
                        self._log(f'  ↳ 이미 완료됨: {filename}', 'WARN')
                    elif req_id is not None:
                        request_ids.append((filename, req_id))
                        self._log(f'  ↳ 진행중인 요청 재사용: {filename} (id={req_id}, {prev_status})')
                    else:
                        self._log(f'  ↳ 중복 항목인데 id 없음: {filename}', 'WARN')
                        self._mark_failed(filename)
                else:
                    self._log(f'  ↳ 복사 요청 실패 ({status}): {filename} / {ret}', 'ERROR')
                    self._mark_failed(filename)
            except Exception as e:
                self._log(f'  ↳ add_copy 예외: {filename}: {e}', 'ERROR')
                P.logger.error(traceback.format_exc())
                self._mark_failed(filename)

        # 요청 단계에서 모두 실패 + 이미 완료된 것도 없으면 NAS 단계 스킵
        if not request_ids and not already_done:
            self._log(f'배치 [{bi}] 처리 가능한 항목 없음. 다음 배치로.', 'WARN')
            return

        # ② 모든 요청이 'completed' 될 때까지 polling
        if request_ids:
            self._progress['phase'] = 'wait'
            self._emit_progress()
            self._wait_batch_complete(bi, request_ids)
            if self._stop_flag:
                return

        # ③ NAS 이동 (이번 배치만의 파일이 GDG:/Downloads에 있을 것)
        self._progress['phase']   = 'nas'
        self._progress['current'] = '(NAS rclone 이동중)'
        self._emit_progress()
        self._run_nas_move(bi)

        # ④ overall 통계 갱신
        self._progress['overall_done']   += self._progress['batch_done'] + len(already_done)
        self._progress['overall_failed'] += self._progress['batch_failed']
        self._emit_progress()

    # ── 배치 내 모든 요청 완료 대기 ───────────────────────────────
    def _wait_batch_complete(self, bi, request_ids):
        Model    = self._get_request_model()
        interval = max(int(P.ModelSetting.get('main_poll_interval') or 15), 3)
        timeout  = int(P.ModelSetting.get('main_copy_timeout')  or 7200)
        deadline = time.time() + timeout
        pending  = dict(request_ids)   # {filename: db_id}
        finished = {}                  # {filename: status}

        self._log(f'배치 [{bi}] 복사 완료 대기 ({len(pending)}개, 타임아웃 {timeout}s)')

        while pending and time.time() < deadline:
            if self._stop_flag:
                self._log(f'배치 [{bi}] polling 중 사용자 중단', 'WARN')
                return
            for fname, did in list(pending.items()):
                status = self._query_status(Model, fname, did)
                if status == COPY_DONE_STATUS:
                    finished[fname] = status
                    pending.pop(fname)
                    self._progress['batch_done'] += 1
                    self._progress['current']     = fname
                    self._log(f'  ✓ 완료: {fname}')
                    self._emit_progress()
                elif status and status.startswith(COPY_FAIL_PREFIX):
                    finished[fname] = status
                    pending.pop(fname)
                    self._progress['batch_failed'] += 1
                    self._log(f'  ✗ 실패 ({status}): {fname}', 'ERROR')
                    self._emit_progress()
            if not pending:
                break
            if not self._interruptible_sleep(interval):
                return

        if pending:
            for fname in pending:
                self._progress['batch_failed'] += 1
                self._log(f'  ✗ 타임아웃: {fname}', 'ERROR')
            self._emit_progress()

    # ── 단일 요청 상태 질의 (DB 우선, 실패 시 lsjson 폴백) ────────
    def _query_status(self, Model, filename, db_id):
        if Model is not None:
            try:
                item = Model.get_by_id(int(db_id))
                if item is not None:
                    return item.status
            except Exception as e:
                self._log(f'status 조회 오류 (id={db_id}): {e}', 'WARN')
        # 폴백: 내 드라이브에 파일이 보이면 완료로 간주
        try:
            from support.expand.rclone import SupportRclone
            remote = P.ModelSetting.get('main_gdrive_remote')
            file_list = SupportRclone.lsjson(remote) or []
            if any(f.get('Name') == filename and not f.get('IsDir', False)
                   for f in file_list):
                return COPY_DONE_STATUS
        except Exception as e:
            self._log(f'lsjson 폴백 오류: {e}', 'WARN')
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
    def process_command(self, command, arg1, arg2, arg3, req):
        ret = {'ret': 'success'}
        try:
            if command == 'save_setting':
                fields = [
                    'main_nas_ip', 'main_nas_port', 'main_nas_user',
                    'main_script_path', 'main_gdrive_remote',
                    'main_max_batch_gb', 'main_poll_interval', 'main_copy_timeout',
                ]
                for key in fields:
                    val = req.form.get(key, '')
                    if val != '':
                        P.ModelSetting.set(key, val)
                plain_pw = req.form.get('main_nas_password_plain', '').strip()
                if plain_pw:
                    P.ModelSetting.set('main_nas_password', self._encrypt(plain_pw))
                ret['msg'] = '설정 저장 완료'

            elif command == 'test_ssh':
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
                    batches = self._pack_batches(files, int(max_gb * GIB))
                    ret['files']         = files
                    ret['batches_count'] = len(batches)
                    ret['total_size']    = sum((f.get('Size', 0) or 0) for f in files)
                    ret['msg'] = (
                        f'{len(files)}개 비디오 / '
                        f'{ret["total_size"]/GIB:.2f} GB / '
                        f'{len(batches)}개 배치 (배치당 ≤ {max_gb:g} GB)'
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

        except Exception as e:
            ret['ret'] = 'error'
            ret['msg'] = str(e)
            P.logger.error(traceback.format_exc())

        return jsonify(ret)
