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


class ModuleMain(PluginModuleBase):

    def __init__(self, P):
        super(ModuleMain, self).__init__(P, name='main', first_menu='setting')
        self.db_default = {
            'main_nas_ip':          '',
            'main_nas_port':        '22',
            'main_nas_user':        '',
            'main_nas_password':    '',   # Fernet 암호화 저장
            'main_script_path':     '/volume1/MK/rclone_move.sh',
            'main_poll_interval':   '30',
            'main_poll_timeout':    '3600',
            'main_encrypt_key':     '',   # Fernet key
            'main_gdrive_remote':   'GDG:/Downloads',
        }
        self._is_running  = False
        self._stop_flag   = False
        self._logs        = []
        self._progress    = {'total': 0, 'done': 0, 'failed': 0, 'current': '', 'index': 0}

    # ── 암호화 ────────────────────────────────────────────────────
    def _fernet(self):
        key = P.ModelSetting.get('main_encrypt_key')
        if not key:
            key = Fernet.generate_key().decode()
            P.ModelSetting.set('main_encrypt_key', key)
        return Fernet(key.encode())

    def _encrypt(self, plain: str) -> str:
        return self._fernet().encrypt(plain.encode()).decode()

    def _decrypt(self, enc: str) -> str:
        try:
            return self._fernet().decrypt(enc.encode()).decode()
        except Exception:
            return ''

    # ── 로그 ──────────────────────────────────────────────────────
    def _log(self, msg, level='INFO'):
        ts    = datetime.now().strftime('%H:%M:%S')
        entry = {'ts': ts, 'level': level, 'msg': msg}
        P.logger.info(f'[{level}] {msg}')
        self._logs.append(entry)
        if len(self._logs) > 300:
            self._logs = self._logs[-300:]
        try:
            F.socketio.emit('gds_copy_log', entry, namespace='/framework')
        except Exception:
            pass

    # ── SSH ───────────────────────────────────────────────────────
    def _ssh_exec(self, command, timeout=600):
        if paramiko is None:
            raise RuntimeError('paramiko 미설치. pip install paramiko 실행 필요.')
        ip       = P.ModelSetting.get('main_nas_ip')
        port     = int(P.ModelSetting.get('main_nas_port') or 22)
        user     = P.ModelSetting.get('main_nas_user')
        password = self._decrypt(P.ModelSetting.get('main_nas_password'))

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(ip, port=port, username=user, password=password, timeout=10)
        stdin, stdout, stderr = client.exec_command(command, timeout=timeout)
        exit_code = stdout.channel.recv_exit_status()
        out = stdout.read().decode(errors='replace').strip()
        err = stderr.read().decode(errors='replace').strip()
        client.close()
        return exit_code, out, err

    def _test_ssh(self):
        try:
            code, out, _ = self._ssh_exec('echo ok', timeout=10)
            return code == 0 and 'ok' in out
        except Exception as e:
            self._log(f'SSH 연결 실패: {e}', 'ERROR')
            return False

    # ── 파일 목록 (gds_tool 의 SupportRcloneWorker 재사용) ────────
    def _get_file_list(self, folder_id):
        try:
            gds = F.PluginManager.get_plugin_instance('gds_tool')
            remote = 'worker:{%s}' % folder_id
            result = gds.SupportRcloneWorker.lsjson(remote)
            if not result:
                return []
            files = [
                f for f in result
                if not f.get('IsDir', False)
                and f.get('MimeType', '').startswith('video/')
            ]
            self._log(f'전체 {len(result)}개 중 비디오 {len(files)}개')
            return files
        except Exception as e:
            self._log(f'파일 목록 오류: {e}', 'ERROR')
            P.logger.error(traceback.format_exc())
            return []

    # ── 내 드라이브 polling ────────────────────────────────────────
    def _wait_for_file(self, filename, file_size):
        from support.expand.rclone import SupportRclone
        remote     = P.ModelSetting.get('main_gdrive_remote')
        interval   = int(P.ModelSetting.get('main_poll_interval') or 30)
        timeout    = int(P.ModelSetting.get('main_poll_timeout') or 3600)

        # 파일 크기 기반 초기 대기 (100 MB/분 추정, 최소 30초)
        estimated = max((file_size / (100 * 1024 * 1024)) * 60, 30) if file_size > 0 else 60
        initial   = min(int(estimated * 0.8), timeout)
        self._log(f'초기 대기 {initial}초 후 polling 시작')
        
        elapsed = 0
        step    = min(initial, 10)   # 10초씩 끊어서 stop_flag 체크
        while elapsed < initial:
            if self._stop_flag:
                return False
            time.sleep(step)
            elapsed += step

        deadline = time.time() + (timeout - initial)
        while time.time() < deadline:
            if self._stop_flag:
                return False
            try:
                file_list = SupportRclone.lsjson(remote)
                if file_list and any(
                    f.get('Name') == filename
                    for f in file_list
                    if not f.get('IsDir', False)
                ):
                    self._log(f'드라이브 확인: {filename}')
                    return True
            except Exception as e:
                self._log(f'polling 오류: {e}', 'WARN')
            time.sleep(interval)

        self._log(f'타임아웃 — 파일 미확인: {filename}', 'ERROR')
        return False

    # ── NAS rclone 실행 ───────────────────────────────────────────
    def _run_rclone(self):
        script = P.ModelSetting.get('main_script_path')
        cmd    = f'bash {script} downloads'
        self._log(f'NAS rclone 실행: {cmd}')
        try:
            code, out, err = self._ssh_exec(cmd, timeout=7200)
            if out:
                self._log(f'rclone stdout: {out[:200]}')
            if code == 0:
                self._log('NAS rclone 완료')
                return True
            else:
                self._log(f'rclone 실패 (exit {code}): {err[:200]}', 'ERROR')
                return False
        except Exception as e:
            self._log(f'SSH 실행 오류: {e}', 'ERROR')
            return False

    # ── 배치 워커 (별도 스레드) ───────────────────────────────────
    def _batch_worker(self, folder_id):
        self._is_running = True
        self._stop_flag  = False
        self._logs       = []
        self._progress   = {'total': 0, 'done': 0, 'failed': 0, 'current': '', 'index': 0}

        try:
            self._log('SSH 연결 테스트...')
            if not self._test_ssh():
                self._log('SSH 연결 실패. 배치 중단.', 'ERROR')
                return

            self._log(f'파일 목록 추출: {folder_id}')
            files = self._get_file_list(folder_id)
            if not files:
                self._log('처리할 비디오 파일이 없습니다.')
                return

            total = len(files)
            self._progress['total'] = total
            self._log(f'배치 시작 — 총 {total}개 파일')
            self._emit_progress()

            gds = F.PluginManager.get_plugin_instance('gds_tool')

            for idx, f in enumerate(files):
                if self._stop_flag:
                    self._log('사용자 중단')
                    break

                file_id   = f['ID']
                filename  = f['Name']
                file_size = f.get('Size', 0)
                mb        = file_size // 1024 // 1024

                self._progress['current'] = filename
                self._progress['index']   = idx + 1
                self._log(f'[{idx+1}/{total}] 시작: {filename} ({mb} MB)')
                self._emit_progress()

                # ① gds_tool add_copy 호출
                try:
                    ret = gds.add_copy(
                        source_id   = file_id,
                        folder_name = filename,
                        board_type  = 'batch',
                        category_type = '',
                        size        = file_size,
                        count       = 1,
                        copy_type   = 'file',
                        remote_path = P.ModelSetting.get('main_gdrive_remote'),
                    )
                    status = ret.get('ret', 'fail')
                    self._log(f'복사 요청: {status}')
                    if status not in ('success', 'already'):
                        self._log(f'복사 요청 실패: {ret}', 'ERROR')
                        self._progress['failed'] += 1
                        self._emit_progress()
                        continue
                except Exception as e:
                    self._log(f'add_copy 오류: {e}', 'ERROR')
                    self._progress['failed'] += 1
                    self._emit_progress()
                    continue

                # ② 내 드라이브 파일 확인 (polling)
                if not self._wait_for_file(filename, file_size):
                    self._log(f'드라이브 확인 실패: {filename}', 'ERROR')
                    self._progress['failed'] += 1
                    self._emit_progress()
                    continue

                # ③ NAS rclone 실행
                if not self._run_rclone():
                    self._log(f'NAS 이동 실패: {filename}', 'ERROR')
                    self._progress['failed'] += 1
                    self._emit_progress()
                    continue

                self._progress['done'] += 1
                self._log(f'완료: {filename}')
                self._emit_progress()

            self._log(
                f'배치 종료 — 성공: {self._progress["done"]}, '
                f'실패: {self._progress["failed"]}, '
                f'전체: {total}'
            )

        except Exception as e:
            self._log(f'배치 예외: {e}', 'ERROR')
            P.logger.error(traceback.format_exc())
        finally:
            self._is_running            = False
            self._progress['current']   = ''
            try:
                F.socketio.emit('gds_copy_done', self._progress, namespace='/framework')
            except Exception:
                pass

    def _emit_progress(self):
        try:
            F.socketio.emit('gds_copy_progress', self._progress, namespace='/framework')
        except Exception:
            pass

    # ── SJVA command 핸들러 ───────────────────────────────────────
    def process_command(self, command, arg1, arg2, arg3, req):
        ret = {'ret': 'success'}
        try:
            if command == 'save_setting':
                fields = [
                    'main_nas_ip', 'main_nas_port', 'main_nas_user',
                    'main_script_path', 'main_poll_interval',
                    'main_poll_timeout', 'main_gdrive_remote',
                ]
                for key in fields:
                    val = req.form.get(key, '')
                    if val != '':
                        P.ModelSetting.set(key, val)
                # 비밀번호는 평문이 들어왔을 때만 업데이트
                plain_pw = req.form.get('main_nas_password_plain', '').strip()
                if plain_pw:
                    P.ModelSetting.set('main_nas_password', self._encrypt(plain_pw))
                ret['msg'] = '설정 저장 완료'

            elif command == 'test_ssh':
                ok = self._test_ssh()
                if ok:
                    ret['msg'] = 'SSH 연결 성공!'
                else:
                    ret['ret'] = 'error'
                    ret['msg'] = 'SSH 연결 실패. IP/포트/계정 확인하세요.'

            elif command == 'preview_files':
                fid = (arg1 or '').strip()
                if not fid:
                    ret['ret'] = 'error'
                    ret['msg'] = '폴더 ID를 입력하세요.'
                else:
                    files = self._get_file_list(fid)
                    ret['files'] = files
                    ret['msg']   = f'{len(files)}개 비디오 파일 확인'

            elif command == 'start_batch':
                fid = (arg1 or '').strip()
                if not fid:
                    ret['ret'] = 'error'
                    ret['msg'] = '폴더 ID를 입력하세요.'
                elif self._is_running:
                    ret['ret'] = 'error'
                    ret['msg'] = '이미 실행 중입니다.'
                else:
                    t = threading.Thread(target=self._batch_worker, args=(fid,), daemon=True)
                    t.start()
                    ret['msg'] = '배치 시작!'

            elif command == 'stop_batch':
                if self._is_running:
                    self._stop_flag = True
                    ret['msg'] = '중단 요청. 현재 파일 완료 후 정지합니다.'
                else:
                    ret['msg'] = '실행 중인 배치 없음.'

            elif command == 'get_status':
                ret['is_running'] = self._is_running
                ret['progress']   = self._progress
                ret['logs']       = self._logs[-100:]

        except Exception as e:
            ret['ret'] = 'error'
            ret['msg'] = str(e)
            P.logger.error(traceback.format_exc())

        return jsonify(ret)
