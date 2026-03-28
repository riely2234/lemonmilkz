"""
Vortex Hosting v12.1 — ACL Cloud UI Style with VPS Support
Install: pip install flask flask-socketio psutil werkzeug eventlet
Run:     python main.py
"""

try:
    import eventlet
    eventlet.monkey_patch()
    _ASYNC_MODE = 'eventlet'
except (ImportError, Exception):
    _ASYNC_MODE = 'threading'

import contextlib, hashlib, json, logging, os, re, shutil, subprocess, sys
import threading, time, zipfile, psutil, secrets
from logging import Formatter, StreamHandler, getLogger
from flask import Flask, jsonify, render_template_string, request, send_file, session
from flask_socketio import SocketIO, join_room
from werkzeug.utils import secure_filename

log = getLogger('vortexhost')

_ANSI_RE = re.compile(r'\x1b\[[0-9;]*[mGKHFABCDJhls]|\x1b\][^\x07]*\x07|\x1b[\[\]()#;?]*(?:[0-9]{1,4}(?:;[0-9]{0,4})*)?[0-9A-ORZcf-nqry=><~]|[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]')
def strip_ansi(s):
    return _ANSI_RE.sub('', s)

log.setLevel(logging.INFO)
_h = StreamHandler()
_h.setFormatter(Formatter('%(asctime)s %(levelname)s %(message)s'))
log.addHandler(_h)

app = Flask(__name__)
app.secret_key = os.environ.get('VORTEX_SECRET_KEY', secrets.token_hex(32))
app.config['PERMANENT_SESSION_LIFETIME'] = 60 * 60 * 24 * 30
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_SECURE'] = False

socketio = SocketIO(app, cors_allowed_origins='*', async_mode=_ASYNC_MODE,
    logger=False, engineio_logger=False, max_http_buffer_size=200 * 1024 * 1024)

BOTS_DIR    = os.path.join(os.getcwd(), 'vortex_bots')
CONFIG_FILE = os.path.join(os.getcwd(), 'vortex_config.json')
USERS_FILE  = os.path.join(os.getcwd(), 'vortex_users.json')
MAX_BOTS_PER_USER = 20

# --- AUTO-CREATE INFRASTRUCTURE -----------------------------------------------
os.makedirs(BOTS_DIR, exist_ok=True)
for fpath in [CONFIG_FILE, USERS_FILE]:
    if not os.path.exists(fpath):
        with open(fpath, 'w', encoding='utf-8') as f:
            json.dump({}, f)

_config_lock = threading.RLock()
_users_lock  = threading.RLock()
bots = {}

def _atomic_write(path: str, data: dict):
    tmp = path + '.tmp'
    bak = path + '.bak'
    try:
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        if os.path.exists(path):
            shutil.copy2(path, bak)
        os.replace(tmp, path)
    except Exception as e:
        log.error(f'Atomic write failed for {path}: {e}')
        with contextlib.suppress(Exception):
            os.remove(tmp)
        raise

def _safe_read(path: str) -> dict:
    for candidate in [path, path + '.bak']:
        if not os.path.exists(candidate):
            continue
        try:
            with open(candidate, encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            log.warning(f'Could not read {candidate}: {e}')
    return {}

def _hash_pw(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()

def load_users() -> dict:
    with _users_lock:
        return _safe_read(USERS_FILE)

def save_users(u: dict):
    with _users_lock:
        _atomic_write(USERS_FILE, u)

def load_config() -> dict:
    with _config_lock:
        return _safe_read(CONFIG_FILE)

def save_config(cfg: dict):
    with _config_lock:
        _atomic_write(CONFIG_FILE, cfg)

def get_bot_dir(bot_id: str) -> str:
    p = os.path.join(BOTS_DIR, bot_id)
    os.makedirs(p, exist_ok=True)
    return p

def safe_path(bot_id: str, fn: str):
    bd = os.path.abspath(get_bot_dir(bot_id))
    clean_fn = os.path.normpath('/' + fn.replace('\\', '/')).lstrip('/')
    if not clean_fn:
        return None
    fp = os.path.abspath(os.path.join(bd, clean_fn))
    if fp == bd or fp.startswith(bd + os.sep):
        return fp
    return None

SUPPORTED_EXTENSIONS = ('.py', '.js', '.ts', '.sh', '.rs', '.cs')

def safe_startup_file(startup_file: str):
    sf = startup_file.strip().replace('\\', '/')
    if not sf or '..' in sf or sf.startswith('/'):
        return None
    if re.search(r'[;&|`$<>!]', sf):
        return None
    if not sf.endswith(SUPPORTED_EXTENSIONS):
        return None
    return sf

def check_owner(bot_id: str) -> bool:
    user = session.get('username')
    return bool(user and load_config().get(bot_id, {}).get('owner') == user)

def check_access(bot_id: str) -> bool:
    user = session.get('username')
    if not user:
        return False
    cfg = load_config().get(bot_id, {})
    return cfg.get('owner') == user or user in cfg.get('shared_with', [])

def emit_log(bot_id: str, msg: str, level: str = 'default'):
    cfg = load_config().get(bot_id, {})
    listeners = list({u for u in [cfg.get('owner')] + cfg.get('shared_with', []) if u})
    for u in listeners:
        with contextlib.suppress(Exception):
            socketio.emit('console_log', {'bot_id': bot_id, 'msg': msg, 'level': level}, room=u)
    entry = {'msg': msg, 'level': level, 'time': time.strftime('%H:%M:%S')}
    bots.setdefault(bot_id, {}).setdefault('logs', []).append(entry)
    if len(bots[bot_id]['logs']) > 500:
        bots[bot_id]['logs'] = bots[bot_id]['logs'][-500:]
    try:
        with open(os.path.join(get_bot_dir(bot_id), 'system.log'), 'a', encoding='utf-8') as f:
            f.write(json.dumps(entry) + '\n')
    except Exception:
        pass

def is_running(bot_id: str) -> bool:
    return (bot_id in bots
            and bots[bot_id].get('process') is not None
            and bots[bot_id]['process'].poll() is None)

def broadcast_status(bot_id: str, status: str, start_t=None):
    cfg = load_config().get(bot_id, {})
    listeners = [cfg.get('owner')] + cfg.get('shared_with', [])
    payload = {'bot_id': bot_id, 'status': status}
    if start_t:
        payload['start_time'] = start_t
    for u in set(listeners):
        if u:
            with contextlib.suppress(Exception):
                socketio.emit('status_update', payload, room=u)

def _run_install(bot_id: str, cmd, cwd=None, timeout=300) -> int:
    import queue as _queue
    q  = _queue.Queue()
    rc = [None]
    env = os.environ.copy()
    env['PYTHONUNBUFFERED']             = '1'
    env['PIP_NO_COLOR']                 = '1'
    env['PIP_DISABLE_PIP_VERSION_CHECK']= '1'

    def _worker():
        try:
            r_fd, w_fd = os.pipe()
            p = subprocess.Popen(cmd, stdout=w_fd, stderr=w_fd,
                                 stdin=subprocess.DEVNULL,
                                 close_fds=True, cwd=cwd, env=env)
            os.close(w_fd)
            buf = b''
            while True:
                try:
                    chunk = os.read(r_fd, 4096)
                except OSError:
                    break
                if not chunk:
                    break
                buf += chunk
                while b'\n' in buf:
                    line, buf = buf.split(b'\n', 1)
                    q.put(strip_ansi(line.decode('utf-8', errors='replace').rstrip()))
            if buf:
                q.put(strip_ansi(buf.decode('utf-8', errors='replace').rstrip()))
            try: os.close(r_fd)
            except: pass
            p.wait()
            rc[0] = p.returncode
        except Exception as e:
            q.put(f'[Error] {e}')
            rc[0] = -1
        finally:
            q.put(None)

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    deadline = time.time() + timeout
    while True:
        try:
            item = q.get(timeout=1.0)
            if item is None:
                break
            if item.strip():
                emit_log(bot_id, item, 'default')
        except _queue.Empty:
            if not t.is_alive():
                break
            if time.time() > deadline:
                emit_log(bot_id, '[Error] Command timed out.', 'error')
                break
    t.join(timeout=5)
    return rc[0] if rc[0] is not None else -1

# --- UNIVERSAL COMMAND EXECUTOR -----------------------------------------------
def _run_shell_cmd(bot_id: str, cmd_str: str):
    bot_dir = get_bot_dir(bot_id)
    try:
        env = os.environ.copy()
        env['PYTHONUNBUFFERED'] = '1'
        p = subprocess.Popen(cmd_str, shell=True, cwd=bot_dir, env=env,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             stdin=subprocess.DEVNULL, text=True, bufsize=1)
        for line in p.stdout:
            emit_log(bot_id, line.rstrip(), 'default')
        p.wait()
        emit_log(bot_id, f'[Terminal] Finished with code {p.returncode}', 'system')
    except Exception as e:
        emit_log(bot_id, f'[Error] Execution failed: {e}', 'error')


def _patch_py_for_asyncio(bot_dir: str, startup_file: str) -> bool:
    full = os.path.join(bot_dir, startup_file)
    try:
        with open(full, 'r', encoding='utf-8', errors='replace') as f:
            src = f.read()
    except Exception:
        return False

    uses_discord  = any(lib in src for lib in ('import discord', 'from discord', 'import nextcord', 'from nextcord', 'import disnake', 'from disnake'))
    already_fixed = '[Vortex]' in src or 'set_event_loop' in src or 'new_event_loop' in src
    uses_uvloop   = 'uvloop' in src or 'winloop' in src or 'EventLoopPolicy' in src

    if not uses_discord or already_fixed or uses_uvloop:
        return False

    patch = (
        '# [Vortex] asyncio compatibility patch\n'
        'import asyncio as _vortex_asyncio\n'
        'try:\n'
        '    _vortex_asyncio.get_event_loop()\n'
        'except RuntimeError:\n'
        '    _vortex_asyncio.set_event_loop(_vortex_asyncio.new_event_loop())\n'
        '# [/Vortex]\n'
    )

    lines = src.splitlines(keepends=True)
    insert_at = 0
    for i, line in enumerate(lines):
        stripped = line.lstrip()
        if stripped.startswith('import ') or stripped.startswith('from '):
            insert_at = i + 1
        elif stripped and not stripped.startswith('#') and insert_at > 0:
            break

    lines.insert(insert_at, patch)
    try:
        with open(full, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        return True
    except Exception:
        return False


def start_bot(bot_id: str, startup_file=None, _restart_count=0):
    cfg     = load_config()
    bot_cfg = cfg.get(bot_id, {})
    bot_dir = get_bot_dir(bot_id)

    raw_sf       = startup_file or bot_cfg.get('startup_file', 'main.py')
    startup_file = safe_startup_file(raw_sf)
    if not startup_file:
        emit_log(bot_id, f'[Error] Invalid startup file: {raw_sf}', 'error')
        return

    full_path = os.path.join(bot_dir, startup_file)

    if is_running(bot_id):
        emit_log(bot_id, '[System] Already running.', 'system')
        return
    if not os.path.exists(full_path):
        emit_log(bot_id, f'[Error] Not found: {startup_file}', 'error')
        return

    ext = startup_file.rsplit('.', 1)[-1].lower()

    if ext == 'py':
        req = os.path.join(bot_dir, 'requirements.txt')
        if os.path.exists(req):
            emit_log(bot_id, '[System] Installing Python requirements...', 'system')
            rc = _run_install(bot_id, [
                sys.executable, '-m', 'pip', 'install',
                '--no-input', '--no-color',
                '--disable-pip-version-check',
                '--progress-bar', 'off',
                '-r', req,
            ])
            if rc != 0:
                emit_log(bot_id, '[Error] pip install failed.', 'error')
                return
            emit_log(bot_id, '[System] Requirements installed.', 'success')

        if _patch_py_for_asyncio(bot_dir, startup_file):
            emit_log(bot_id, '[System] Applied asyncio compatibility patch.', 'system')

    elif ext in ('js', 'ts'):
        pkg = os.path.join(bot_dir, 'package.json')
        if os.path.exists(pkg):
            emit_log(bot_id, '[System] Running npm install...', 'system')
            rc = _run_install(bot_id,
                ['npm', 'install', '--no-progress', '--no-audit', '--no-fund'],
                cwd=bot_dir)
            if rc != 0:
                emit_log(bot_id, '[Error] npm install failed.', 'error')
                return
            emit_log(bot_id, '[System] npm packages installed.', 'success')
        if ext == 'ts' and not shutil.which('ts-node') and not shutil.which('npx'):
            emit_log(bot_id, '[Error] ts-node/npx not found.', 'error')
            return

    elif ext == 'rs':
        if not os.path.exists(os.path.join(bot_dir, 'Cargo.toml')):
            emit_log(bot_id, '[Error] Cargo.toml not found.', 'error')
            return
        if not shutil.which('cargo'):
            emit_log(bot_id, '[Error] cargo not found.', 'error')
            return
        emit_log(bot_id, '[System] Building Rust project...', 'system')
        rc = _run_install(bot_id, ['cargo', 'build', '--release'], cwd=bot_dir, timeout=600)
        if rc != 0:
            emit_log(bot_id, '[Error] cargo build failed.', 'error')
            return
        emit_log(bot_id, '[System] Rust build successful.', 'success')

    elif ext == 'cs':
        if not any(f.endswith('.csproj') for f in os.listdir(bot_dir)):
            emit_log(bot_id, '[Error] No .csproj file found.', 'error')
            return
        if not shutil.which('dotnet'):
            emit_log(bot_id, '[Error] dotnet not found.', 'error')
            return
        emit_log(bot_id, '[System] Building C# project...', 'system')
        rc = _run_install(bot_id, ['dotnet', 'build', '--configuration', 'Release'], cwd=bot_dir, timeout=600)
        if rc != 0:
            emit_log(bot_id, '[Error] dotnet build failed.', 'error')
            return
        emit_log(bot_id, '[System] C# build successful.', 'success')

    if ext == 'py':
        cmd = [sys.executable, '-u', full_path]
    elif ext == 'js':
        cmd = ['node', full_path]
    elif ext == 'ts':
        cmd = (['ts-node', full_path] if shutil.which('ts-node') else ['npx', 'ts-node', full_path])
    elif ext == 'sh':
        cmd = ['bash', full_path]
    elif ext == 'rs':
        bin_dir  = os.path.join(bot_dir, 'target', 'release')
        pkg_name = None
        try:
            with open(os.path.join(bot_dir, 'Cargo.toml')) as f:
                _ct = f.read()
            m = re.search(r'^\s*name\s*=\s*"([^"]+)"', _ct, re.MULTILINE)
            if m:
                pkg_name = m.group(1)
        except Exception:
            pass
        binary = None
        if pkg_name:
            for sfx in ('', '.exe'):
                c = os.path.join(bin_dir, pkg_name + sfx)
                if os.path.isfile(c):
                    binary = c; break
        if not binary and os.path.isdir(bin_dir):
            skip = ('.d', '.rlib', '.pdb', '.exp', '.lib', '.so', '.dylib', '.dll')
            cands = [f for f in os.listdir(bin_dir)
                     if os.path.isfile(os.path.join(bin_dir, f))
                     and not f.endswith(skip) and not f.startswith('.')]
            if cands:
                binary = os.path.join(bin_dir, cands[0])
        if not binary:
            emit_log(bot_id, '[Error] Could not locate compiled Rust binary.', 'error')
            return
        cmd = [binary]
    elif ext == 'cs':
        cmd = ['dotnet', 'run', '--configuration', 'Release', '--project', bot_dir]
    else:
        emit_log(bot_id, f'[Error] Unsupported extension: .{ext}', 'error')
        return

    run_env = os.environ.copy()
    for _k, _v in bot_cfg.get('env', {}).items():
        if _k:
            run_env[str(_k)] = str(_v)
    run_env['PYTHONUNBUFFERED']         = '1'
    run_env['PYTHONDONTWRITEBYTECODE']  = '1'

    emit_log(bot_id, f'[System] Starting {startup_file}...', 'system')
    try:
        out_r, out_w = os.pipe()
        in_r,  in_w  = os.pipe()

        proc = subprocess.Popen(
            cmd,
            stdout=out_w, stderr=out_w,
            stdin=in_r,
            close_fds=True,
            cwd=bot_dir,
            env=run_env,
        )
        os.close(out_w)
        os.close(in_r)

        start_t = time.time()
        bots.setdefault(bot_id, {}).update({
            'process':      proc,
            'startup_file': startup_file,
            'start_time':   start_t,
            'auto_restart': bot_cfg.get('auto_restart', False),
            'stdin_fd':     in_w,
        })
        bot_cfg['startup_file'] = startup_file
        cfg[bot_id] = bot_cfg
        save_config(cfg)
        broadcast_status(bot_id, 'online', start_t)

        def _read(r_fd=out_r):
            buf = b''
            try:
                while True:
                    try:
                        chunk = os.read(r_fd, 4096)
                    except OSError:
                        break
                    if not chunk:
                        break
                    buf += chunk
                    while b'\n' in buf:
                        line, buf = buf.split(b'\n', 1)
                        txt = strip_ansi(line.decode('utf-8', errors='replace').rstrip())
                        if txt:
                            emit_log(bot_id, txt, 'default')
                if buf:
                    txt = strip_ansi(buf.decode('utf-8', errors='replace').rstrip())
                    if txt:
                        emit_log(bot_id, txt, 'default')
            except Exception:
                pass
            finally:
                try: os.close(r_fd)
                except: pass
                try:
                    fd = bots.get(bot_id, {}).get('stdin_fd')
                    if fd is not None:
                        os.close(fd)
                        bots[bot_id]['stdin_fd'] = None
                except: pass

            proc.wait()
            rc = proc.returncode
            uptime = time.time() - start_t
            broadcast_status(bot_id, 'offline')
            emit_log(bot_id, f'[System] Exited with code {rc} (uptime {uptime:.1f}s).', 'system')

            should_restart = (
                bots.get(bot_id, {}).get('auto_restart')
                and rc != 0
                and not bots.get(bot_id, {}).get('_stopping')
            )
            if should_restart:
                delay = min(3 * (2 ** _restart_count), 60)
                emit_log(bot_id, f'[System] Auto-restart #{_restart_count+1} in {delay}s...', 'system')
                time.sleep(delay)
                if (bots.get(bot_id, {}).get('auto_restart')
                        and not is_running(bot_id)
                        and not bots.get(bot_id, {}).get('_stopping')):
                    start_bot(bot_id, startup_file, _restart_count=_restart_count + 1)

        threading.Thread(target=_read, daemon=True).start()

    except Exception as e:
        emit_log(bot_id, f'[Error] Failed to start: {e}', 'error')


def stop_bot(bot_id: str, disable_auto_restart=True):
    if bot_id in bots:
        bots[bot_id]['_stopping'] = True
        if disable_auto_restart:
            bots[bot_id]['auto_restart'] = False
        try:
            fd = bots[bot_id].get('stdin_fd')
            if fd is not None:
                os.close(fd)
                bots[bot_id]['stdin_fd'] = None
        except Exception:
            pass
        proc = bots[bot_id].get('process')
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            emit_log(bot_id, '[System] Stopped.', 'system')
            broadcast_status(bot_id, 'offline')
        bots[bot_id]['_stopping'] = False


HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=5,user-scalable=yes">
<title>Vortex Hosting</title>
<script src="https://cdn.socket.io/4.6.1/socket.io.min.js" crossorigin="anonymous"></script>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
:root{
  --bg:#0f1117;--surface:#161b26;--surface2:#1e2433;--surface3:#242b3d;
  --border:#2a3347;--border2:#344060;
  --teal:#00d4aa;--teal-dim:rgba(0,212,170,0.1);--teal-glow:rgba(0,212,170,0.25);
  --blue:#4d9fff;--blue-dim:rgba(77,159,255,0.1);
  --green:#22c55e;--green-dim:rgba(34,197,94,0.1);
  --orange:#f97316;--orange-dim:rgba(249,115,22,0.1);
  --red:#ef4444;--red-dim:rgba(239,68,68,0.1);
  --purple:#a855f7;--purple-dim:rgba(168,85,247,0.1);
  --yellow:#eab308;--yellow-dim:rgba(234,179,8,0.1);
  --text:#e2e8f0;--text2:#94a3b8;--text3:#475569;
  --font:'Inter',sans-serif;--mono:'JetBrains Mono',monospace;
  --radius:10px;--radius-sm:6px;--radius-lg:14px;
}
html,body{width:100%;height:100%;overflow:hidden;background:var(--bg);color:var(--text);font-family:var(--font);-webkit-font-smoothing:antialiased}
::-webkit-scrollbar{width:4px;height:4px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--border2);border-radius:10px}
#app{display:flex;width:100%;height:100%}

/* FIX: page visibility - without this all pages stack on top of each other */
.page{display:none}
.page.active{display:block;animation:fadeUp .2s ease}
@keyframes fadeUp{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}

.sidebar{width:240px;min-width:240px;height:100%;display:flex;flex-direction:column;background:var(--surface);border-right:1px solid var(--border);transition:transform .3s cubic-bezier(0.16,1,0.3,1);position:relative;z-index:200}
.logo-area{padding:20px 20px 16px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:10px}
.logo-icon{width:34px;height:34px;background:linear-gradient(135deg,var(--teal),#0099cc);border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:16px;font-weight:700;color:#000;flex-shrink:0;letter-spacing:-1px}
.logo-text{font-size:15px;font-weight:700;color:var(--text);letter-spacing:-.3px}
.logo-sub{font-size:10px;color:var(--text3);font-family:var(--mono)}
.nav-section-label{font-size:10px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:var(--text3);padding:14px 20px 6px}
.nav-item{display:flex;align-items:center;gap:10px;padding:8px 16px;margin:1px 8px;border-radius:var(--radius-sm);font-size:13.5px;font-weight:500;color:var(--text2);cursor:pointer;transition:all .15s;border:1px solid transparent;-webkit-tap-highlight-color:transparent;outline:none;user-select:none}
.nav-item:hover{background:var(--surface2);color:var(--text);border-color:var(--border)}
.nav-item.active{background:rgba(0,212,170,0.08);color:var(--teal);border-color:rgba(0,212,170,0.2)}
.nav-item.disabled{opacity:.4;cursor:default;pointer-events:none}
.nav-icon{width:16px;text-align:center;font-size:14px;opacity:.7;flex-shrink:0}
.nav-item.active .nav-icon{opacity:1}
.instance-count-badge{font-size:10px;font-weight:600;background:var(--teal-dim);color:var(--teal);border:1px solid rgba(0,212,170,0.25);border-radius:20px;padding:1px 8px}
.new-bot-btn{display:flex;align-items:center;gap:8px;width:100%;padding:8px 12px;border:1px dashed rgba(0,212,170,0.25);border-radius:var(--radius-sm);background:transparent;color:var(--text3);cursor:pointer;font-size:12px;font-weight:500;font-family:var(--font);transition:all .2s;margin-bottom:8px}
.new-bot-btn:hover{border-color:var(--teal);color:var(--teal);background:var(--teal-dim)}
.bot-list{flex:1;overflow-y:auto;padding:0 8px 8px;-webkit-overflow-scrolling:touch}
.bot-list-item{display:flex;align-items:center;gap:8px;padding:8px 12px;border-radius:var(--radius-sm);cursor:pointer;transition:all .15s;margin-bottom:2px;border:1px solid transparent}
.bot-list-item:hover{background:var(--surface2);border-color:var(--border)}
.bot-list-item.active{background:rgba(0,212,170,0.06);border-color:rgba(0,212,170,0.18)}
.bot-list-dot{width:6px;height:6px;border-radius:50%;flex-shrink:0}
.bot-list-dot.online{background:var(--teal);box-shadow:0 0 6px var(--teal-glow)}
.bot-list-dot.offline{background:var(--text3)}
.bot-list-name{font-size:12.5px;font-weight:500;color:var(--text2);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.bot-list-item.active .bot-list-name{color:var(--text)}
.bot-list-type{font-size:9px;font-weight:600;padding:1px 5px;border-radius:3px;text-transform:uppercase;letter-spacing:.04em;flex-shrink:0}
.bot-list-type.vps{background:var(--blue-dim);color:var(--blue);border:1px solid rgba(77,159,255,0.2)}
.bot-list-type.bot{background:var(--green-dim);color:var(--green);border:1px solid rgba(34,197,94,0.2)}
.sidebar-footer{padding:12px 16px;border-top:1px solid var(--border);display:flex;align-items:center;justify-content:space-between}
.user-info{display:flex;align-items:center;gap:8px}
.user-avatar{width:28px;height:28px;background:linear-gradient(135deg,var(--teal),var(--blue));border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;color:#fff}
.user-name{font-size:12px;font-weight:500;color:var(--text2)}
.logout-btn{background:none;border:none;color:var(--text3);cursor:pointer;font-size:11px;font-family:var(--font);transition:color .15s;padding:4px 8px;border-radius:4px}
.logout-btn:hover{color:var(--red);background:var(--red-dim)}
.main{flex:1;min-width:0;height:100%;display:flex;flex-direction:column}
.topbar{height:56px;min-height:56px;background:var(--surface);border-bottom:1px solid var(--border);display:flex;align-items:center;padding:0 24px;gap:12px;flex-shrink:0}
.mobile-menu-btn{display:none;background:var(--surface2);border:1px solid var(--border);color:var(--text2);padding:6px 10px;border-radius:var(--radius-sm);cursor:pointer;font-size:14px;touch-action:manipulation}
.topbar-title{font-size:15px;font-weight:600;color:var(--text);flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.topbar-bot-name{font-size:12px;color:var(--text3);font-family:var(--mono);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:200px}
.uptime-badge{font-size:10px;font-family:var(--mono);font-weight:600;color:var(--green);background:var(--green-dim);border:1px solid rgba(34,197,94,0.2);padding:3px 10px;border-radius:20px;white-space:nowrap;display:none}
.uptime-badge.visible{display:inline-flex;align-items:center;gap:5px}
.status-pill{display:flex;align-items:center;gap:6px;padding:5px 12px;border-radius:20px;font-size:11px;font-weight:600;letter-spacing:.03em;border:1px solid;transition:all .3s;flex-shrink:0}
.status-pill.online{background:var(--green-dim);color:var(--green);border-color:rgba(34,197,94,0.3)}
.status-pill.offline{background:var(--surface2);color:var(--text3);border-color:var(--border)}
.status-led{width:6px;height:6px;border-radius:50%;flex-shrink:0}
.status-pill.online .status-led{background:var(--green);animation:pulse-green 2s ease-in-out infinite}
.status-pill.offline .status-led{background:var(--text3)}
@keyframes pulse-green{0%,100%{box-shadow:0 0 0 0 rgba(34,197,94,0.4)}50%{box-shadow:0 0 0 4px rgba(34,197,94,0)}}
.page-area{flex:1;overflow-y:auto;-webkit-overflow-scrolling:touch;padding:24px}
.page-header{margin-bottom:24px}
.page-header-top{display:flex;align-items:center;justify-content:space-between;margin-bottom:4px;flex-wrap:wrap;gap:10px}
.page-title{font-size:20px;font-weight:700;color:var(--text)}
.page-subtitle{font-size:13px;color:var(--text3)}
.filter-tabs{display:flex;gap:4px;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-sm);padding:3px}
.filter-tab{padding:5px 14px;border-radius:5px;font-size:12px;font-weight:500;color:var(--text3);cursor:pointer;transition:all .15s;border:none;background:transparent;font-family:var(--font)}
.filter-tab.active{background:var(--surface2);color:var(--text);border:1px solid var(--border)}
.filter-tab:hover:not(.active){color:var(--text2)}
.stats-row{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-bottom:24px}
.stat-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);padding:20px;display:flex;align-items:center;gap:16px;transition:all .2s}
.stat-card:hover{border-color:var(--border2);transform:translateY(-1px)}
.stat-icon-wrap{width:44px;height:44px;border-radius:10px;display:flex;align-items:center;justify-content:center;font-size:20px;flex-shrink:0}
.stat-info{flex:1;min-width:0}
.stat-number{font-size:28px;font-weight:700;line-height:1;margin-bottom:4px}
.stat-label{font-size:12px;color:var(--text3);font-weight:500}
.bots-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:16px}
.bot-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);overflow:hidden;cursor:pointer;transition:all .2s;display:flex;flex-direction:column}
.bot-card:hover{border-color:var(--border2);transform:translateY(-2px);box-shadow:0 8px 24px rgba(0,0,0,0.3)}
.bot-card.selected{border-color:rgba(0,212,170,0.35);box-shadow:0 0 0 1px rgba(0,212,170,0.1)}
.bot-card-header{padding:16px 16px 12px;display:flex;align-items:flex-start;justify-content:space-between;gap:8px}
.bot-card-name-row{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.bot-card-name{font-size:14px;font-weight:600;color:var(--text)}
.bot-card-runtime{font-size:11px;color:var(--text3);font-family:var(--mono)}
.bot-card-free-badge{font-size:10px;font-weight:600;padding:2px 8px;border-radius:20px;background:var(--green-dim);color:var(--green);border:1px solid rgba(34,197,94,0.25)}
.bot-card-online-badge{font-size:10px;font-weight:600;padding:2px 8px;border-radius:20px;background:var(--teal-dim);color:var(--teal);border:1px solid rgba(0,212,170,0.3);display:flex;align-items:center;gap:5px}
.bot-card-online-dot{width:5px;height:5px;border-radius:50%;background:var(--teal);animation:pulse-teal 2s ease-in-out infinite}
@keyframes pulse-teal{0%,100%{opacity:1}50%{opacity:.4}}
.bot-card-offline-badge{font-size:10px;font-weight:600;padding:2px 8px;border-radius:20px;background:var(--surface2);color:var(--text3);border:1px solid var(--border)}
.bot-card-specs{display:grid;grid-template-columns:1fr 1fr;gap:1px;background:var(--border);border-top:1px solid var(--border);border-bottom:1px solid var(--border);margin:0}
.bot-spec{background:var(--surface);padding:12px 16px}
.bot-spec-label{font-size:9px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:var(--text3);margin-bottom:4px;display:flex;align-items:center;gap:5px}
.bot-spec-icon{font-size:10px;opacity:.7}
.bot-spec-value{font-size:13px;font-weight:600;color:var(--text)}
.bot-card-footer{padding:12px 16px;display:flex;flex-direction:column;gap:8px}
.bot-card-expiry{display:flex;align-items:center;gap:8px;background:var(--surface2);border-radius:var(--radius-sm);padding:8px 12px;font-size:12px}
.bot-card-expiry-icon{color:var(--text3);font-size:13px}
.bot-card-expiry-text{color:var(--text2)}
.bot-card-renewal{display:flex;align-items:center;gap:8px;background:rgba(249,115,22,0.08);border:1px solid rgba(249,115,22,0.2);border-radius:var(--radius-sm);padding:8px 12px;font-size:11px;color:var(--orange)}
.bot-card-renewal-icon{font-size:13px}
.bot-card-actions{display:flex;gap:8px;padding:0 16px 16px}
.btn-manage{flex:1;display:flex;align-items:center;justify-content:center;gap:6px;padding:8px 14px;background:rgba(0,212,170,0.1);color:var(--teal);border:1px solid rgba(0,212,170,0.25);border-radius:var(--radius-sm);font-size:12px;font-weight:600;cursor:pointer;transition:all .15s;font-family:var(--font)}
.btn-manage:hover{background:rgba(0,212,170,0.2);border-color:rgba(0,212,170,0.4)}
.btn-modify{display:flex;align-items:center;gap:6px;padding:8px 14px;background:var(--surface2);color:var(--text2);border:1px solid var(--border);border-radius:var(--radius-sm);font-size:12px;font-weight:600;cursor:pointer;transition:all .15s;font-family:var(--font)}
.btn-modify:hover{background:var(--surface3);border-color:var(--border2);color:var(--text)}
.search-row{display:flex;align-items:center;gap:10px;margin-bottom:16px;flex-wrap:wrap}
.search-box{flex:1;min-width:200px;position:relative}
.search-input{width:100%;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-sm);padding:8px 12px 8px 36px;font-size:13px;color:var(--text);outline:none;font-family:var(--font);transition:border-color .15s}
.search-input:focus{border-color:var(--border2)}
.search-input::placeholder{color:var(--text3)}
.search-icon{position:absolute;left:11px;top:50%;transform:translateY(-50%);color:var(--text3);font-size:14px;pointer-events:none}
.panel{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);margin-bottom:16px;overflow:hidden}
.panel-head{display:flex;align-items:center;justify-content:space-between;padding:14px 18px;border-bottom:1px solid var(--border);flex-wrap:wrap;gap:8px}
.panel-title{font-size:14px;font-weight:600;color:var(--text);display:flex;align-items:center;gap:8px}
.panel-title-icon{width:26px;height:26px;border-radius:6px;background:var(--surface2);border:1px solid var(--border);display:flex;align-items:center;justify-content:center;font-size:12px}
.panel-body{padding:18px}
.hint{font-size:12px;color:var(--text3);padding:8px 18px;background:rgba(0,0,0,0.2);border-bottom:1px solid var(--border)}
.term-header{background:rgba(0,0,0,0.4);padding:10px 14px;display:flex;align-items:center;gap:8px;border-bottom:1px solid var(--border)}
.term-dots{display:flex;gap:5px}
.term-dot{width:10px;height:10px;border-radius:50%}
.term-label{flex:1;text-align:center;font-family:var(--mono);font-size:10px;color:var(--text3);letter-spacing:.05em}
.terminal{background:rgba(0,0,0,0.4);padding:12px;overflow-y:auto;-webkit-overflow-scrolling:touch;font-family:var(--mono);font-size:12px;line-height:1.7}
.log-row{display:flex;align-items:baseline;gap:8px;padding:2px 4px;border-radius:4px}
.log-row:hover{background:rgba(255,255,255,0.02)}
.log-ts{font-size:10px;color:var(--text3);flex-shrink:0;min-width:52px}
.log-tag{font-size:8px;padding:2px 5px;border-radius:3px;flex-shrink:0;text-transform:uppercase;font-weight:700;letter-spacing:.05em}
.log-tag.sys{background:rgba(77,159,255,0.12);color:var(--blue);border:1px solid rgba(77,159,255,0.2)}
.log-tag.err{background:var(--red-dim);color:var(--red);border:1px solid rgba(239,68,68,0.2)}
.log-tag.ok{background:var(--green-dim);color:var(--green);border:1px solid rgba(34,197,94,0.2)}
.log-tag.out{background:rgba(255,255,255,0.04);color:var(--text3);border:1px solid var(--border)}
.log-tag.in{background:var(--teal-dim);color:var(--teal);border:1px solid rgba(0,212,170,0.2)}
.log-msg{flex:1;word-break:break-all;color:var(--text2)}
.log-msg.sys{color:var(--blue)}.log-msg.err{color:var(--red)}.log-msg.ok{color:var(--green)}.log-msg.in{color:var(--teal)}
.term-input-row{display:flex;align-items:center;gap:8px;background:rgba(0,0,0,0.3);border-top:1px solid var(--border);padding:10px 14px}
.term-input-row:focus-within{background:rgba(0,212,170,0.02);border-top-color:rgba(0,212,170,0.15)}
.term-prompt{font-family:var(--mono);font-size:14px;color:var(--teal);flex-shrink:0}
.term-input{flex:1;background:none;border:none;outline:none;font-family:var(--mono);font-size:12px;color:var(--text);caret-color:var(--teal)}
.term-input::placeholder{color:var(--text3)}
.form-group{margin-bottom:16px}
.form-label{font-size:12px;font-weight:600;color:var(--text2);display:block;margin-bottom:6px}
.form-input,.form-select,.form-textarea{width:100%;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius-sm);padding:9px 12px;font-size:13px;color:var(--text);outline:none;font-family:var(--font);transition:border-color .15s}
.form-input:focus,.form-select:focus,.form-textarea:focus{border-color:rgba(0,212,170,0.4)}
.form-input::placeholder,.form-textarea::placeholder{color:var(--text3)}
.form-select option{background:var(--surface2)}
.form-textarea{resize:vertical;min-height:110px;line-height:1.6}
.form-grid-2{display:grid;grid-template-columns:1fr 1fr;gap:14px}
.help-text{font-size:11px;color:var(--text3);margin-top:5px}
.help-text.info{color:rgba(0,212,170,0.6);padding:7px 10px;background:var(--teal-dim);border-left:2px solid rgba(0,212,170,0.3);border-radius:0 4px 4px 0;margin-top:8px}
.btn{display:inline-flex;align-items:center;gap:6px;padding:8px 16px;border-radius:var(--radius-sm);font-size:12px;font-weight:600;border:1px solid;cursor:pointer;transition:all .15s;font-family:var(--font);white-space:nowrap;touch-action:manipulation}
.btn:active{transform:scale(0.97)}
.btn:disabled{opacity:.4;cursor:not-allowed;pointer-events:none}
.btn.loading{opacity:.6;pointer-events:none;position:relative}
.btn-teal{background:rgba(0,212,170,0.12);color:var(--teal);border-color:rgba(0,212,170,0.3)}
.btn-teal:hover{background:rgba(0,212,170,0.2);border-color:rgba(0,212,170,0.5)}
.btn-green{background:var(--green-dim);color:var(--green);border-color:rgba(34,197,94,0.3)}
.btn-green:hover{background:rgba(34,197,94,0.18);border-color:rgba(34,197,94,0.5)}
.btn-red{background:var(--red-dim);color:var(--red);border-color:rgba(239,68,68,0.3)}
.btn-red:hover{background:rgba(239,68,68,0.2);border-color:rgba(239,68,68,0.5)}
.btn-orange{background:var(--orange-dim);color:var(--orange);border-color:rgba(249,115,22,0.3)}
.btn-orange:hover{background:rgba(249,115,22,0.2);border-color:rgba(249,115,22,0.5)}
.btn-ghost{background:var(--surface2);color:var(--text2);border-color:var(--border)}
.btn-ghost:hover{background:var(--surface3);border-color:var(--border2);color:var(--text)}
.btn-sm{padding:6px 12px;font-size:11px}
.btn-row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.icon-btn{width:28px;height:28px;display:inline-flex;align-items:center;justify-content:center;border-radius:5px;border:1px solid var(--border);background:var(--surface2);color:var(--text2);cursor:pointer;transition:all .14s;font-size:12px;padding:0;touch-action:manipulation}
.icon-btn:hover{border-color:var(--border2);color:var(--text)}
.icon-btn.red{color:var(--red);border-color:rgba(239,68,68,0.25);background:var(--red-dim)}
.icon-btn.red:hover{background:rgba(239,68,68,0.2);border-color:rgba(239,68,68,0.5)}
.icon-btn.teal{color:var(--teal);border-color:rgba(0,212,170,0.2);background:var(--teal-dim)}
.icon-btn.teal:hover{background:rgba(0,212,170,0.2);border-color:rgba(0,212,170,0.4)}
.file-table-wrap{overflow-x:auto;-webkit-overflow-scrolling:touch}
.file-table{width:100%;border-collapse:collapse}
.file-table th{font-size:10px;font-weight:600;letter-spacing:.06em;text-transform:uppercase;color:var(--text3);padding:9px 14px;border-bottom:1px solid var(--border);text-align:left;background:rgba(0,0,0,0.15)}
.file-table td{padding:8px 14px;border-bottom:1px solid var(--border);font-size:12.5px;vertical-align:middle}
.file-table tr:last-child td{border-bottom:none}
.file-table tr:hover td{background:rgba(255,255,255,0.015)}
.fn-cell{display:flex;align-items:center;gap:7px}
.fn-icon{font-size:13px;opacity:.65;flex-shrink:0}
.fn-link{color:var(--teal);cursor:pointer;transition:color .15s;font-family:var(--mono);font-size:12px}
.fn-link:hover{color:#fff}
.fn-rename{display:none;align-items:center;gap:5px;flex:1}
.fn-rename.on{display:flex}
.fn-rename-input{flex:1;background:var(--surface2);border:1px solid rgba(0,212,170,0.3);border-radius:4px;padding:4px 8px;font-family:var(--mono);font-size:11px;color:var(--teal);outline:none}
.fn-rename-ok,.fn-rename-cancel{font-size:10px;padding:3px 7px;border-radius:3px;cursor:pointer;font-family:var(--font);font-weight:600;border:1px solid;flex-shrink:0;transition:all .14s}
.fn-rename-ok{background:var(--teal-dim);color:var(--teal);border-color:rgba(0,212,170,0.3)}
.fn-rename-cancel{background:transparent;color:var(--text3);border-color:var(--border)}
.ext-badge{font-size:9px;letter-spacing:.05em;text-transform:uppercase;padding:2px 6px;border-radius:4px;border:1px solid var(--border);background:var(--surface2);color:var(--text3);font-family:var(--mono)}
.file-actions{display:flex;gap:4px;align-items:center}
.env-row{display:grid;grid-template-columns:1fr 1.5fr auto;gap:6px;margin-bottom:6px;align-items:center}
.env-field{background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius-sm);padding:7px 10px;font-family:var(--mono);font-size:11px;color:var(--text);outline:none;width:100%;transition:border-color .15s}
.env-field:focus{border-color:rgba(0,212,170,0.3)}
.env-key{color:var(--teal)}
.drop-zone{border:2px dashed rgba(0,212,170,0.2);padding:28px 18px;text-align:center;transition:all .25s;background:rgba(0,212,170,0.02);border-radius:var(--radius-lg);cursor:pointer}
.drop-zone.dragging{border-color:var(--teal);background:rgba(0,212,170,0.05)}
.drop-icon{font-size:32px;margin-bottom:10px;display:block;color:var(--text3);transition:all .25s}
.drop-zone.dragging .drop-icon{color:var(--teal);transform:translateY(-3px)}
.drop-title{font-size:15px;font-weight:600;color:var(--text);margin-bottom:5px}
.drop-sub{font-size:12px;color:var(--text3);margin-bottom:14px}
.upload-row{display:flex;align-items:center;gap:8px;padding:8px 12px;background:var(--surface2);border:1px solid var(--border);border-radius:6px;margin-top:5px;font-family:var(--mono);font-size:10px;color:var(--text2)}
.upload-bar-wrap{flex:1;height:3px;background:rgba(0,212,170,0.1);border-radius:2px;overflow:hidden}
.upload-bar-fill{height:100%;background:linear-gradient(90deg,var(--teal),var(--blue));border-radius:2px;transition:width .1s}
.subuser-row{display:flex;justify-content:space-between;align-items:center;padding:8px 12px;background:var(--surface2);border:1px solid var(--border);border-radius:6px;margin-bottom:4px}
.danger-zone{border:1px solid rgba(239,68,68,0.2);border-left:3px solid var(--red);background:rgba(239,68,68,0.04);padding:16px;margin-top:16px;border-radius:var(--radius-lg)}
.res-item{margin-bottom:20px}
.res-header{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:8px}
.res-label{font-size:12px;font-weight:500;color:var(--text2)}
.res-value{font-size:18px;font-weight:700;color:var(--teal)}
.res-track{height:5px;background:rgba(0,0,0,0.4);border:1px solid var(--border);border-radius:3px;overflow:hidden}
.res-fill{height:100%;border-radius:3px;transition:width 1s ease}
.modal-veil{display:none;position:fixed;inset:0;background:rgba(0,0,0,0.75);z-index:10000;align-items:center;justify-content:center;backdrop-filter:blur(6px);padding:12px}
.modal-veil.open{display:flex;animation:fadein .2s ease}
@keyframes fadein{from{opacity:0}to{opacity:1}}
.modal-box{background:var(--surface);border:1px solid var(--border2);border-top:2px solid var(--teal);border-radius:var(--radius-lg);padding:26px;width:100%;max-width:500px;max-height:90vh;overflow-y:auto;box-shadow:0 20px 60px rgba(0,0,0,0.8);animation:slideUp .25s cubic-bezier(0.16,1,0.3,1)}
.modal-box.wide{max-width:900px}
@keyframes slideUp{from{transform:translateY(16px) scale(0.97);opacity:0}to{transform:translateY(0) scale(1);opacity:1}}
.modal-title{font-size:17px;font-weight:700;color:var(--text);margin-bottom:20px;display:flex;align-items:center;gap:8px}
.modal-footer{display:flex;justify-content:flex-end;gap:8px;margin-top:20px;padding-top:16px;border-top:1px solid var(--border)}
#loginOverlay{position:fixed;inset:0;background:var(--bg);z-index:99999;display:flex;align-items:center;justify-content:center;padding:12px}
#loginOverlay::before{content:'';position:absolute;inset:0;pointer-events:none;background:radial-gradient(ellipse 60% 50% at 30% 30%,rgba(0,212,170,0.06) 0%,transparent 60%),radial-gradient(ellipse 50% 60% at 80% 70%,rgba(77,159,255,0.04) 0%,transparent 60%)}
.login-card{background:var(--surface);border:1px solid var(--border);border-top:2px solid var(--teal);padding:36px 32px;width:100%;max-width:380px;border-radius:var(--radius-lg);box-shadow:0 20px 60px rgba(0,0,0,0.7);position:relative;z-index:1;animation:slideUp .5s cubic-bezier(0.16,1,0.3,1)}
.login-logo-row{display:flex;align-items:center;gap:12px;margin-bottom:6px}
.login-logo-icon{width:40px;height:40px;background:linear-gradient(135deg,var(--teal),#0099cc);border-radius:10px;display:flex;align-items:center;justify-content:center;font-size:18px;font-weight:700;color:#000}
.login-logo-text{font-size:22px;font-weight:800;color:var(--text);letter-spacing:-.5px}
.login-tagline{font-size:12px;color:var(--text3);margin-bottom:24px;font-family:var(--mono)}
.auth-tabs{display:flex;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius-sm);padding:3px;margin-bottom:20px}
.auth-tab{flex:1;text-align:center;padding:7px 10px;font-size:12px;font-weight:600;color:var(--text3);cursor:pointer;border-radius:5px;transition:all .2s;border:1px solid transparent}
.auth-tab.active{color:var(--teal);background:rgba(0,212,170,0.1);border-color:rgba(0,212,170,0.2)}
.remember-row{display:flex;align-items:center;gap:8px;margin-bottom:14px;cursor:pointer;user-select:none;-webkit-user-select:none}
.remember-check{width:16px;height:16px;border:1.5px solid var(--border2);border-radius:4px;background:var(--surface2);display:flex;align-items:center;justify-content:center;transition:all .18s;flex-shrink:0}
.remember-check.on{background:var(--teal-dim);border-color:rgba(0,212,170,0.5)}
.remember-check.on::after{content:'\2713';font-size:9px;color:var(--teal);font-weight:700}
.remember-label{font-size:12px;color:var(--text3)}
.runtime-switcher{display:flex;align-items:center;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius-sm);padding:3px;gap:2px}
.runtime-btn{font-family:var(--mono);font-size:10px;font-weight:600;padding:5px 10px;border-radius:4px;border:1px solid transparent;background:transparent;color:var(--text3);cursor:pointer;transition:all .15s;white-space:nowrap}
.runtime-btn:hover{color:var(--text);background:var(--surface3)}
.runtime-btn.active.rt-py{background:rgba(34,197,94,0.1);color:var(--green);border-color:rgba(34,197,94,0.28)}
.runtime-btn.active.rt-js{background:rgba(234,179,8,0.1);color:#eab308;border-color:rgba(234,179,8,0.3)}
.runtime-btn.active.rt-ts{background:var(--blue-dim);color:var(--blue);border-color:rgba(77,159,255,0.3)}
.runtime-btn.active.rt-rs{background:var(--orange-dim);color:var(--orange);border-color:rgba(249,115,22,0.3)}
.runtime-btn.active.rt-cs{background:var(--purple-dim);color:var(--purple);border-color:rgba(168,85,247,0.3)}
.runtime-btn.active.rt-sh{background:var(--blue-dim);color:var(--blue);border-color:rgba(77,159,255,0.3)}
.code-editor{width:100%;min-height:440px;background:rgba(0,0,0,0.5);border:1px solid var(--border);border-left:3px solid var(--teal);border-radius:var(--radius-sm);padding:14px;font-family:var(--mono);font-size:12.5px;color:var(--text);outline:none;resize:vertical;line-height:1.7;caret-color:var(--teal);transition:border-color .18s;tab-size:4;-moz-tab-size:4}
.code-editor:focus{border-color:rgba(0,212,170,0.3);border-left-color:var(--teal)}
.toast-tray{position:fixed;bottom:20px;right:20px;z-index:20000;display:flex;flex-direction:column;gap:8px;pointer-events:none}
.toast{background:var(--surface2);border:1px solid var(--border2);border-radius:var(--radius);padding:11px 14px;font-size:13px;color:var(--text);display:flex;align-items:center;gap:9px;box-shadow:0 8px 24px rgba(0,0,0,0.6);pointer-events:all;min-width:220px;animation:ti .28s cubic-bezier(0.16,1,0.3,1);position:relative;overflow:hidden}
.toast::before{content:'';position:absolute;left:0;top:0;bottom:0;width:3px;border-radius:3px 0 0 3px}
.toast.success::before{background:var(--green)}.toast.error::before{background:var(--red)}.toast.info::before{background:var(--teal)}
.toast-icon{width:20px;height:20px;border-radius:4px;display:flex;align-items:center;justify-content:center;font-size:11px;flex-shrink:0}
.toast.success .toast-icon{background:var(--green-dim);color:var(--green)}
.toast.error .toast-icon{background:var(--red-dim);color:var(--red)}
.toast.info .toast-icon{background:var(--teal-dim);color:var(--teal)}
.toast-close{margin-left:auto;cursor:pointer;color:var(--text3);font-size:13px;transition:color .15s}
.toast-close:hover{color:var(--text)}
@keyframes ti{from{transform:translateX(12px);opacity:0}to{transform:translateX(0);opacity:1}}
.empty-state{text-align:center;padding:48px 24px;color:var(--text3)}
.empty-icon{font-size:40px;margin-bottom:12px;opacity:.5}
.empty-title{font-size:15px;font-weight:600;color:var(--text2);margin-bottom:6px}
.empty-sub{font-size:13px;margin-bottom:20px}
.sidebar-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,0.7);z-index:150;cursor:pointer}
@media(max-width:860px){
  .sidebar{position:fixed;left:0;top:0;bottom:0;transform:translateX(-110%);z-index:200;width:260px}
  .sidebar.open{transform:translateX(0)}
  .sidebar-overlay{display:block;opacity:0;pointer-events:none;transition:opacity .3s}
  .sidebar-overlay.open{opacity:1;pointer-events:all}
  .mobile-menu-btn{display:flex}
  .page-area{padding:14px}
  .stats-row{grid-template-columns:1fr 1fr;gap:10px}
  .form-grid-2{grid-template-columns:1fr}
  .bots-grid{grid-template-columns:1fr}
  .topbar{padding:0 14px}
  .topbar-bot-name{display:none}
  .terminal{max-height:50vh}
  .toast-tray{bottom:12px;right:8px;left:8px}
  .toast{min-width:0;width:100%}
  .runtime-switcher{display:none}
}
@media(max-width:480px){
  .stats-row{grid-template-columns:1fr}
  .runtime-switcher{gap:1px}
  .runtime-btn{padding:4px 7px;font-size:9px}
  .login-card{padding:24px 18px}
  .uptime-badge{display:none !important}
}
</style>
</head>
<body>

<!-- LOGIN OVERLAY -->
<div id="loginOverlay">
  <div class="login-card">
    <div class="login-logo-row">
      <div class="login-logo-icon">V</div>
      <div class="login-logo-text">Vortex Hosting</div>
    </div>
    <div class="login-tagline">// Hosting Platform v12.1</div>
    <div class="auth-tabs">
      <div class="auth-tab active" id="tabLogin" onclick="switchAuthMode('login')">Login</div>
      <div class="auth-tab" id="tabRegister" onclick="switchAuthMode('register')">Register</div>
    </div>
    <div class="form-group" style="margin-bottom:10px">
      <label class="form-label">Username</label>
      <input class="form-input" id="authUsername" placeholder="Enter username" autocomplete="username"
        onkeydown="if(event.key==='Enter')submitAuth()">
    </div>
    <div class="form-group" style="margin-bottom:14px">
      <label class="form-label">Password</label>
      <input type="password" class="form-input" id="authPassword" placeholder="Enter password"
        autocomplete="current-password" onkeydown="if(event.key==='Enter')submitAuth()">
    </div>
    <div class="remember-row" onclick="toggleRememberMe()">
      <div class="remember-check" id="rememberCheck"></div>
      <span class="remember-label">Remember me for 30 days</span>
    </div>
    <button class="btn btn-teal" id="authBtn" style="width:100%;padding:11px;font-size:13px;justify-content:center" onclick="submitAuth()">
      Sign In
    </button>
    <p style="font-size:11px;color:var(--text3);text-align:center;margin-top:12px">Instances persist across server restarts</p>
  </div>
</div>

<div class="sidebar-overlay" id="sidebarOverlay" onclick="toggleSidebar()"></div>

<div id="app">
  <!-- SIDEBAR -->
  <aside class="sidebar" id="sidebar">
    <div class="logo-area">
      <div class="logo-icon">V</div>
      <div>
        <div class="logo-text">Vortex Hosting</div>
        <div class="logo-sub">v12.1</div>
      </div>
    </div>

    <div style="padding:8px 0">
      <div class="nav-section-label">Menu</div>
      <div class="nav-item active" data-page="projects" onclick="navTo('projects',this)">
        <span class="nav-icon">&#8862;</span> My Projects
      </div>
      <div class="nav-item" data-page="console" onclick="navTo('console',this)">
        <span class="nav-icon">_</span> Console
      </div>
      <div class="nav-item" data-page="resources" onclick="navTo('resources',this)">
        <span class="nav-icon">&#9635;</span> Resources
      </div>
      <div class="nav-section-label" style="margin-top:4px">Account</div>
      <div class="nav-item disabled" data-page="referral">
        <span class="nav-icon">&#9672;</span> Referral
      </div>
      <div class="nav-item disabled" data-page="orders">
        <span class="nav-icon">&#8801;</span> Orders
      </div>
      <div class="nav-item disabled" data-page="support">
        <span class="nav-icon">&#9678;</span> Support
      </div>
      <div class="nav-item" data-page="settings" onclick="navTo('settings',this)">
        <span class="nav-icon">&#9881;</span> Account
      </div>
    </div>

    <div style="flex:1;display:flex;flex-direction:column;overflow:hidden;border-top:1px solid var(--border);padding-top:10px">
      <div style="padding:0 12px 6px;display:flex;justify-content:space-between;align-items:center">
        <span style="font-size:10px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:var(--text3)">Instances</span>
        <span class="instance-count-badge" id="botCount">0</span>
      </div>
      <button class="new-bot-btn" style="margin:0 8px 8px" onclick="openCreateModal()">
        <span style="font-size:16px;line-height:1">+</span> Deploy New Instance
      </button>
      <div class="bot-list" id="botList"></div>
    </div>

    <div class="sidebar-footer">
      <div class="user-info">
        <div class="user-avatar" id="userAvatar">U</div>
        <span class="user-name" id="userName">&mdash;</span>
      </div>
      <button class="logout-btn" onclick="logout()">Sign out</button>
    </div>
  </aside>

  <!-- MAIN CONTENT -->
  <main class="main">
    <div class="topbar">
      <button class="mobile-menu-btn" onclick="toggleSidebar()">&#9776;</button>
      <div class="topbar-title" id="topbarTitle">My Projects</div>
      <span class="topbar-bot-name" id="topbarBotName"></span>
      <div class="uptime-badge" id="uptimeBadge">&#9650; <span id="uptimeText">0s</span></div>
      <div style="margin-left:auto;display:flex;align-items:center;gap:8px">
        <div class="runtime-switcher" id="runtimeSwitcher">
          <button class="runtime-btn rt-py active" onclick="setRuntime('py')">PY</button>
          <button class="runtime-btn rt-js" onclick="setRuntime('js')">JS</button>
          <button class="runtime-btn rt-ts" onclick="setRuntime('ts')">TS</button>
          <button class="runtime-btn rt-sh" onclick="setRuntime('sh')">SH</button>
          <button class="runtime-btn rt-rs" onclick="setRuntime('rs')">RS</button>
          <button class="runtime-btn rt-cs" onclick="setRuntime('cs')">C#</button>
        </div>
        <div class="status-pill offline" id="statusPill">
          <div class="status-led"></div>
          <span id="statusText">Offline</span>
        </div>
      </div>
    </div>

    <div class="page-area">

      <!-- PROJECTS PAGE -->
      <div class="page active" id="page-projects">
        <div class="page-header">
          <div class="page-header-top">
            <div>
              <div class="page-title">My Projects</div>
              <div class="page-subtitle">Manage your servers and applications</div>
            </div>
            <button class="btn btn-teal" onclick="openCreateModal()">+ Create Project</button>
          </div>
        </div>
        <div class="stats-row">
          <div class="stat-card">
            <div class="stat-icon-wrap" style="background:rgba(77,159,255,0.1)"><span style="font-size:20px">&#8862;</span></div>
            <div class="stat-info">
              <div class="stat-number" style="color:var(--blue)" id="statTotal">0</div>
              <div class="stat-label">Total Projects</div>
            </div>
          </div>
          <div class="stat-card">
            <div class="stat-icon-wrap" style="background:var(--green-dim)"><span style="font-size:20px">&#10003;</span></div>
            <div class="stat-info">
              <div class="stat-number" style="color:var(--green)" id="statActive">0</div>
              <div class="stat-label">Active Projects</div>
            </div>
          </div>
          <div class="stat-card">
            <div class="stat-icon-wrap" style="background:var(--blue-dim)"><span style="font-size:20px">&#9654;</span></div>
            <div class="stat-info">
              <div class="stat-number" style="color:var(--blue)" id="statVps">0</div>
              <div class="stat-label">VPS Instances</div>
            </div>
          </div>
        </div>
        <div class="search-row">
          <div class="search-box">
            <span class="search-icon">&#8981;</span>
            <input class="search-input" id="searchInput" placeholder="Search for a bot by name..." oninput="filterBots(this.value)">
          </div>
          <div class="filter-tabs" id="filterTabs">
            <button class="filter-tab active" onclick="setFilter('all',this)" data-filter="all">All</button>
            <button class="filter-tab" onclick="setFilter('bots',this)" data-filter="bots">Bots</button>
            <button class="filter-tab" onclick="setFilter('vps',this)" data-filter="vps">VPS</button>
          </div>
          <button class="btn btn-ghost btn-sm" onclick="sortBots()">&#8693; Name</button>
        </div>
        <div class="bots-grid" id="botsGrid"></div>
      </div>

      <!-- MANAGE PAGE -->
      <div class="page" id="page-manage">
        <div class="page-header">
          <div class="page-header-top">
            <div>
              <div class="page-title" id="manageBotTitle">Bot Name</div>
              <div class="page-subtitle">Manage instance &middot; <span id="manageBotId" style="font-family:var(--mono);font-size:11px"></span></div>
            </div>
            <div class="btn-row">
              <button class="btn btn-ghost btn-sm" onclick="navTo('projects',null)">&larr; Back</button>
              <button class="btn btn-green btn-sm" id="btnStartTop" onclick="startBot()">&#9654; Start</button>
              <button class="btn btn-red btn-sm" id="btnStopTop" onclick="stopBot()">&#9632; Stop</button>
              <button class="btn btn-orange btn-sm" id="btnRestartTop" onclick="restartBot()">&#8634; Restart</button>
            </div>
          </div>
        </div>
        <div class="panel">
          <div class="panel-head">
            <div class="panel-title"><div class="panel-title-icon">&#9654;</div> Launch Control</div>
          </div>
          <div class="hint">Set the entry point file then press Start.</div>
          <div class="panel-body">
            <div style="max-width:440px;margin-bottom:14px">
              <div class="form-group" style="margin:0">
                <label class="form-label">Startup File</label>
                <input class="form-input" id="sfInput" value="main.py" placeholder="main.py / index.js / start.sh / src/main.rs">
                <p class="help-text">File Vortex runs when you press Start.</p>
              </div>
            </div>
            <div class="btn-row">
              <button class="btn btn-green" id="btnStart" onclick="startBot()">&#9654; Start Process</button>
              <button class="btn btn-red" id="btnStop" onclick="stopBot()">&#9632; Stop</button>
              <button class="btn btn-orange" id="btnRestart" onclick="restartBot()">&#8634; Restart</button>
              <button class="btn btn-ghost" style="margin-left:auto;color:var(--red)" onclick="killBot()">&#10005; Force Kill</button>
            </div>
          </div>
        </div>
        <div class="panel">
          <div class="panel-head">
            <div class="panel-title"><div class="panel-title-icon">&#8801;</div> Live Output</div>
            <button class="btn btn-ghost btn-sm" onclick="navTo('console',null)">Full Console &rarr;</button>
          </div>
          <div class="term-header">
            <div class="term-dots"><div class="term-dot" style="background:#ff5f57"></div><div class="term-dot" style="background:#ffbd2e"></div><div class="term-dot" style="background:#28ca42"></div></div>
            <div class="term-label">stdout // live</div>
          </div>
          <div class="terminal" id="miniTerm" style="height:180px"></div>
        </div>
        <div class="filter-tabs" style="margin-bottom:14px" id="manageTabBar">
          <button class="filter-tab active" onclick="showManageTab('files',this)">Files</button>
          <button class="filter-tab" onclick="showManageTab('env',this)">Environment</button>
          <button class="filter-tab" onclick="showManageTab('cfg',this)">Settings</button>
        </div>
        <div id="manageTab-files">
          <input type="file" multiple id="fileUploadInput" style="display:none" onchange="handleUpload(this.files,false)">
          <input type="file" webkitdirectory directory multiple id="folderUploadInput" style="display:none" onchange="handleUpload(this.files,true)">
          <div class="panel">
            <div class="panel-head">
              <div class="panel-title"><div class="panel-title-icon">&#8801;</div> File Manager</div>
              <div class="btn-row">
                <button class="btn btn-ghost btn-sm" onclick="openNewFileModal()">+ New File</button>
                <button class="btn btn-teal btn-sm" onclick="loadFiles()">&#8635; Refresh</button>
              </div>
            </div>
            <div class="file-table-wrap">
              <table class="file-table">
                <thead><tr><th>Name</th><th>Type</th><th>Size</th><th>Modified</th><th>Actions</th></tr></thead>
                <tbody id="fileList"></tbody>
              </table>
            </div>
            <div style="padding:14px">
              <div class="drop-zone" id="dropZone">
                <span class="drop-icon">&#8679;</span>
                <div class="drop-title">Drop files here</div>
                <div class="drop-sub">ZIP files are auto-extracted</div>
                <div style="display:flex;gap:8px;justify-content:center;flex-wrap:wrap;position:relative;z-index:5">
                  <button class="btn btn-teal btn-sm" onclick="event.stopPropagation();document.getElementById('fileUploadInput').click()">&#8679; Upload Files</button>
                  <button class="btn btn-ghost btn-sm" onclick="event.stopPropagation();document.getElementById('folderUploadInput').click()">Upload Folder</button>
                </div>
              </div>
              <div id="uploadProgress" style="margin-top:8px"></div>
            </div>
          </div>
        </div>
        <div id="manageTab-env" style="display:none">
          <div class="panel">
            <div class="panel-head">
              <div class="panel-title"><div class="panel-title-icon">&#8859;</div> Environment Variables</div>
              <button class="btn btn-teal btn-sm" onclick="saveEnv()">Save</button>
            </div>
            <div class="hint">Key=value pairs injected at startup. Restart after saving.</div>
            <div class="panel-body">
              <div id="envRows"></div>
              <button class="btn btn-ghost btn-sm" onclick="addEnvRow('','')" style="margin-top:8px">+ Add Variable</button>
              <p class="help-text info" style="margin-top:10px">Restart your instance after saving for changes to take effect.</p>
            </div>
          </div>
        </div>
        <div id="manageTab-cfg" style="display:none">
          <div class="panel">
            <div class="panel-head">
              <div class="panel-title"><div class="panel-title-icon">&#9881;</div> Instance Configuration</div>
            </div>
            <div class="panel-body">
              <div class="form-grid-2">
                <div class="form-group">
                  <label class="form-label">Instance Name</label>
                  <input class="form-input" id="stName" placeholder="My Bot">
                </div>
                <div class="form-group">
                  <label class="form-label">Startup File</label>
                  <input class="form-input" id="stStartup" placeholder="main.py">
                </div>
              </div>
              <div class="form-group">
                <label class="form-label">Instance Type</label>
                <select class="form-select" id="stType">
                  <option value="bot">Bot</option>
                  <option value="vps">VPS</option>
                </select>
              </div>
              <div class="form-group">
                <label class="form-label">Crash Recovery</label>
                <select class="form-select" id="stAR">
                  <option value="false">Disabled</option>
                  <option value="true">Auto-restart on crash</option>
                </select>
              </div>
              <button class="btn btn-teal" onclick="saveSettings()">Save Configuration</button>
              <div id="accessMgmtSection" style="margin-top:20px;padding-top:20px;border-top:1px solid var(--border)">
                <div style="font-size:13px;font-weight:600;color:var(--text);margin-bottom:12px">Access Management</div>
                <div class="form-group">
                  <label class="form-label">Grant Access</label>
                  <div style="display:flex;gap:8px">
                    <input class="form-input" id="newSubuser" placeholder="Enter username...">
                    <button class="btn btn-orange" onclick="addSubuser()">Grant</button>
                  </div>
                  <p class="help-text">Shared users can start/stop and view logs.</p>
                </div>
                <div id="subuserList"></div>
              </div>
            </div>
          </div>
          <div class="danger-zone" id="dangerZoneSection">
            <div style="font-size:11px;font-weight:700;letter-spacing:.05em;color:var(--red);margin-bottom:6px;text-transform:uppercase">Danger Zone</div>
            <div style="font-size:12px;color:var(--text2);margin-bottom:12px">Permanently destroys this instance and all files.</div>
            <button class="btn btn-red" onclick="deleteBot()">&#10005; Destroy Instance</button>
          </div>
        </div>
      </div>

      <!-- CONSOLE PAGE -->
      <div class="page" id="page-console">
        <div class="panel">
          <div class="panel-head">
            <div class="panel-title"><div class="panel-title-icon">_</div> Process Console</div>
            <div class="btn-row">
              <button class="btn btn-ghost btn-sm" onclick="clearConsole()">&#8856; Clear</button>
              <button class="btn btn-ghost btn-sm" onclick="exportLogs()">&#8595; Export</button>
            </div>
          </div>
          <div class="hint">Type commands below. Use <code style="color:var(--teal)">$</code> prefix to force shell (e.g. <code style="color:var(--teal)">$ pip install discord.py</code>).</div>
          <div class="term-header">
            <div class="term-dots"><div class="term-dot" style="background:#ff5f57"></div><div class="term-dot" style="background:#ffbd2e"></div><div class="term-dot" style="background:#28ca42"></div></div>
            <div class="term-label" id="termTitle">NO INSTANCE SELECTED</div>
          </div>
          <div class="terminal" id="mainTerm" style="height:460px"></div>
          <div class="term-input-row">
            <span class="term-prompt">&#10095;</span>
            <input class="term-input" id="termIn" placeholder="Type command... (Prefix with $ for shell while online)" onkeydown="if(event.key==='Enter')sendInput()">
            <button class="btn btn-teal btn-sm" onclick="sendInput()">Send</button>
          </div>
        </div>
      </div>

      <!-- RESOURCES PAGE -->
      <div class="page" id="page-resources">
        <div class="panel">
          <div class="panel-head">
            <div class="panel-title"><div class="panel-title-icon">&#9635;</div> System Resources</div>
            <span style="font-size:11px;color:var(--teal);background:var(--teal-dim);border:1px solid rgba(0,212,170,0.2);padding:3px 10px;border-radius:20px">Live &middot; 4s</span>
          </div>
          <div class="hint">System-wide resource usage for the host machine.</div>
          <div class="panel-body">
            <div class="res-item">
              <div class="res-header"><span class="res-label">CPU</span><span class="res-value" id="rCpu">&mdash;</span></div>
              <div class="res-track"><div class="res-fill" id="pCpu" style="width:0%;background:linear-gradient(90deg,var(--teal),var(--blue))"></div></div>
            </div>
            <div class="res-item">
              <div class="res-header"><span class="res-label">Memory</span><span class="res-value" id="rMem">&mdash;</span></div>
              <div class="res-track"><div class="res-fill" id="pMem" style="width:0%;background:linear-gradient(90deg,var(--blue),var(--purple))"></div></div>
            </div>
            <div class="res-item">
              <div class="res-header"><span class="res-label">Disk</span><span class="res-value" id="rDsk">&mdash;</span></div>
              <div class="res-track"><div class="res-fill" id="pDsk" style="width:0%;background:linear-gradient(90deg,var(--orange),var(--red))"></div></div>
              <p class="help-text" style="margin-top:8px">Root partition. Instance files live in <code style="color:var(--teal);font-family:var(--mono)">vortex_bots/</code>.</p>
            </div>
          </div>
        </div>
      </div>

      <!-- SETTINGS PAGE -->
      <div class="page" id="page-settings">
        <div class="panel">
          <div class="panel-head">
            <div class="panel-title"><div class="panel-title-icon">&#9881;</div> Account Settings</div>
          </div>
          <div class="panel-body">
            <p style="color:var(--text2);font-size:13px">Logged in as <strong id="settingsUser" style="color:var(--teal)"></strong></p>
            <p style="color:var(--text3);font-size:12px;margin-top:8px">Additional account settings coming soon.</p>
            <button class="btn btn-red" style="margin-top:20px" onclick="logout()">Sign Out</button>
          </div>
        </div>
      </div>

    </div>
  </main>
</div>

<div class="toast-tray" id="toastTray"></div>

<!-- CREATE MODAL -->
<div class="modal-veil" id="mCreate">
  <div class="modal-box">
    <div class="modal-title">Deploy New Instance</div>
    <div class="form-group">
      <label class="form-label">Instance Name</label>
      <input class="form-input" id="mName" placeholder="My Discord Bot" onkeydown="if(event.key==='Enter')createBot()">
    </div>
    <div class="form-group">
      <label class="form-label">Instance Type</label>
      <select class="form-select" id="mType" onchange="onCreateTypeChange()">
        <option value="bot">Bot (Python/JS/Rust/C#)</option>
        <option value="vps">VPS (Linux Shell / Persistent)</option>
      </select>
    </div>
    <div class="form-group">
      <label class="form-label">Startup File</label>
      <input class="form-input" id="mFile" value="main.py" placeholder="main.py / index.ts / main.sh" onkeydown="if(event.key==='Enter')createBot()">
      <p class="help-text" id="mFileHint">The file Vortex runs when you press Start.</p>
    </div>
    <p class="help-text info">After creating, go to File Manager to upload your code.</p>
    <div class="modal-footer">
      <button class="btn btn-ghost" onclick="closeModal('mCreate')">Cancel</button>
      <button class="btn btn-teal" onclick="createBot()">Initialize Instance</button>
    </div>
  </div>
</div>

<!-- EDITOR MODAL -->
<div class="modal-veil" id="mEditor">
  <div class="modal-box wide">
    <div class="modal-title">Edit <span id="edName" style="color:var(--teal)">file</span></div>
    <textarea class="code-editor" id="edContent"></textarea>
    <div class="modal-footer">
      <button class="btn btn-ghost" onclick="closeModal('mEditor')">Discard</button>
      <button class="btn btn-teal" onclick="saveFile()">Save Changes</button>
    </div>
  </div>
</div>

<!-- NEW FILE MODAL -->
<div class="modal-veil" id="mNewFile">
  <div class="modal-box">
    <div class="modal-title">Create File</div>
    <div class="form-group">
      <label class="form-label">Filename</label>
      <input class="form-input" id="nfName" placeholder="e.g. src/app.py or config.json">
      <p class="help-text">Use slashes for subdirectories: <code style="color:var(--teal);font-family:var(--mono)">cogs/admin.py</code></p>
    </div>
    <div class="form-group">
      <label class="form-label">Initial Content</label>
      <textarea class="form-textarea" id="nfContent" placeholder="# Start writing..." style="height:120px"></textarea>
    </div>
    <div class="modal-footer">
      <button class="btn btn-ghost" onclick="closeModal('mNewFile')">Cancel</button>
      <button class="btn btn-teal" onclick="createNewFile()">Create File</button>
    </div>
  </div>
</div>

<script>
/* == RUNTIME ================================================================ */
var RUNTIMES={py:{ext:'py',defaultFile:'main.py'},js:{ext:'js',defaultFile:'index.js'},ts:{ext:'ts',defaultFile:'index.ts'},sh:{ext:'sh',defaultFile:'start.sh'},rs:{ext:'rs',defaultFile:'src/main.rs'},cs:{ext:'cs',defaultFile:'Program.cs'}};
var currentRuntime='py';
function setRuntime(rt){
  if(!RUNTIMES[rt])return;
  currentRuntime=rt;
  document.querySelectorAll('.runtime-btn').forEach(function(b){b.classList.remove('active')});
  var btn=document.querySelector('.runtime-btn.rt-'+rt);if(btn)btn.classList.add('active');
  var def=RUNTIMES[rt].defaultFile;
  ['sfInput','mFile','stStartup'].forEach(function(id){var el=document.getElementById(id);if(el&&!el._userEdited)el.value=def;});
  toast('Runtime: '+rt.toUpperCase()+' \u00b7 '+def,'info');
}
function detectRuntime(sf){
  if(!sf)return;
  var ext=sf.split('.').pop().toLowerCase();
  var map={py:'py',js:'js',ts:'ts',rs:'rs',cs:'cs',sh:'sh'};
  var rt=map[ext];if(rt&&rt!==currentRuntime)setRuntime(rt);
}

/* == CREATE TYPE CHANGE ===================================================== */
function onCreateTypeChange(){
  var t=document.getElementById('mType').value;
  var f=document.getElementById('mFile');
  var h=document.getElementById('mFileHint');
  if(t==='vps'){
    f.value='start.sh';
    if(h)h.textContent='For VPS instances, use a shell script as the entry point.';
  } else {
    f.value=RUNTIMES[currentRuntime]?RUNTIMES[currentRuntime].defaultFile:'main.py';
    if(h)h.textContent='The file Vortex runs when you press Start.';
  }
}

/* == REMEMBER ME ============================================================ */
var rememberMe=false;
function toggleRememberMe(){rememberMe=!rememberMe;var el=document.getElementById('rememberCheck');if(el)el.classList.toggle('on',rememberMe);}

/* == SOCKET ================================================================= */
var sock;
try{
  sock=io({transports:['websocket','polling'],reconnectionAttempts:10,reconnectionDelay:1500,timeout:10000});
  sock.on('console_log',function(d){try{if(d&&d.bot_id===curBot)appendLog(d.msg,d.level);}catch(e){}});
  sock.on('status_update',function(d){try{
    if(!d)return;
    if(botRegistry[d.bot_id])botRegistry[d.bot_id].status=d.status;
    renderBotsGrid();renderBotList();
    if(d.bot_id===curBot)applyStatus(d.status);
    if(d.status==='online'&&d.start_time)startTimes[d.bot_id]=d.start_time*1000;else delete startTimes[d.bot_id];
    updateUptime();
  }catch(e){console.error('status_update error:',e);}});
  sock.on('files_changed',function(d){try{if(d&&d.bot_id===curBot&&document.getElementById('page-manage')&&document.getElementById('page-manage').classList.contains('active'))loadFiles();}catch(e){}});
}catch(e){sock={emit:function(){},on:function(){}};}

var curBot=null,botRegistry={},startTimes={},uptimeIv=null,resIv=null;
var currentUser='',authMode='login';
var _searchQuery='',_currentFilter='all',_sortDir=true;
var _renameMap={};

/* == UPTIME TIMER =========================================================== */
function startUptimeTimer(){
  stopUptimeTimer();
  updateUptime();
  uptimeIv=setInterval(updateUptime,1000);
}
function stopUptimeTimer(){
  if(uptimeIv){clearInterval(uptimeIv);uptimeIv=null;}
  var badge=document.getElementById('uptimeBadge');
  if(badge)badge.classList.remove('visible');
}
function updateUptime(){
  var badge=document.getElementById('uptimeBadge');
  var text=document.getElementById('uptimeText');
  if(!badge||!text)return;
  if(!curBot||!startTimes[curBot]){badge.classList.remove('visible');return;}
  var elapsed=Math.floor((Date.now()-startTimes[curBot])/1000);
  if(elapsed<0)elapsed=0;
  var h=Math.floor(elapsed/3600),m=Math.floor((elapsed%3600)/60),s=elapsed%60;
  var str='';
  if(h>0)str+=h+'h ';
  if(m>0||h>0)str+=m+'m ';
  str+=s+'s';
  text.textContent=str;
  badge.classList.add('visible');
}

/* == SIDEBAR ================================================================ */
function toggleSidebar(){
  var sb=document.getElementById('sidebar'),o=document.getElementById('sidebarOverlay');
  if(!sb||!o)return;
  var open=sb.classList.toggle('open');
  o.classList.toggle('open',open);
}

/* == AUTH ==================================================================== */
function checkAuth(){
  return fetch('/api/me').then(function(r){
    if(r.status===401){document.getElementById('loginOverlay').style.display='flex';return false;}
    return r.json().then(function(d){
      currentUser=d.username;
      document.getElementById('loginOverlay').style.display='none';
      document.getElementById('userName').textContent=d.username;
      document.getElementById('settingsUser').textContent=d.username;
      var av=document.getElementById('userAvatar');if(av)av.textContent=(d.username[0]||'U').toUpperCase();
      return true;
    });
  }).catch(function(){document.getElementById('loginOverlay').style.display='flex';return false;});
}
function switchAuthMode(mode){
  authMode=mode;
  document.getElementById('tabLogin').classList.toggle('active',mode==='login');
  document.getElementById('tabRegister').classList.toggle('active',mode==='register');
  document.getElementById('authBtn').textContent=mode==='login'?'Sign In':'Create Account';
}
function submitAuth(){
  var u=document.getElementById('authUsername').value.trim(),p=document.getElementById('authPassword').value;
  if(!u||!p){toast('Username and password required','error');return;}
  var ep=authMode==='login'?'/api/login':'/api/register';
  var btn=document.getElementById('authBtn');if(btn)btn.classList.add('loading');
  fetch(ep,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:u,password:p,remember_me:rememberMe})})
  .then(function(r){return r.json().then(function(res){
    if(btn)btn.classList.remove('loading');
    if(r.ok)location.reload();else toast(res.error||'Authentication failed','error');
  });})
  .catch(function(){if(btn)btn.classList.remove('loading');toast('Network error','error');});
}
function logout(){fetch('/api/logout',{method:'POST'}).catch(function(){}).then(function(){location.reload();});}

/* == NAV ==================================================================== */
var PAGE_TITLES={projects:'My Projects',manage:'Manage Instance',console:'Console',resources:'Resources',settings:'Account'};
function navTo(name,el){
  try{
    document.querySelectorAll('.sidebar .nav-item').forEach(function(n){n.classList.remove('active')});
    var d=document.querySelector('.sidebar .nav-item[data-page="'+name+'"]');if(d)d.classList.add('active');
    document.querySelectorAll('.page').forEach(function(p){p.classList.remove('active')});
    var pg=document.getElementById('page-'+name);
    if(pg)pg.classList.add('active');
    else{var fb=document.getElementById('page-projects');if(fb)fb.classList.add('active');}
    var tt=document.getElementById('topbarTitle');if(tt)tt.textContent=PAGE_TITLES[name]||name;
    if(name==='resources')startRes();else stopRes();
    if(name==='console')loadBotLogs();
    if(name==='projects'){renderBotsGrid();updateStats();}
    if(name==='manage'){loadFiles();startUptimeTimer();}else{stopUptimeTimer();}
    if(window.innerWidth<=860&&document.getElementById('sidebar')&&document.getElementById('sidebar').classList.contains('open'))toggleSidebar();
  }catch(e){console.error('navTo error:',e);}
}
function showManageTab(tab,btn){
  ['files','env','cfg'].forEach(function(t){var el=document.getElementById('manageTab-'+t);if(el)el.style.display=t===tab?'':'none';});
  document.querySelectorAll('#manageTabBar .filter-tab').forEach(function(b){b.classList.remove('active')});
  if(btn)btn.classList.add('active');
  if(tab==='files')loadFiles();
  if(tab==='env')loadEnv();
  if(tab==='cfg')loadSettings();
}

/* == FILTER / SEARCH ======================================================== */
function setFilter(f,btn){
  _currentFilter=f;
  document.querySelectorAll('#filterTabs .filter-tab').forEach(function(b){b.classList.remove('active')});
  if(btn)btn.classList.add('active');
  renderBotsGrid();
}
function filterBots(q){_searchQuery=q.toLowerCase();renderBotsGrid();}
function sortBots(){_sortDir=!_sortDir;renderBotsGrid();}

/* == BOTS ==================================================================== */
function loadBots(){
  return fetch('/api/bots').then(function(r){
    if(!r||r.status===401){document.getElementById('loginOverlay').style.display='flex';return;}
    return r.json().then(function(data){
      botRegistry=data||{};
      Object.keys(botRegistry).forEach(function(id){
        var b=botRegistry[id];
        if(b&&b.status==='online'&&b.start_time)startTimes[id]=b.start_time*1000;
      });
      document.getElementById('botCount').textContent=Object.keys(botRegistry).length;
      renderBotsGrid();renderBotList();updateStats();
      if(Object.keys(botRegistry).length>0&&!curBot)selectBot(Object.keys(botRegistry)[0],false);
      try{if(sock&&sock.connected)sock.emit('join',{});else if(sock)sock.on('connect',function(){sock.emit('join',{});});}catch(e){}
    });
  }).catch(function(e){console.error('loadBots error:',e);});
}

function updateStats(){
  try{
    var all=Object.values(botRegistry||{});
    var el1=document.getElementById('statTotal');if(el1)el1.textContent=all.length;
    var el2=document.getElementById('statActive');if(el2)el2.textContent=all.filter(function(b){return b&&b.status==='online'}).length;
    var el3=document.getElementById('statVps');if(el3)el3.textContent=all.filter(function(b){return b&&(b.type||'bot')==='vps'}).length;
  }catch(e){console.error('updateStats error:',e);}
}

function renderBotsGrid(){try{
  var g=document.getElementById('botsGrid');if(!g)return;
  var entries=Object.entries(botRegistry||{});
  if(_searchQuery)entries=entries.filter(function(e){return(e[1].name||'').toLowerCase().indexOf(_searchQuery)!==-1});
  if(_currentFilter!=='all'){
    entries=entries.filter(function(e){
      var t=e[1].type||'bot';
      if(_currentFilter==='bots')return t==='bot';
      if(_currentFilter==='vps')return t==='vps';
      return true;
    });
  }
  entries.sort(function(a,b){var c=(a[1].name||'').localeCompare(b[1].name||'');return _sortDir?c:-c;});
  if(!entries.length){
    g.innerHTML='<div class="empty-state"><div class="empty-icon">&#8862;</div><div class="empty-title">No instances yet</div><div class="empty-sub">Create your first project to get started</div><button class="btn btn-teal" onclick="openCreateModal()">+ Create Project</button></div>';
    return;
  }
  g.innerHTML='';
  entries.forEach(function(pair){
    var id=pair[0],b=pair[1];
    var online=b.status==='online';
    var sf=b.startup_file||'main.py';
    var ext=sf.split('.').pop().toLowerCase();
    var rtLabel={py:'python',js:'node',ts:'typescript',rs:'rust',cs:'csharp',sh:'bash'}[ext]||ext;
    var t=b.type||'bot';
    var typeLabel=t==='vps'?'VPS':'Bot';
    var typeBadge=t==='vps'
      ?'<span class="bot-card-free-badge" style="background:var(--blue-dim);color:var(--blue);border-color:rgba(77,159,255,0.3)">VPS</span>'
      :'<span class="bot-card-free-badge">Bot</span>';
    var card=document.createElement('div');card.className='bot-card'+(id===curBot?' selected':'');
    card.onclick=function(e){if(e.target.closest('.btn-manage,.btn-modify'))return;manageBotClick(id);};
    card.innerHTML=
      '<div class="bot-card-header">'+
        '<div>'+
          '<div class="bot-card-name-row" style="margin-bottom:4px">'+
            '<span class="bot-card-name">'+escH(b.name||id)+'</span>'+
            '<span class="bot-card-runtime">'+rtLabel+'</span>'+
            typeBadge+
          '</div>'+
          (online
            ?'<span class="bot-card-online-badge"><span class="bot-card-online-dot"></span> Online</span>'
            :'<span class="bot-card-offline-badge">\u25CF Offline</span>')+
        '</div>'+
      '</div>'+
      '<div class="bot-card-specs">'+
        '<div class="bot-spec"><div class="bot-spec-label"><span class="bot-spec-icon">\u{1F4BE}</span>RAM</div><div class="bot-spec-value">512 MB</div></div>'+
        '<div class="bot-spec"><div class="bot-spec-label"><span class="bot-spec-icon">\u27E8\u27E9</span>TYPE</div><div class="bot-spec-value">'+typeLabel+'</div></div>'+
      '</div>'+
      '<div class="bot-card-footer">'+
        '<div class="bot-card-expiry">'+
          '<span class="bot-card-expiry-icon">\u23F1</span>'+
          '<span class="bot-card-expiry-text">Expires in <strong>14 days</strong></span>'+
        '</div>'+
        '<div class="bot-card-renewal">'+
          '<span class="bot-card-renewal-icon">\u26A0</span>'+
          'Renewal will be available 3 days before expiration'+
        '</div>'+
      '</div>'+
      '<div class="bot-card-actions">'+
        '<button class="btn-manage" onclick="manageBotClick(\''+id+'\')">\u{1F916} Manage '+typeLabel+'</button>'+
        '<button class="btn-modify" onclick="selectBot(\''+id+'\',true);navTo(\'manage\',null);showManageTab(\'cfg\',null)">\u2699 Modify</button>'+
      '</div>';
    g.appendChild(card);
  });
}catch(e){console.error('renderBotsGrid error:',e);}
}

function manageBotClick(id){selectBot(id,true);navTo('manage',null);}

function renderBotList(){try{
  var el=document.getElementById('botList');if(!el)return;
  el.innerHTML='';
  var entries=Object.entries(botRegistry||{});
  if(!entries.length){
    el.innerHTML='<div style="padding:12px;text-align:center;color:var(--text3);font-size:11px">No instances yet</div>';
    return;
  }
  entries.forEach(function(pair){
    var id=pair[0],b=pair[1]||{};
    var d=document.createElement('div');d.className='bot-list-item'+(id===curBot?' active':'');
    var on=b.status==='online';
    var t=b.type||'bot';
    d.innerHTML='<div class="bot-list-dot '+(on?'online':'offline')+'"></div><div class="bot-list-name">'+escH(b.name||id)+'</div><span class="bot-list-type '+t+'">'+t+'</span>';
    d.onclick=function(){manageBotClick(id);};el.appendChild(d);
  });
}catch(e){console.error('renderBotList error:',e);}
}

function selectBot(id,navigating){
  try{
    curBot=id;var b=botRegistry[id]||{};
    var _s=function(eid,v){var el=document.getElementById(eid);if(el)el.textContent=v;};
    var _v=function(eid,v){var el=document.getElementById(eid);if(el)el.value=v;};
    _s('topbarBotName',b.name||id||'');
    _v('sfInput',b.startup_file||'main.py');
    _s('manageBotTitle',b.name||id||'');
    _s('manageBotId',id||'');
    _s('termTitle',(b.name||id||'').toUpperCase()+' // STDOUT');
    applyStatus(b.status||'offline');
    renderBotList();renderBotsGrid();updateStats();
    detectRuntime(b.startup_file||'main.py');
    if(navigating)loadBotLogs();
    updateUptime();
  }catch(e){console.error('selectBot error:',e);}
}

function loadBotLogs(){
  if(!curBot)return;
  fetch('/api/bot/'+curBot+'/logs').then(function(r){
    if(!r||r.status===401)return;
    return r.json().then(function(logs){
      ['mainTerm','miniTerm'].forEach(function(id){var el=document.getElementById(id);if(el)el.innerHTML='';});
      if(logs&&logs.forEach)logs.forEach(function(entry){appendLog(entry.msg,entry.level,entry.time);});
    });
  }).catch(function(){});
}

function applyStatus(s){
  try{
    var on=s==='online';
    var pill=document.getElementById('statusPill');
    if(pill)pill.className='status-pill '+(on?'online':'offline');
    var st=document.getElementById('statusText');if(st)st.textContent=on?'Online':'Offline';
    updateStats();
    if(on)startUptimeTimer();else stopUptimeTimer();
  }catch(e){console.error('applyStatus error:',e);}
}

/* == BOT ACTIONS ============================================================ */
function createBot(){
  var n=document.getElementById('mName').value.trim();
  var t=document.getElementById('mType').value;
  var f=document.getElementById('mFile').value.trim()||(RUNTIMES[currentRuntime]?RUNTIMES[currentRuntime].defaultFile:'main.py');
  if(!n){toast('Instance name required','error');return;}
  fetch('/api/bots',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:n,startup_file:f,type:t})})
  .then(function(r){return r.json().then(function(res){
    if(!r.ok){toast(res.error||'Failed to create','error');return;}
    botRegistry[res.id]=res;closeModal('mCreate');document.getElementById('mName').value='';
    document.getElementById('botCount').textContent=Object.keys(botRegistry).length;
    renderBotsGrid();renderBotList();updateStats();
    selectBot(res.id,false);toast('"'+n+'" deployed','success');
  });}).catch(function(e){toast('Network error: '+e.message,'error');});
}
function startBot(){
  if(!curBot){toast('Select an instance first','error');return;}
  var sf=document.getElementById('sfInput').value.trim()||(RUNTIMES[currentRuntime]?RUNTIMES[currentRuntime].defaultFile:'main.py');
  fetch('/api/bot/'+curBot+'/start',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({startup_file:sf})})
  .then(function(r){
    if(r&&!r.ok){return r.json().then(function(e){toast(e.error||'Start failed','error');}).catch(function(){toast('Start failed','error');});}
    else{toast('Booting...','info');}
  }).catch(function(e){toast('Network error: '+e.message,'error');});
}
function stopBot(){
  if(!curBot){toast('Select an instance first','error');return;}
  fetch('/api/bot/'+curBot+'/stop',{method:'POST'}).then(function(){toast('Stopped','success');}).catch(function(e){toast('Network error: '+e.message,'error');});
}
function restartBot(){
  if(!curBot){toast('Select an instance first','error');return;}
  fetch('/api/bot/'+curBot+'/stop',{method:'POST'}).then(function(){
    toast('Restarting...','info');setTimeout(startBot,1200);
  }).catch(function(e){toast('Network error: '+e.message,'error');});
}
function killBot(){
  if(!curBot)return;if(!confirm('Force kill?'))return;
  fetch('/api/bot/'+curBot+'/kill',{method:'POST'}).then(function(){toast('Force killed','error');}).catch(function(){});
}
function deleteBot(){
  if(!curBot||!confirm('Permanently destroy this instance and ALL its files?'))return;
  fetch('/api/bot/'+curBot,{method:'DELETE'}).then(function(r){
    if(!r||!r.ok){toast('Delete failed','error');return;}
    delete botRegistry[curBot];curBot=null;
    document.getElementById('topbarBotName').textContent='';
    applyStatus('offline');renderBotsGrid();renderBotList();
    document.getElementById('botCount').textContent=Object.keys(botRegistry).length;
    navTo('projects',null);toast('Instance destroyed','error');
  }).catch(function(e){toast('Network error: '+e.message,'error');});
}

/* == CONSOLE ================================================================ */
function appendLog(msg,level,ts){
  var tagMap={system:'sys',error:'err',success:'ok',warn:'out','default':'out',stdin:'in'};
  var tag=tagMap[level]||'out',t=ts||new Date().toTimeString().slice(0,8);
  var row='<div class="log-row"><span class="log-ts">'+escH(t)+'</span><span class="log-tag '+tag+'">'+tag+'</span><span class="log-msg '+tag+'">'+escH(msg)+'</span></div>';
  ['mainTerm','miniTerm'].forEach(function(id){var el=document.getElementById(id);if(!el)return;el.innerHTML+=row;el.scrollTop=el.scrollHeight;});
}
function clearConsole(){['mainTerm','miniTerm'].forEach(function(id){var el=document.getElementById(id);if(el)el.innerHTML='';});toast('Console cleared','info');}
function exportLogs(){
  var el=document.getElementById('mainTerm');if(!el)return;
  var lines=Array.from(el.querySelectorAll('.log-row')).map(function(r){return r.textContent.trim()}).join('\n');
  var a=document.createElement('a');a.href='data:text/plain;charset=utf-8,'+encodeURIComponent(lines);a.download=(curBot||'vortex')+'.log';a.click();toast('Exported','success');
}
function sendInput(){
  if(!curBot)return;var inp=document.getElementById('termIn');if(!inp)return;
  var v=inp.value;if(!v.trim())return;inp.value='';v=v.replace(/[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f]/g,'');
  fetch('/api/bot/'+curBot+'/input',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({input:v})}).catch(function(){});
}

/* == FILE MANAGER =========================================================== */
var EXT_ICONS={py:'\u{1F40D}',js:'\u26A1',jsx:'\u269B',tsx:'\u269B',ts:'\u27E8\u27E9',json:'{}',txt:'\u2261',md:'#',zip:'\u2262',env:'\u229B',sh:'$',html:'<>',css:'\u25D0',rs:'\u2699',cs:'\u25C6',toml:'\u2699'};
var EXT_COLORS={py:'#22c55e',js:'#eab308',json:'#4d9fff',md:'#f97316',txt:'#475569',sh:'#4d9fff',zip:'#ef4444',ts:'#4d9fff',html:'#f97316',rs:'#f97316',cs:'#a855f7'};

function loadFiles(){
  var tb=document.getElementById('fileList');
  if(!curBot){if(tb)tb.innerHTML='<tr><td colspan="5" style="padding:28px;text-align:center;color:var(--text3);font-size:12px">Select an instance first</td></tr>';return;}
  if(tb)tb.innerHTML='<tr><td colspan="5" style="padding:28px;text-align:center;color:var(--text3);font-size:12px">Loading...</td></tr>';
  fetch('/api/bot/'+curBot+'/files').then(function(r){
    if(!r||!r.ok)throw new Error('HTTP '+r.status);
    return r.json();
  }).then(function(files){
    if(!files||!files.length){if(tb)tb.innerHTML='<tr><td colspan="5" style="padding:28px;text-align:center;color:var(--text3);font-size:12px">No files yet \u2014 upload some</td></tr>';return;}
    var k;for(k in _renameMap)delete _renameMap[k];
    if(tb)tb.innerHTML='';
    files.forEach(function(f){
      var ext=(f.name.split('.').pop()||'').toLowerCase();
      var ic=EXT_ICONS[ext]||'\u25A1',c=EXT_COLORS[ext]||'#475569';
      var rid='r'+Math.random().toString(36).slice(2,9);_renameMap[rid]=f.name;
      var tr=document.createElement('tr');
      var tdN=document.createElement('td');
      var fnCell=document.createElement('div');fnCell.className='fn-cell';
      var icon=document.createElement('span');icon.className='fn-icon';icon.textContent=ic;
      var link=document.createElement('span');link.className='fn-link';link.id='fnl_'+rid;link.title=f.name;link.textContent=f.display||f.name.split('/').pop();link.onclick=(function(n){return function(){editFile(n)}})(f.name);
      var rnw=document.createElement('div');rnw.className='fn-rename';rnw.id='fnr_'+rid;
      var rni=document.createElement('input');rni.className='fn-rename-input';rni.id='fni_'+rid;rni.value=link.textContent;rni.onkeydown=(function(r){return function(e){if(e.key==='Enter')doRename(r);if(e.key==='Escape')cancelRename(r);}})(rid);
      var rnok=document.createElement('button');rnok.className='fn-rename-ok';rnok.textContent='\u2713';rnok.onclick=(function(r){return function(){doRename(r)}})(rid);
      var rnx=document.createElement('button');rnx.className='fn-rename-cancel';rnx.textContent='\u2715';rnx.onclick=(function(r){return function(){cancelRename(r)}})(rid);
      rnw.append(rni,rnok,rnx);fnCell.append(icon,link,rnw);tdN.appendChild(fnCell);
      var tdT=document.createElement('td');var badge=document.createElement('span');badge.className='ext-badge';badge.style.color=c;badge.textContent='.'+(ext||'\u2014');tdT.appendChild(badge);
      var tdS=document.createElement('td');tdS.style.color='var(--text3)';tdS.textContent=f.size;
      var tdM=document.createElement('td');tdM.style.cssText='color:var(--text3);font-size:11px';tdM.textContent=f.modified;
      var tdA=document.createElement('td');var act=document.createElement('div');act.className='file-actions';
      var be=document.createElement('button');be.className='icon-btn teal';be.title='Edit';be.textContent='\u270F';be.onclick=(function(n){return function(){editFile(n)}})(f.name);
      var br=document.createElement('button');br.className='icon-btn';br.title='Rename';br.textContent='\u27F3';br.id='rnb_'+rid;br.onclick=(function(r){return function(){toggleRename(r)}})(rid);
      var bd=document.createElement('button');bd.className='icon-btn';bd.title='Download';bd.textContent='\u2193';bd.onclick=(function(n){return function(){dlFile(n)}})(f.name);
      var bx=document.createElement('button');bx.className='icon-btn red';bx.title='Delete';bx.textContent='\u2715';bx.onclick=(function(n){return function(){delFile(n)}})(f.name);
      act.append(be,br,bd,bx);tdA.appendChild(act);tr.append(tdN,tdT,tdS,tdM,tdA);if(tb)tb.appendChild(tr);
    });
  }).catch(function(){if(tb)tb.innerHTML='<tr><td colspan="5" style="padding:28px;text-align:center;color:var(--text3);font-size:12px">Failed to load files</td></tr>';});
}
function toggleRename(rid){var l=document.getElementById('fnl_'+rid),rn=document.getElementById('fnr_'+rid),b=document.getElementById('rnb_'+rid);if(!l||!rn||!b)return;if(rn.classList.contains('on')){cancelRename(rid);}else{l.style.display='none';rn.classList.add('on');b.textContent='\u2715';var inp=document.getElementById('fni_'+rid);if(inp){inp.focus();var v=inp.value,dot=v.lastIndexOf('.');inp.setSelectionRange(0,dot>0?dot:v.length);}}}
function cancelRename(rid){var l=document.getElementById('fnl_'+rid),rn=document.getElementById('fnr_'+rid),b=document.getElementById('rnb_'+rid);if(l)l.style.display='';if(rn)rn.classList.remove('on');if(b)b.textContent='\u27F3';}
function doRename(rid){var old=_renameMap[rid];if(!old)return;var inp=document.getElementById('fni_'+rid);if(!inp)return;var nb=inp.value.trim();if(!nb)return;var parts=old.split('/');parts[parts.length-1]=nb;var nn=parts.join('/');if(nn===old){cancelRename(rid);return;}fetch('/api/bot/'+curBot+'/file/'+encodeURIComponent(old)+'/rename',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({new_name:nn})}).then(function(r){if(!r)return;return r.json().then(function(res){if(res.error){toast(res.error,'error');return;}toast('Renamed \u2192 '+nb,'success');loadFiles();});}).catch(function(e){toast('Rename failed','error');});}
function editFile(name){if(!curBot)return;fetch('/api/bot/'+curBot+'/file/'+encodeURIComponent(name)).then(function(r){if(!r)return;return r.json().then(function(d){document.getElementById('edName').textContent=name;document.getElementById('edContent').value=d.content==='[Binary \u2014 cannot display]'?'':d.content;document.getElementById('edContent').dataset.fn=name;document.getElementById('mEditor').classList.add('open');});}).catch(function(e){toast('Failed to load file','error');});}
function saveFile(){var name=document.getElementById('edContent').dataset.fn;if(!name)return;fetch('/api/bot/'+curBot+'/file/'+encodeURIComponent(name),{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({content:document.getElementById('edContent').value})}).then(function(r){if(!r||!r.ok){toast('Save failed','error');return;}closeModal('mEditor');loadFiles();toast(name+' saved','success');}).catch(function(){toast('Save failed','error');});}
function openNewFileModal(){if(!curBot){toast('Select an instance first','error');return;}document.getElementById('mNewFile').classList.add('open');setTimeout(function(){var el=document.getElementById('nfName');if(el)el.focus();},80);}
function createNewFile(){var name=document.getElementById('nfName').value.trim();if(!name){toast('Filename required','error');return;}fetch('/api/bot/'+curBot+'/file/'+encodeURIComponent(name),{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({content:document.getElementById('nfContent').value})}).then(function(r){if(!r||!r.ok){toast('Create failed','error');return;}closeModal('mNewFile');document.getElementById('nfName').value='';document.getElementById('nfContent').value='';loadFiles();toast('File created','success');}).catch(function(){toast('Create failed','error');});}
function delFile(name){if(!confirm('Delete "'+name+'"?'))return;fetch('/api/bot/'+curBot+'/file/'+encodeURIComponent(name),{method:'DELETE'}).then(function(r){if(!r||!r.ok){toast('Delete failed','error');return;}loadFiles();toast('File deleted','success');}).catch(function(){toast('Delete failed','error');});}
function dlFile(name){window.location.href='/api/bot/'+curBot+'/file/'+encodeURIComponent(name)+'/download';}

/* == DRAG & DROP ============================================================ */
function handleUpload(files,isFolder){
  var fileArr=files?Array.from(files):[];
  try{document.getElementById('fileUploadInput').value='';}catch(e){}
  try{document.getElementById('folderUploadInput').value='';}catch(e){}
  if(!curBot){toast('Select an instance first','error');return;}
  if(!fileArr.length)return;
  var prog=document.getElementById('uploadProgress');
  var ok=0,fail=0,total=fileArr.length;

  function doUpload(idx){
    if(idx>=fileArr.length){
      loadFiles();
      if(ok>0&&fail===0)toast(ok+' file'+(ok>1?'s':'')+' uploaded','success');
      else if(ok>0&&fail>0)toast(ok+' uploaded, '+fail+' failed','info');
      return;
    }
    var file=fileArr[idx];
    var relPath=(isFolder&&file.webkitRelativePath)?file.webkitRelativePath:file.name;
    var fd=new FormData();fd.append('file',file);fd.append('relative_path',relPath);fd.append('is_folder_upload',isFolder?'1':'0');
    var sid='up_'+Math.random().toString(36).slice(2);
    var wrap=document.createElement('div');wrap.className='upload-row';
    var sn=relPath.length>50?'\u2026'+relPath.slice(-47):relPath;
    wrap.innerHTML='<span style="color:var(--teal)">\u21E7</span><span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+escH(sn)+'</span><div class="upload-bar-wrap"><div class="upload-bar-fill" id="'+sid+'" style="width:0%"></div></div><span id="'+sid+'st" style="font-size:9px;color:var(--text3);flex-shrink:0">0%</span>';
    if(prog)prog.appendChild(wrap);

    var xhr=new XMLHttpRequest();xhr.open('POST','/api/bot/'+curBot+'/upload');
    xhr.upload.addEventListener('progress',function(e){if(e.lengthComputable){var pct=Math.round(e.loaded/e.total*95);var b=document.getElementById(sid),s=document.getElementById(sid+'st');if(b)b.style.width=pct+'%';if(s)s.textContent=pct+'%';}});
    xhr.addEventListener('load',function(){
      var resp={};try{resp=JSON.parse(xhr.responseText);}catch(e){}
      if(resp.error||xhr.status>=300){
        var s=document.getElementById(sid+'st');if(s){s.textContent='\u2715';s.style.color='var(--red)';}
        fail++;toast('Upload failed: '+(resp.error||'HTTP '+xhr.status),'error');setTimeout(function(){wrap.remove()},4000);
      } else {
        var b=document.getElementById(sid),s=document.getElementById(sid+'st');
        if(b){b.style.width='100%';b.style.background='var(--green)';}if(s){s.textContent='\u2713';s.style.color='var(--green)';}
        ok++;setTimeout(function(){wrap.remove()},2000);
      }
      doUpload(idx+1);
    });
    xhr.addEventListener('error',function(){
      var s=document.getElementById(sid+'st');if(s){s.textContent='\u2715';s.style.color='var(--red)';}
      fail++;toast('Upload failed: network error','error');setTimeout(function(){wrap.remove()},4000);
      doUpload(idx+1);
    });
    xhr.send(fd);
  }
  doUpload(0);
}
var _dzDepth=0;
document.addEventListener('dragenter',function(e){e.preventDefault();_dzDepth++;var dz=document.getElementById('dropZone');if(dz)dz.classList.add('dragging');});
document.addEventListener('dragleave',function(){if(--_dzDepth<=0){_dzDepth=0;var dz=document.getElementById('dropZone');if(dz)dz.classList.remove('dragging');}});
document.addEventListener('dragover',function(e){e.preventDefault()});
document.addEventListener('drop',function(e){e.preventDefault();_dzDepth=0;var dz=document.getElementById('dropZone');if(dz)dz.classList.remove('dragging');if(e.dataTransfer.files.length)handleUpload(e.dataTransfer.files,false);});

/* == CODE EDITOR TAB SUPPORT ================================================ */
document.addEventListener('keydown',function(e){
  var ed=document.getElementById('edContent');
  if(e.target===ed&&e.key==='Tab'){
    e.preventDefault();
    var start=ed.selectionStart,end=ed.selectionEnd;
    ed.value=ed.value.substring(0,start)+'    '+ed.value.substring(end);
    ed.selectionStart=ed.selectionEnd=start+4;
  }
});

/* == ENV ==================================================================== */
function loadEnv(){if(!curBot)return;fetch('/api/bot/'+curBot+'/env').then(function(r){if(!r)return;return r.json().then(function(env){var c=document.getElementById('envRows');if(!c)return;c.innerHTML='';var e=env?Object.entries(env):[];if(e.length)e.forEach(function(pair){addEnvRow(pair[0],pair[1])});else addEnvRow('','');});}).catch(function(){addEnvRow('','');});}
function addEnvRow(k,v){if(k===undefined)k='';if(v===undefined)v='';var d=document.createElement('div');d.className='env-row';var ki=document.createElement('input');ki.className='env-field env-key';ki.placeholder='VARIABLE_NAME';ki.value=k;var vi=document.createElement('input');vi.className='env-field';vi.placeholder='value';vi.value=v;var db=document.createElement('button');db.className='icon-btn red';db.textContent='\u2715';db.style.cssText='width:28px;height:28px';db.onclick=function(){d.remove()};d.append(ki,vi,db);document.getElementById('envRows').appendChild(d);}
function saveEnv(){if(!curBot)return;var env={};document.querySelectorAll('.env-row').forEach(function(r){var inputs=r.querySelectorAll('.env-field');var k=inputs[0]?inputs[0].value.trim():'',v=inputs[1]?inputs[1].value:'';if(k)env[k]=v||'';});fetch('/api/bot/'+curBot+'/env',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(env)}).then(function(r){if(!r||!r.ok){toast('Save failed','error');return;}toast('Environment saved','success');}).catch(function(){toast('Save failed','error');});}

/* == SETTINGS ================================================================ */
function loadSettings(){
  if(!curBot)return;var b=botRegistry[curBot]||{};
  document.getElementById('stName').value=b.name||'';
  document.getElementById('stStartup').value=b.startup_file||'main.py';
  document.getElementById('stType').value=b.type||'bot';
  document.getElementById('stAR').value=b.auto_restart?'true':'false';

  /* Hide owner-only sections for shared users */
  var isShared=!!b.is_shared;
  var amEl=document.getElementById('accessMgmtSection');
  var dzEl=document.getElementById('dangerZoneSection');
  if(amEl)amEl.style.display=isShared?'none':'';
  if(dzEl)dzEl.style.display=isShared?'none':'';

  if(!isShared){
    fetch('/api/bot/'+curBot+'/subusers').then(function(r){
      if(!r)return;return r.json().then(function(users){
        var c=document.getElementById('subuserList');if(!c)return;c.innerHTML='';
        if(!users||!users.length){c.innerHTML='<div style="font-size:11px;color:var(--text3);padding:6px 0">No shared users yet.</div>';return;}
        users.forEach(function(u){
          var div=document.createElement('div');div.className='subuser-row';
          var ns=document.createElement('span');ns.style.color='var(--text2)';ns.style.fontSize='12px';ns.textContent=u;
          var db=document.createElement('button');db.className='icon-btn red';db.style.cssText='width:26px;height:26px;font-size:10px';db.textContent='\u2715';db.onclick=(function(user){return function(){removeSubuser(user)}})(u);
          div.append(ns,db);c.appendChild(div);
        });
      });
    }).catch(function(){});
  }
}
function saveSettings(){if(!curBot)return;var data={name:document.getElementById('stName').value.trim(),startup_file:document.getElementById('stStartup').value.trim()||'main.py',type:document.getElementById('stType').value,auto_restart:document.getElementById('stAR').value==='true'};fetch('/api/bot/'+curBot+'/settings',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)}).then(function(r){if(!r||!r.ok){toast('Save failed','error');return;}return r.json().then(function(upd){botRegistry[curBot]=Object.assign({},botRegistry[curBot],upd);document.getElementById('topbarBotName').textContent=data.name||curBot;document.getElementById('manageBotTitle').textContent=data.name||curBot;document.getElementById('sfInput').value=data.startup_file;renderBotList();renderBotsGrid();toast('Configuration saved','success');});}).catch(function(){toast('Save failed','error');});}
function addSubuser(){if(!curBot)return;var u=document.getElementById('newSubuser').value.trim();if(!u)return;fetch('/api/bot/'+curBot+'/subusers',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:u})}).then(function(r){if(r&&r.ok){document.getElementById('newSubuser').value='';loadSettings();toast('Access granted to '+u,'success');}else toast('User not found','error');}).catch(function(){toast('Failed to grant access','error');});}
function removeSubuser(u){if(!curBot)return;fetch('/api/bot/'+curBot+'/subusers/'+encodeURIComponent(u),{method:'DELETE'}).then(function(r){if(r&&r.ok){loadSettings();toast('Access revoked for '+u,'success');}}).catch(function(){});}

/* == RESOURCES =============================================================== */
function startRes(){stopRes();fetchRes();resIv=setInterval(fetchRes,4000);}
function stopRes(){if(resIv){clearInterval(resIv);resIv=null;}}
function fetchRes(){
  fetch('/api/resources').then(function(r){if(!r.ok)return;return r.json().then(function(d){
    var set=function(id,v){var el=document.getElementById(id);if(el)el.textContent=v;};
    var sw=function(id,v){var el=document.getElementById(id);if(el)el.style.width=v;};
    set('rCpu',d.cpu+'%');sw('pCpu',d.cpu+'%');set('rMem',d.mem_used);sw('pMem',d.mem_pct+'%');set('rDsk',d.disk_pct+'%');sw('pDsk',d.disk_pct+'%');
  });}).catch(function(){});
}

/* == MODAL =================================================================== */
function openCreateModal(){document.getElementById('mFile').value=RUNTIMES[currentRuntime]?RUNTIMES[currentRuntime].defaultFile:'main.py';document.getElementById('mCreate').classList.add('open');setTimeout(function(){var el=document.getElementById('mName');if(el)el.focus();},80);}
function closeModal(id){var el=document.getElementById(id);if(el)el.classList.remove('open');}
document.querySelectorAll('.modal-veil').forEach(function(m){m.addEventListener('click',function(e){if(e.target===m)m.classList.remove('open');});});

/* == UTILS =================================================================== */
function escH(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');}
function toast(msg,type){
  if(!type)type='success';
  var tray=document.getElementById('toastTray');if(!tray)return;
  var icons={success:'\u2713',error:'\u2715',info:'i'};
  var t=document.createElement('div');t.className='toast '+type;
  t.innerHTML='<div class="toast-icon">'+icons[type]+'</div><span>'+escH(msg)+'</span><span class="toast-close" onclick="this.parentElement.remove()">\u2715</span>';
  tray.appendChild(t);
  setTimeout(function(){t.style.transition='all .3s';t.style.opacity='0';t.style.transform='translateX(10px)';setTimeout(function(){t.remove()},300);},3500);
}

/* == GLOBAL ERROR HANDLER =================================================== */
window.addEventListener('unhandledrejection',function(e){console.error('Unhandled promise:',e.reason);e.preventDefault();});
window.onerror=function(msg,src,line,col,err){console.error('JS Error:',msg,src,line);return true;};

/* == BOOT ==================================================================== */
try{
  checkAuth().then(function(ok){if(ok){loadBots();fetchRes();setInterval(fetchRes,5000);}}).catch(function(e){console.error('Boot error:',e);document.getElementById('loginOverlay').style.display='flex';});
}catch(e){console.error('Boot exception:',e);document.getElementById('loginOverlay').style.display='flex';}
</script>
</body>
</html>"""


# --- SOCKET HANDLERS ----------------------------------------------------------

@socketio.on('connect')
def handle_connect():
    user = session.get('username')
    if user:
        join_room(user)

@socketio.on('join')
def handle_join(data=None):
    user = session.get('username')
    if user:
        join_room(user)

@app.route('/')
def index():
    return render_template_string(HTML)

# --- AUTH ROUTES ---------------------------------------------------------------

@app.route('/api/login', methods=['POST'])
def login():
    data = request.json or {}
    username    = data.get('username', '').strip()
    password    = data.get('password', '')
    remember_me = bool(data.get('remember_me', False))
    if not username or not password:
        return jsonify({'error': 'Credentials required'}), 400
    users = load_users()
    if username not in users:
        return jsonify({'error': 'User not found'}), 401
    stored = users[username].get('pwd', '')
    if stored != _hash_pw(password) and stored != password:
        return jsonify({'error': 'Invalid password'}), 401
    if stored == password:
        users[username]['pwd'] = _hash_pw(password)
        save_users(users)
    session['username'] = username
    session.permanent = remember_me
    return jsonify({'ok': True})

@app.route('/api/register', methods=['POST'])
def register():
    data = request.json or {}
    username    = data.get('username', '').strip()
    password    = data.get('password', '')
    remember_me = bool(data.get('remember_me', False))
    if not username or not password:
        return jsonify({'error': 'Credentials required'}), 400
    if len(username) > 64 or len(password) > 256:
        return jsonify({'error': 'Input too long'}), 400
    if not re.match(r'^[a-zA-Z0-9_\-\.]+$', username):
        return jsonify({'error': 'Username may only contain letters, numbers, _, -, .'}), 400
    users = load_users()
    if username in users:
        return jsonify({'error': 'Username already exists'}), 400
    users[username] = {'pwd': _hash_pw(password)}
    save_users(users)
    session['username'] = username
    session.permanent = remember_me
    return jsonify({'ok': True})

@app.route('/api/logout', methods=['POST'])
def do_logout():
    session.clear()
    return jsonify({'ok': True})

@app.route('/api/me')
def me():
    if 'username' in session:
        return jsonify({'username': session['username']})
    return jsonify({'error': 'unauthorized'}), 401

# --- BOT MANAGEMENT ROUTES -----------------------------------------------------

@app.route('/api/bots')
def get_bots():
    user = session.get('username')
    if not user:
        return jsonify({'error': 'unauth'}), 401
    cfg = load_config(); out = {}
    for bid, bc in cfg.items():
        owner  = bc.get('owner')
        shared = bc.get('shared_with', [])
        if owner == user or user in shared:
            running = is_running(bid)
            out[bid] = {
                'id': bid, 'name': bc.get('name', bid),
                'startup_file': bc.get('startup_file', 'main.py'),
                'type': bc.get('type', 'bot'),
                'status': 'online' if running else 'offline',
                'auto_restart': bc.get('auto_restart', False),
                'start_time': bots.get(bid, {}).get('start_time') if running else None,
                'is_shared': owner != user,
            }
    return jsonify(out)

@app.route('/api/bots', methods=['POST'])
def create_bot_route():
    user = session.get('username')
    if not user:
        return jsonify({'error': 'unauth'}), 401
    cfg = load_config()
    user_bots = [b for b in cfg.values() if b.get('owner') == user]
    if len(user_bots) >= MAX_BOTS_PER_USER:
        return jsonify({'error': f'Maximum {MAX_BOTS_PER_USER} instances per user'}), 400
    data = request.json or {}
    name = str(data.get('name', 'New Instance')).strip()[:64] or 'New Instance'
    sf_raw = str(data.get('startup_file', 'main.py')).strip()
    startup_file = safe_startup_file(sf_raw) or 'main.py'
    btype = str(data.get('type', 'bot')).strip().lower()
    if btype not in ('bot', 'vps'): btype = 'bot'

    bid = f"bot_{int(time.time() * 1000)}"
    cfg[bid] = {
        'name': name, 'startup_file': startup_file,
        'type': btype,
        'auto_restart': False, 'env': {},
        'owner': user, 'shared_with': [],
    }
    save_config(cfg)
    bot_dir = get_bot_dir(bid)

    full_sf_path = os.path.join(bot_dir, startup_file)
    if not os.path.exists(full_sf_path):
        os.makedirs(os.path.dirname(full_sf_path), exist_ok=True)
        ext = startup_file.split('.')[-1].lower()
        content = ""
        if btype == 'vps' or ext == 'sh':
            content = "#!/bin/bash\necho 'VPS Initialized.'\n# Add your persistent commands here\nwhile true; do sleep 1000; done\n"
        elif ext == 'py':
            content = "print('Hello from Vortex Python Bot!')\nimport time\nwhile True:\n    time.sleep(10)\n"
        elif ext == 'js':
            content = "console.log('Hello from Vortex Node Bot!');\nsetInterval(() => {}, 10000);\n"
        elif ext == 'ts':
            content = "console.log('Hello from Vortex TypeScript Bot!');\nsetInterval(() => {}, 10000);\n"
        elif ext == 'rs':
            content = "fn main() {\n    println!(\"Hello from Vortex Rust Bot!\");\n    loop { std::thread::sleep(std::time::Duration::from_secs(10)); }\n}\n"
        elif ext == 'cs':
            content = "using System;\nclass Program {\n    static void Main() {\n        Console.WriteLine(\"Hello from Vortex C# Bot!\");\n        while(true) System.Threading.Thread.Sleep(10000);\n    }\n}\n"

        with open(full_sf_path, 'w', encoding='utf-8') as f:
            f.write(content)

    return jsonify({'id': bid, **cfg[bid], 'status': 'offline', 'is_shared': False})

@app.route('/api/bot/<bid>', methods=['DELETE'])
def del_bot(bid):
    if not check_owner(bid):
        return jsonify({'error': 'unauth'}), 401
    stop_bot(bid)
    cfg = load_config(); cfg.pop(bid, None); save_config(cfg)
    bd = os.path.join(BOTS_DIR, bid)
    if os.path.exists(bd):
        shutil.rmtree(bd)
    bots.pop(bid, None)
    return jsonify({'ok': True})

# --- SUBUSER ROUTES ------------------------------------------------------------

@app.route('/api/bot/<bid>/subusers', methods=['GET'])
def get_subusers(bid):
    if not check_owner(bid):
        return jsonify({'error': 'unauth'}), 401
    return jsonify(load_config().get(bid, {}).get('shared_with', []))

@app.route('/api/bot/<bid>/subusers', methods=['POST'])
def add_subuser(bid):
    if not check_owner(bid):
        return jsonify({'error': 'unauth'}), 401
    target = (request.json or {}).get('username', '').strip()
    if target not in load_users():
        return jsonify({'error': 'User does not exist'}), 404
    cfg = load_config(); shared = cfg.get(bid, {}).get('shared_with', [])
    if target not in shared and target != cfg[bid]['owner']:
        shared.append(target); cfg[bid]['shared_with'] = shared; save_config(cfg)
    return jsonify({'ok': True})

@app.route('/api/bot/<bid>/subusers/<user>', methods=['DELETE'])
def remove_subuser(bid, user):
    if not check_owner(bid):
        return jsonify({'error': 'unauth'}), 401
    cfg = load_config(); shared = cfg.get(bid, {}).get('shared_with', [])
    if user in shared:
        shared.remove(user); cfg[bid]['shared_with'] = shared; save_config(cfg)
    return jsonify({'ok': True})

# --- PROCESS CONTROL ROUTES ----------------------------------------------------

@app.route('/api/bot/<bid>/start', methods=['POST'])
def start_route(bid):
    if not check_access(bid):
        return jsonify({'error': 'unauth'}), 401
    sf = (request.json or {}).get('startup_file')
    if sf and not safe_startup_file(sf):
        return jsonify({'error': 'Invalid startup file'}), 400
    threading.Thread(target=start_bot, args=(bid, sf), daemon=True).start()
    return jsonify({'ok': True})

@app.route('/api/bot/<bid>/stop', methods=['POST'])
def stop_route(bid):
    if not check_access(bid):
        return jsonify({'error': 'unauth'}), 401
    stop_bot(bid)
    return jsonify({'ok': True})

@app.route('/api/bot/<bid>/kill', methods=['POST'])
def kill_route(bid):
    if not check_access(bid):
        return jsonify({'error': 'unauth'}), 401
    if bid in bots:
        bots[bid]['auto_restart'] = False
        proc = bots[bid].get('process')
        if proc:
            try:
                proc.kill()
                emit_log(bid, '[System] Force killed.', 'error')
            except Exception:
                pass
    return jsonify({'ok': True})

@app.route('/api/bot/<bid>/input', methods=['POST'])
def input_route(bid):
    if not check_access(bid):
        return jsonify({'error': 'unauth'}), 401
    inp = (request.json or {}).get('input', '').strip()
    if not inp:
        return jsonify({'ok': True})

    force_shell = False
    if inp.startswith('$'):
        inp = inp[1:].strip()
        force_shell = True

    if not is_running(bid) or force_shell:
        emit_log(bid, f'[Terminal] > {inp}', 'system')
        threading.Thread(target=_run_shell_cmd, args=(bid, inp), daemon=True).start()
        return jsonify({'ok': True})

    if bid in bots and bots[bid].get('process'):
        p = bots[bid]['process']
        if p.poll() is None:
            try:
                data = inp + '\n'
                stdin_fd = bots[bid].get('stdin_fd')
                if stdin_fd is not None:
                    os.write(stdin_fd, data.encode('utf-8', errors='replace'))
                elif p.stdin:
                    p.stdin.write(data)
                    p.stdin.flush()
                emit_log(bid, f'> {inp}', 'stdin')
            except OSError as e:
                emit_log(bid, f'[Error] stdin write failed: {e}', 'error')
    return jsonify({'ok': True})

@app.route('/api/bot/<bid>/logs')
def logs_route(bid):
    if not check_access(bid):
        return jsonify({'error': 'unauth'}), 401
    mem = bots.get(bid, {}).get('logs', [])
    if mem:
        return jsonify(mem)
    lf = os.path.join(get_bot_dir(bid), 'system.log'); disk = []
    if os.path.exists(lf):
        try:
            with open(lf, 'r', encoding='utf-8') as f:
                for line in f.readlines()[-500:]:
                    try: disk.append(json.loads(line.strip()))
                    except Exception: pass
        except Exception:
            pass
    return jsonify(disk)

# --- FILE ROUTES ---------------------------------------------------------------

@app.route('/api/bot/<bid>/files')
def files_route(bid):
    if not check_access(bid):
        return jsonify({'error': 'unauth'}), 401
    bd = get_bot_dir(bid); out = []
    for root, dirs, files in os.walk(bd):
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        for f in files:
            if f == 'system.log':
                continue
            fp  = os.path.join(root, f)
            rel = os.path.relpath(fp, bd).replace('\\', '/')
            try:
                sz = os.path.getsize(fp)
                mtime = time.strftime('%Y-%m-%d %H:%M', time.localtime(os.path.getmtime(fp)))
            except OSError:
                sz = 0; mtime = '\u2014'
            s = f"{sz}B" if sz < 1024 else f"{sz // 1024}KB" if sz < 1024 ** 2 else f"{sz // 1024 // 1024}MB"
            out.append({'name': rel, 'display': rel, 'size': s, 'modified': mtime})
    out.sort(key=lambda x: x['name'])
    return jsonify(out)

@app.route('/api/bot/<bid>/file/<path:fn>', methods=['GET'])
def get_file(bid, fn):
    if not check_access(bid):
        return jsonify({'error': 'unauth'}), 401
    fp = safe_path(bid, fn)
    if not fp:
        return jsonify({'error': 'invalid path'}), 403
    if not os.path.exists(fp):
        return jsonify({'content': ''})
    if os.path.isdir(fp):
        return jsonify({'error': 'path is a directory'}), 400
    try:
        return jsonify({'content': open(fp, encoding='utf-8', errors='replace').read()})
    except Exception:
        return jsonify({'content': '[Binary \u2014 cannot display]'})

@app.route('/api/bot/<bid>/file/<path:fn>', methods=['PUT'])
def put_file(bid, fn):
    if not check_access(bid):
        return jsonify({'error': 'unauth'}), 401
    fp = safe_path(bid, fn)
    if not fp:
        return jsonify({'error': 'invalid path'}), 403
    if os.path.isdir(fp):
        return jsonify({'error': 'path is a directory'}), 400
    content = (request.json or {}).get('content', '')
    if not isinstance(content, str):
        return jsonify({'error': 'content must be a string'}), 400
    os.makedirs(os.path.dirname(fp), exist_ok=True)
    with open(fp, 'w', encoding='utf-8') as f:
        f.write(content)
    return jsonify({'ok': True})

@app.route('/api/bot/<bid>/file/<path:fn>', methods=['DELETE'])
def del_file(bid, fn):
    if not check_access(bid):
        return jsonify({'error': 'unauth'}), 401
    fp = safe_path(bid, fn)
    if not fp:
        return jsonify({'error': 'invalid path'}), 403
    if not os.path.exists(fp):
        return jsonify({'error': 'file not found'}), 404
    if os.path.isdir(fp):
        return jsonify({'error': 'path is a directory'}), 400
    os.remove(fp)
    return jsonify({'ok': True})

@app.route('/api/bot/<bid>/file/<path:fn>/rename', methods=['POST'])
def rename_file(bid, fn):
    if not check_access(bid):
        return jsonify({'error': 'unauth'}), 401
    new_name = (request.json or {}).get('new_name', '').strip()
    if not new_name:
        return jsonify({'error': 'new_name required'}), 400
    src = safe_path(bid, fn)
    dst = safe_path(bid, new_name)
    if not src or not dst:
        return jsonify({'error': 'invalid path'}), 403
    if not os.path.exists(src):
        return jsonify({'error': 'source file not found'}), 404
    if os.path.isdir(src):
        return jsonify({'error': 'source is a directory'}), 400
    if os.path.exists(dst):
        return jsonify({'error': 'a file with that name already exists'}), 409
    dst_dir = os.path.dirname(dst)
    if dst_dir:
        os.makedirs(dst_dir, exist_ok=True)
    os.rename(src, dst)
    return jsonify({'ok': True, 'new_name': new_name})

@app.route('/api/bot/<bid>/file/<path:fn>/download')
def dl_file(bid, fn):
    if not check_access(bid):
        return jsonify({'error': 'unauth'}), 401
    fp = safe_path(bid, fn)
    if not fp or not os.path.exists(fp):
        return jsonify({'error': 'not found'}), 404
    if os.path.isdir(fp):
        return jsonify({'error': 'cannot download a directory'}), 400
    return send_file(fp, as_attachment=True)

@app.route('/api/bot/<bid>/upload', methods=['POST'])
def upload_route(bid):
    if not check_access(bid):
        return jsonify({'error': 'unauth'}), 401
    if 'file' not in request.files:
        return jsonify({'error': 'no file field'}), 400
    file = request.files['file']
    if not file or not file.filename:
        return jsonify({'error': 'no file selected'}), 400

    bd     = get_bot_dir(bid)
    abs_bd = os.path.abspath(bd)
    raw_relative  = (request.form.get('relative_path') or file.filename or '').strip()
    is_folder_up  = request.form.get('is_folder_upload', '0') == '1'
    if not raw_relative:
        return jsonify({'error': 'could not determine filename'}), 400

    normalized = raw_relative.replace('\\', '/')
    parts = [p for p in normalized.split('/') if p and p not in ('.', '..')]
    if is_folder_up and len(parts) > 1:
        parts = parts[1:]

    safe_parts = []
    for p in parts:
        s = secure_filename(p)
        if s:
            safe_parts.append(s)
        else:
            cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '', p).strip(' .')
            if cleaned:
                safe_parts.append(cleaned)

    if not safe_parts:
        return jsonify({'error': 'invalid filename'}), 400

    rel_path = '/'.join(safe_parts)
    dest     = os.path.abspath(os.path.join(abs_bd, *safe_parts))
    if dest != abs_bd and not dest.startswith(abs_bd + os.sep):
        return jsonify({'error': 'path traversal blocked'}), 403

    fname = safe_parts[-1]

    if fname.lower().endswith('.zip'):
        tmp_zip = dest
        try:
            parent = os.path.dirname(tmp_zip)
            if parent and parent != abs_bd:
                os.makedirs(parent, exist_ok=True)
            file.save(tmp_zip)
        except Exception as e:
            return jsonify({'error': f'Save failed: {e}'}), 500

        extracted, blocked = 0, 0
        try:
            with zipfile.ZipFile(tmp_zip, 'r') as zf:
                if any(m.flag_bits & 0x1 for m in zf.infolist()):
                    os.remove(tmp_zip)
                    return jsonify({'error': 'password-protected zip'}), 400

                all_file_parts = []
                for m in zf.infolist():
                    mp = m.filename.replace('\\', '/')
                    pc = [p for p in mp.split('/') if p and p not in ('.', '..')]
                    if pc:
                        all_file_parts.append(pc)

                strip_prefix = None
                if all_file_parts:
                    candidate = all_file_parts[0][0]
                    if all(p[0] == candidate for p in all_file_parts if p):
                        strip_prefix = candidate

                for member in zf.infolist():
                    member_path = member.filename.replace('\\', '/')
                    if member_path.endswith('/'):
                        continue
                    m_parts = [p for p in member_path.split('/') if p and p not in ('.', '..')]
                    if not m_parts:
                        continue
                    if strip_prefix and m_parts[0] == strip_prefix:
                        m_parts = m_parts[1:]
                    if not m_parts:
                        continue

                    safe_m_parts = []
                    for p in m_parts:
                        s = secure_filename(p)
                        if s:
                            safe_m_parts.append(s)
                        else:
                            cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '', p).strip(' .')
                            if cleaned:
                                safe_m_parts.append(cleaned)
                    if not safe_m_parts:
                        continue

                    dest_file = os.path.abspath(os.path.join(abs_bd, *safe_m_parts))
                    if not dest_file.startswith(abs_bd + os.sep) and dest_file != abs_bd:
                        blocked += 1
                        continue

                    os.makedirs(os.path.dirname(dest_file), exist_ok=True)
                    with zf.open(member) as src_f, open(dest_file, 'wb') as dst_f:
                        dst_f.write(src_f.read())
                    extracted += 1

            os.remove(tmp_zip)
            msg = f'[System] Extracted {extracted} file(s) from {fname}'
            if blocked:
                msg += f' ({blocked} path(s) blocked)'
            emit_log(bid, msg, 'success' if extracted else 'warn')
            cfg2 = load_config().get(bid, {})
            for _u in list({u for u in [cfg2.get('owner')] + cfg2.get('shared_with', []) if u}):
                with contextlib.suppress(Exception):
                    socketio.emit('files_changed', {'bot_id': bid}, room=_u)

        except zipfile.BadZipFile:
            if os.path.exists(tmp_zip):
                os.remove(tmp_zip)
            return jsonify({'error': 'bad zip'}), 400
        except Exception as e:
            if os.path.exists(tmp_zip):
                os.remove(tmp_zip)
            return jsonify({'error': str(e)}), 500
    else:
        try:
            parent = os.path.dirname(dest)
            if parent and parent != abs_bd:
                os.makedirs(parent, exist_ok=True)
            file.save(dest)
            emit_log(bid, f'[System] Uploaded {rel_path}', 'system')
            cfg3 = load_config().get(bid, {})
            for _u in list({u for u in [cfg3.get('owner')] + cfg3.get('shared_with', []) if u}):
                with contextlib.suppress(Exception):
                    socketio.emit('files_changed', {'bot_id': bid}, room=_u)
        except Exception as e:
            return jsonify({'error': f'Save failed: {e}'}), 500

    return jsonify({'ok': True, 'filename': rel_path})

# --- ENV & SETTINGS ROUTES -----------------------------------------------------

@app.route('/api/bot/<bid>/env')
def get_env(bid):
    if not check_access(bid):
        return jsonify({'error': 'unauth'}), 401
    return jsonify(load_config().get(bid, {}).get('env', {}))

@app.route('/api/bot/<bid>/env', methods=['PUT'])
def put_env(bid):
    if not check_owner(bid):
        return jsonify({'error': 'unauth'}), 401
    data = request.json
    if not isinstance(data, dict):
        return jsonify({'error': 'expected JSON object'}), 400
    cleaned = {str(k): str(v) for k, v in data.items() if k}
    cfg = load_config(); cfg.setdefault(bid, {})['env'] = cleaned; save_config(cfg)
    return jsonify({'ok': True})

@app.route('/api/bot/<bid>/settings', methods=['PUT'])
def put_settings(bid):
    if not check_owner(bid):
        return jsonify({'error': 'unauth'}), 401
    data = request.json or {}
    cfg  = load_config(); bc = cfg.setdefault(bid, {})
    name = str(data.get('name', bc.get('name', bid))).strip()[:64] or bid
    sf_raw = str(data.get('startup_file', 'main.py')).strip()
    startup_file = safe_startup_file(sf_raw) or 'main.py'
    btype = str(data.get('type', bc.get('type', 'bot'))).strip().lower()
    if btype not in ('bot', 'vps'): btype = 'bot'

    bc['name']         = name
    bc['startup_file'] = startup_file
    bc['type']         = btype
    bc['auto_restart'] = bool(data.get('auto_restart', False))
    save_config(cfg)
    if bid in bots:
        bots[bid]['auto_restart'] = bc['auto_restart']
    return jsonify(bc)

# --- RESOURCES ROUTE -----------------------------------------------------------

_cpu_cache = {'val': 0.0, 'ts': 0.0}
_cpu_lock  = threading.Lock()

def _update_cpu():
    while True:
        try:
            v = psutil.cpu_percent(interval=1)
            with _cpu_lock:
                _cpu_cache['val'] = v
                _cpu_cache['ts']  = time.time()
        except Exception:
            pass
        try:
            time.sleep(3)
        except Exception:
            break

try:
    threading.Thread(target=_update_cpu, daemon=True).start()
except Exception as e:
    log.warning(f'CPU monitor thread failed to start: {e}')

@app.route('/api/resources')
def resources():
    try:
        with _cpu_lock:
            cpu = _cpu_cache['val']
        mem  = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        def fmt(b):
            return f"{b // 1024 // 1024}MB" if b < 1024 ** 3 else f"{b / 1024 ** 3:.1f}GB"
        return jsonify({
            'cpu':       round(cpu, 1),
            'mem_used':  fmt(mem.used),
            'mem_total': fmt(mem.total),
            'mem_pct':   round(mem.percent, 1),
            'disk_used': fmt(disk.used),
            'disk_total':fmt(disk.total),
            'disk_pct':  round(disk.percent, 1),
        })
    except Exception:
        return jsonify({
            'cpu': 0, 'mem_used': '0MB', 'mem_total': '0MB',
            'mem_pct': 0, 'disk_used': '0MB', 'disk_total': '0MB', 'disk_pct': 0
        })


# --- ENTRY POINT ---------------------------------------------------------------

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    print('\n' + '\u2501' * 60)
    print(f'  VORTEX HOSTING v12.1  \u00b7  mode={_ASYNC_MODE}  \u00b7  port={port}')
    print(f'  Data: {CONFIG_FILE}')
    print(f'  Users: {USERS_FILE}')
    print('\u2501' * 60 + '\n')
    if _ASYNC_MODE == 'eventlet':
        socketio.run(app, host='0.0.0.0', port=port, debug=False)
    else:
        socketio.run(app, host='0.0.0.0', port=port, debug=False, allow_unsafe_werkzeug=True)
