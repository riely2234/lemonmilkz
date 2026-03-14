"""
Vortex Hosting v12.0 — UI Clarity + Remember Me + Mobile Fix + Data Persistence
Install: pip install flask flask-socketio psutil werkzeug eventlet
Run:     python main.py
Supported runtimes: Python, Node.js, TypeScript (ts-node), Bash, Rust (cargo), C# (dotnet)

Changes from v11.5:
  - Better UI: clearer labels, tooltips, onboarding hints, section descriptions
  - System clock shows local time + timezone + date
  - Remember Me: session persists across browser restarts via signed cookie
  - Data persistence: atomic writes + backup files survive GitHub-pull restarts
  - Mobile fix: no crash on load — removed eventlet monkey-patch issues on mobile,
    fixed viewport, touch events, SocketIO transport fallback
"""

try:
    import eventlet
    eventlet.monkey_patch()
    _ASYNC_MODE = 'eventlet'
except ImportError:
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
# Persistent sessions: 30-day lifetime, survives server restarts if SECRET_KEY is set
app.config['PERMANENT_SESSION_LIFETIME'] = 60 * 60 * 24 * 30  # 30 days
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_SECURE'] = False  # set True behind HTTPS

socketio = SocketIO(app, cors_allowed_origins='*', async_mode=_ASYNC_MODE,
    logger=False, engineio_logger=False, max_http_buffer_size=200 * 1024 * 1024)

BOTS_DIR    = os.path.join(os.getcwd(), 'vortex_bots')
CONFIG_FILE = os.path.join(os.getcwd(), 'vortex_config.json')
USERS_FILE  = os.path.join(os.getcwd(), 'vortex_users.json')
MAX_BOTS_PER_USER = 20
os.makedirs(BOTS_DIR, exist_ok=True)

_config_lock = threading.RLock()
_users_lock  = threading.RLock()
bots = {}

# ─── Data persistence helpers ─────────────────────────────────────────────────
# Atomic write: write to .tmp then os.replace — safe against mid-write crashes.
# Also keeps a .bak so a single bad write never loses all data.

def _atomic_write(path: str, data: dict):
    """Write JSON atomically with backup. Safe against restart mid-write."""
    tmp = path + '.tmp'
    bak = path + '.bak'
    try:
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        # Keep previous version as .bak before replacing
        if os.path.exists(path):
            shutil.copy2(path, bak)
        os.replace(tmp, path)
    except Exception as e:
        log.error(f'Atomic write failed for {path}: {e}')
        # Clean up orphaned tmp
        with contextlib.suppress(Exception):
            os.remove(tmp)
        raise

def _safe_read(path: str) -> dict:
    """Read JSON with fallback to .bak if main file is corrupt."""
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
        '# [Vortex] asyncio compatibility patch — safe for Python 3.10+\n'
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
            emit_log(bot_id, '[System] Installing Python requirements…', 'system')
            rc = _run_install(bot_id, [
                sys.executable, '-m', 'pip', 'install',
                '--no-input', '--no-color',
                '--disable-pip-version-check',
                '--progress-bar', 'off',
                '-r', req,
            ])
            if rc != 0:
                emit_log(bot_id, '[Error] pip install failed — see output above.', 'error')
                return
            emit_log(bot_id, '[System] Requirements installed.', 'success')

        if _patch_py_for_asyncio(bot_dir, startup_file):
            emit_log(bot_id, '[System] Applied asyncio compatibility patch.', 'system')

    elif ext in ('js', 'ts'):
        pkg = os.path.join(bot_dir, 'package.json')
        if os.path.exists(pkg):
            emit_log(bot_id, '[System] Running npm install…', 'system')
            rc = _run_install(bot_id,
                ['npm', 'install', '--no-progress', '--no-audit', '--no-fund'],
                cwd=bot_dir)
            if rc != 0:
                emit_log(bot_id, '[Error] npm install failed — see output above.', 'error')
                return
            emit_log(bot_id, '[System] npm packages installed.', 'success')
        if ext == 'ts' and not shutil.which('ts-node') and not shutil.which('npx'):
            emit_log(bot_id, '[Error] ts-node/npx not found. Install Node.js.', 'error')
            return

    elif ext == 'rs':
        if not os.path.exists(os.path.join(bot_dir, 'Cargo.toml')):
            emit_log(bot_id, '[Error] Cargo.toml not found.', 'error')
            return
        if not shutil.which('cargo'):
            emit_log(bot_id, '[Error] cargo not found. Install Rust toolchain.', 'error')
            return
        emit_log(bot_id, '[System] Building Rust project…', 'system')
        rc = _run_install(bot_id, ['cargo', 'build', '--release'],
                          cwd=bot_dir, timeout=600)
        if rc != 0:
            emit_log(bot_id, '[Error] cargo build failed.', 'error')
            return
        emit_log(bot_id, '[System] Rust build successful.', 'success')

    elif ext == 'cs':
        if not any(f.endswith('.csproj') for f in os.listdir(bot_dir)):
            emit_log(bot_id, '[Error] No .csproj file found.', 'error')
            return
        if not shutil.which('dotnet'):
            emit_log(bot_id, '[Error] dotnet not found. Install the .NET SDK.', 'error')
            return
        emit_log(bot_id, '[System] Building C# project…', 'system')
        rc = _run_install(bot_id,
            ['dotnet', 'build', '--configuration', 'Release'],
            cwd=bot_dir, timeout=600)
        if rc != 0:
            emit_log(bot_id, '[Error] dotnet build failed.', 'error')
            return
        emit_log(bot_id, '[System] C# build successful.', 'success')

    if ext == 'py':
        cmd = [sys.executable, '-u', full_path]
    elif ext == 'js':
        cmd = ['node', full_path]
    elif ext == 'ts':
        cmd = (['ts-node', full_path] if shutil.which('ts-node')
               else ['npx', 'ts-node', full_path])
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
        emit_log(bot_id, f'[System] Running: {binary}', 'system')
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

    emit_log(bot_id, f'[System] Starting {startup_file}…', 'system')
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
                emit_log(bot_id, f'[System] Auto-restart #{_restart_count+1} in {delay}s…', 'system')
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


# ─── HTML ─────────────────────────────────────────────────────────────────────


HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=5,user-scalable=yes">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="mobile-web-app-capable" content="yes">
<title>VORTEX HOSTING</title>
<script>window._sockReady=false;</script>
<script src="https://cdn.socket.io/4.6.1/socket.io.min.js" crossorigin="anonymous"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@400;500;600&family=Bebas+Neue&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
:root{
  /* Amber/orange terminal theme */
  --void:#0C0A06;--deep:#110E08;--base:#16120A;--panel:#1C1710;--elevated:#242018;
  --border:rgba(255,160,30,0.1);--border-mid:rgba(255,160,30,0.18);--border-hi:rgba(255,160,30,0.38);
  --amber:#FF9A00;--amber-dim:rgba(255,154,0,0.1);--amber-glow:rgba(255,154,0,0.35);
  --orange:#FF5C00;--orange-dim:rgba(255,92,0,0.1);
  --gold:#FFD060;--gold-dim:rgba(255,208,96,0.1);
  --green:#39FF8A;--green-dim:rgba(57,255,138,0.08);--green-glow:rgba(57,255,138,0.35);
  --red:#FF3A3A;--red-dim:rgba(255,58,58,0.1);--red-glow:rgba(255,58,58,0.35);
  --blue:#4DA6FF;--blue-dim:rgba(77,166,255,0.1);
  --text:#F5E8CC;--text-2:#7A6645;--text-3:#3D3020;
  --font-disp:'Bebas Neue',sans-serif;--font-sans:'DM Sans',sans-serif;--font-mono:'Space Mono',monospace;
  --ease-spring:cubic-bezier(0.34,1.56,0.64,1);--ease-out:cubic-bezier(0.16,1,0.3,1);
  --radius:6px;--radius-lg:10px;
}
html,body{width:100%;height:100%;overflow:hidden;background:var(--void);color:var(--text);font-family:var(--font-sans);-webkit-font-smoothing:antialiased}

/* Subtle scanline texture */
body::before{content:'';position:fixed;inset:0;z-index:0;pointer-events:none;
  background:repeating-linear-gradient(0deg,transparent,transparent 2px,rgba(0,0,0,0.04) 2px,rgba(0,0,0,0.04) 4px);
  opacity:.6}
/* Ambient glow */
#amb{position:fixed;inset:0;z-index:0;pointer-events:none;
  background:radial-gradient(ellipse 55% 35% at 0% 0%,rgba(255,120,0,0.07) 0%,transparent 65%),
             radial-gradient(ellipse 40% 50% at 100% 100%,rgba(255,60,0,0.05) 0%,transparent 65%),
             radial-gradient(ellipse 50% 60% at 50% 50%,rgba(255,160,0,0.025) 0%,transparent 75%)}
#app{display:flex;width:100%;height:100%;position:relative;z-index:1}
::-webkit-scrollbar{width:3px;height:3px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:rgba(255,154,0,0.25);border-radius:10px}
::-webkit-scrollbar-thumb:hover{background:rgba(255,154,0,0.45)}

/* ═══ SIDEBAR ═══════════════════════════════════════════════════════════════ */
.sidebar{width:248px;min-width:248px;height:100%;display:flex;flex-direction:column;
  background:linear-gradient(180deg,var(--deep) 0%,var(--void) 100%);
  border-right:1px solid var(--border-mid);
  box-shadow:4px 0 40px rgba(0,0,0,0.7);
  transition:transform .38s var(--ease-out);position:relative;z-index:9500}
.sidebar::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;
  background:linear-gradient(90deg,transparent,var(--amber),var(--orange),transparent);
  box-shadow:0 0 16px var(--amber-glow)}
.sidebar-close-btn{display:none;position:absolute;right:12px;top:18px;
  background:var(--elevated);border:1px solid var(--border-mid);color:var(--text-2);
  font-size:12px;cursor:pointer;width:26px;height:26px;border-radius:4px;
  align-items:center;justify-content:center;transition:all .18s;z-index:10}
.sidebar-close-btn:hover{color:var(--amber);border-color:var(--border-hi)}

.logo{padding:22px 20px 16px;border-bottom:1px solid var(--border)}
.logo-wordmark{font-family:var(--font-disp);font-size:32px;letter-spacing:8px;line-height:1;
  color:var(--gold);text-shadow:0 0 30px rgba(255,208,96,0.3);
  background:linear-gradient(135deg,var(--gold) 0%,var(--amber) 55%,var(--orange) 100%);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent}
.logo-sub{font-family:var(--font-mono);font-size:8px;letter-spacing:4px;color:var(--text-3);
  text-transform:uppercase;margin-top:4px}
.logo-line{position:absolute;bottom:0;left:0;right:0;height:1px;
  background:linear-gradient(90deg,var(--amber),var(--orange));opacity:.5}

.nav{padding:16px 10px 6px;flex-shrink:0}
.nav-section{font-family:var(--font-mono);font-size:8px;font-weight:700;letter-spacing:4px;
  color:var(--text-3);text-transform:uppercase;padding:6px 8px 5px;
  display:flex;align-items:center;gap:8px}
.nav-section::after{content:'';flex:1;height:1px;background:var(--border)}
.nav-item{display:flex;align-items:center;gap:10px;padding:9px 11px;border-radius:var(--radius);
  font-size:13px;font-weight:500;color:var(--text-2);cursor:pointer;
  transition:all .18s var(--ease-out);margin-bottom:2px;
  border:1px solid transparent;font-family:var(--font-sans);
  position:relative;overflow:hidden;-webkit-tap-highlight-color:transparent;outline:none}
.nav-item::before{content:'';position:absolute;left:0;top:0;bottom:0;width:2px;
  background:var(--amber);transform:scaleY(0);transition:transform .2s var(--ease-spring);border-radius:0 2px 2px 0}
.nav-item:hover{background:rgba(255,154,0,0.06);color:var(--text);border-color:var(--border);transform:translateX(2px)}
.nav-item.active{background:linear-gradient(90deg,rgba(255,154,0,0.12),rgba(255,154,0,0.04));
  color:var(--gold);border-color:rgba(255,154,0,0.22)}
.nav-item.active::before{transform:scaleY(1)}
.nav-icon{width:18px;text-align:center;font-size:14px;flex-shrink:0;opacity:.55;transition:opacity .15s}
.nav-item:hover .nav-icon,.nav-item.active .nav-icon{opacity:1}

.instances-header{display:flex;align-items:center;justify-content:space-between;
  padding:12px 20px 8px;flex-shrink:0;border-top:1px solid var(--border)}
.instances-label{font-family:var(--font-mono);font-size:8px;letter-spacing:4px;color:var(--text-3);text-transform:uppercase}
.instances-count{font-family:var(--font-mono);font-size:10px;font-weight:700;color:var(--amber);
  background:var(--amber-dim);border:1px solid var(--border-mid);border-radius:3px;padding:1px 7px}

.new-bot-btn{margin:0 10px 10px;display:flex;align-items:center;justify-content:center;gap:8px;
  padding:10px;border:1px dashed rgba(255,154,0,0.22);border-radius:var(--radius);
  font-family:var(--font-mono);font-size:10px;font-weight:700;letter-spacing:2px;
  color:var(--text-3);background:transparent;cursor:pointer;
  transition:all .22s var(--ease-out);text-transform:uppercase;
  -webkit-tap-highlight-color:transparent;outline:none}
.new-bot-btn:hover{border-color:var(--amber);color:var(--amber);background:var(--amber-dim);transform:translateY(-1px)}
.new-bot-btn-plus{width:18px;height:18px;border-radius:3px;background:var(--elevated);
  display:flex;align-items:center;justify-content:center;font-size:14px}

.bot-list{flex:1;overflow-y:auto;-webkit-overflow-scrolling:touch;padding:0 10px 10px;-webkit-tap-highlight-color:transparent}
.bot-item{display:flex;align-items:center;gap:9px;padding:10px 11px;border-radius:var(--radius);
  cursor:pointer;transition:all .18s;margin-bottom:4px;
  border:1px solid var(--border);background:rgba(0,0,0,0.2);
  -webkit-tap-highlight-color:transparent;outline:none}
.bot-item:hover{border-color:var(--border-mid);background:rgba(255,154,0,0.04)}
.bot-item.active{background:rgba(255,154,0,0.08);border-color:rgba(255,154,0,0.28);
  box-shadow:0 0 16px rgba(255,154,0,0.06)}
.bot-dot{width:7px;height:7px;border-radius:50%;flex-shrink:0}
.bot-dot.online{background:var(--green);box-shadow:0 0 8px var(--green-glow);animation:dp 2.2s ease-in-out infinite}
@keyframes dp{0%,100%{box-shadow:0 0 8px var(--green-glow)}50%{box-shadow:0 0 16px var(--green-glow),0 0 6px var(--green)}}
.bot-dot.offline{background:var(--text-3)}
.bot-name{font-family:var(--font-sans);font-size:13px;font-weight:500;color:var(--text-2);
  overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex:1;transition:color .15s}
.bot-item.active .bot-name,.bot-item:hover .bot-name{color:var(--text)}
.bot-status{font-family:var(--font-mono);font-size:8px;letter-spacing:2px;color:var(--text-3);text-transform:uppercase;margin-top:2px}
.bot-item.active .bot-status.online{color:var(--green)}
.bot-shared-tag{font-family:var(--font-mono);font-size:8px;color:var(--orange);
  background:var(--orange-dim);border:1px solid rgba(255,92,0,0.22);border-radius:3px;padding:1px 5px;flex-shrink:0}

.sidebar-footer{padding:12px 20px;border-top:1px solid var(--border);
  display:flex;justify-content:space-between;align-items:flex-end;
  background:rgba(0,0,0,0.45);flex-shrink:0;gap:8px}
.logout-btn{font-family:var(--font-mono);font-size:9px;letter-spacing:2px;color:var(--text-3);
  cursor:pointer;text-transform:uppercase;background:none;border:none;transition:color .15s}
.logout-btn:hover{color:var(--red)}
.sidebar-clock-wrap{display:flex;flex-direction:column;align-items:flex-end;gap:2px}
.sidebar-clock{font-family:var(--font-mono);font-size:14px;color:var(--amber);font-weight:700;letter-spacing:1px}
.sidebar-date{font-family:var(--font-mono);font-size:8px;color:var(--text-3);letter-spacing:.5px}
.sidebar-tz{font-family:var(--font-mono);font-size:7px;color:rgba(255,154,0,0.35);letter-spacing:.5px}

/* ═══ OVERLAY & MOBILE NAV ════════════════════════════════════════════════════ */
.sidebar-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,0.8);
  z-index:9400;opacity:0;transition:opacity .3s;cursor:pointer;backdrop-filter:blur(4px)}
.sidebar-overlay.open{opacity:1}

.mobile-bottom-nav{display:none;position:fixed;bottom:12px;left:10px;right:10px;height:60px;
  background:rgba(22,18,10,0.97);backdrop-filter:blur(30px);-webkit-backdrop-filter:blur(30px);
  border:1px solid var(--border-mid);border-radius:18px;
  z-index:9000;justify-content:space-around;align-items:center;padding:0 6px;
  box-shadow:0 8px 40px rgba(0,0,0,0.85),0 0 0 1px rgba(255,154,0,0.05)}

.m-nav-item{display:flex;flex-direction:column;align-items:center;gap:2px;
  color:var(--text-3);cursor:pointer;transition:all .22s var(--ease-spring);
  padding:6px 10px;border-radius:12px;flex:1;-webkit-tap-highlight-color:transparent;min-width:0}
.m-nav-icon{font-size:17px;transition:transform .22s var(--ease-spring);line-height:1}
.m-nav-label{font-family:var(--font-mono);font-size:8px;font-weight:700;letter-spacing:.5px;
  text-transform:uppercase;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:100%}
.m-nav-item.active{color:var(--amber)}
.m-nav-item.active .m-nav-icon{transform:translateY(-3px) scale(1.15);
  filter:drop-shadow(0 0 5px var(--amber-glow))}

/* Mobile process controls — fixed bar above bottom nav */
.mobile-proc-bar{display:none;position:fixed;bottom:84px;left:10px;right:10px;
  background:rgba(28,23,16,0.97);backdrop-filter:blur(20px);-webkit-backdrop-filter:blur(20px);
  border:1px solid var(--border-mid);border-radius:14px;
  z-index:8990;align-items:center;padding:8px 12px;gap:8px;
  box-shadow:0 4px 24px rgba(0,0,0,0.7)}
.mpb-status{display:flex;align-items:center;gap:6px;flex:1;min-width:0}
.mpb-dot{width:7px;height:7px;border-radius:50%;background:var(--text-3);flex-shrink:0}
.mpb-dot.online{background:var(--green);box-shadow:0 0 6px var(--green-glow);animation:dp 2.2s ease-in-out infinite}
.mpb-name{font-family:var(--font-mono);font-size:10px;color:var(--text-2);
  overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex:1}
.mpb-state{font-family:var(--font-mono);font-size:9px;font-weight:700;letter-spacing:1px;
  color:var(--text-3);text-transform:uppercase;flex-shrink:0}
.mpb-state.online{color:var(--green)}
.mpb-btns{display:flex;gap:5px;flex-shrink:0}
.mpb-btn{height:32px;padding:0 10px;border-radius:8px;font-size:9px;font-weight:700;
  letter-spacing:.5px;border:none;cursor:pointer;font-family:var(--font-mono);
  text-transform:uppercase;white-space:nowrap;transition:all .15s;-webkit-tap-highlight-color:transparent;touch-action:manipulation}
.mpb-btn.start{background:rgba(57,255,138,0.14);color:var(--green);border:1px solid rgba(57,255,138,0.3)}
.mpb-btn.stop{background:rgba(255,58,58,0.12);color:var(--red);border:1px solid rgba(255,58,58,0.28)}
.mpb-btn.restart{background:rgba(255,154,0,0.12);color:var(--amber);border:1px solid rgba(255,154,0,0.28)}

/* ═══ MAIN AREA ═══════════════════════════════════════════════════════════════ */
.main{flex:1;min-width:0;height:100%;display:flex;flex-direction:column;position:relative;z-index:10}

.topbar{height:58px;min-height:58px;
  background:linear-gradient(90deg,rgba(22,18,10,0.97),rgba(17,14,8,0.97));
  backdrop-filter:blur(20px);border-bottom:1px solid var(--border-mid);
  display:flex;align-items:center;justify-content:space-between;
  padding:0 22px;gap:12px;flex-shrink:0;
  box-shadow:0 2px 24px rgba(0,0,0,0.6);position:relative}
.topbar::after{content:'';position:absolute;bottom:-1px;left:0;right:0;height:1px;
  background:linear-gradient(90deg,transparent,rgba(255,154,0,0.18),transparent)}

.topbar-left{display:flex;align-items:center;gap:10px;min-width:0}
.mobile-menu-btn{display:none;background:var(--elevated);border:1px solid var(--border-mid);
  color:var(--text-2);padding:6px 10px;border-radius:var(--radius);font-size:14px;
  cursor:pointer;transition:all .15s;flex-shrink:0;touch-action:manipulation}
.mobile-menu-btn:hover{border-color:var(--border-hi);color:var(--amber)}

.breadcrumb{display:flex;align-items:center;gap:7px;min-width:0}
.bc-brand{font-family:var(--font-mono);font-size:10px;letter-spacing:3px;color:var(--text-3);text-transform:uppercase}
.bc-sep{color:rgba(255,160,30,0.3);opacity:.6}
.bc-page{font-family:var(--font-disp);font-size:20px;letter-spacing:4px;color:var(--text);text-transform:uppercase}
.bc-bot{font-family:var(--font-mono);font-size:10px;color:var(--orange);
  background:var(--orange-dim);padding:3px 9px;border-radius:3px;
  border:1px solid rgba(255,92,0,0.22);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:150px}

.topbar-right{display:flex;align-items:center;gap:7px;flex-shrink:0}

/* Runtime switcher */
.runtime-switcher{display:flex;align-items:center;background:rgba(0,0,0,0.5);
  border:1px solid var(--border-mid);border-radius:var(--radius);padding:3px;gap:2px;flex-shrink:0}
.runtime-btn{font-family:var(--font-mono);font-size:9px;font-weight:700;letter-spacing:.5px;
  text-transform:uppercase;padding:5px 9px;border-radius:4px;border:1px solid transparent;
  background:transparent;color:var(--text-3);cursor:pointer;transition:all .18s;
  white-space:nowrap;display:flex;align-items:center;gap:4px;touch-action:manipulation}
.runtime-btn:hover{color:var(--text);background:rgba(255,255,255,0.06)}
.runtime-btn.active.rt-py{background:rgba(57,255,138,0.1);color:var(--green);border-color:rgba(57,255,138,0.32)}
.runtime-btn.active.rt-ts{background:rgba(77,166,255,0.12);color:var(--blue);border-color:rgba(77,166,255,0.35)}
.runtime-btn.active.rt-js{background:rgba(255,208,96,0.12);color:var(--gold);border-color:rgba(255,208,96,0.38)}
.runtime-btn.active.rt-rs{background:rgba(255,92,0,0.12);color:var(--orange);border-color:rgba(255,92,0,0.35)}
.runtime-btn.active.rt-cs{background:rgba(180,120,255,0.12);color:#C090FF;border-color:rgba(180,120,255,0.35)}
.runtime-icon{font-size:11px;line-height:1}
.runtime-sep{width:1px;height:12px;background:var(--border-mid);flex-shrink:0}

.host-switcher{display:flex;align-items:center;background:rgba(0,0,0,0.5);
  border:1px solid var(--border-mid);border-radius:var(--radius);padding:3px;gap:2px}
.host-btn{font-family:var(--font-mono);font-size:9px;font-weight:700;letter-spacing:1.5px;
  text-transform:uppercase;padding:5px 10px;border-radius:4px;border:none;
  background:transparent;color:var(--text-3);cursor:pointer;transition:all .18s;white-space:nowrap}
.host-btn:hover{color:var(--text);background:rgba(255,255,255,0.06)}
.host-btn.active{background:rgba(255,154,0,0.14);color:var(--amber);
  border:1px solid rgba(255,154,0,0.28)}
.host-sep{width:1px;height:14px;background:var(--border-mid)}

.status-badge{display:flex;align-items:center;gap:6px;padding:5px 12px;
  border-radius:20px;font-family:var(--font-mono);font-size:9px;font-weight:700;
  letter-spacing:1.5px;transition:all .3s;border:1px solid var(--border);
  background:rgba(0,0,0,0.4);text-transform:uppercase}
.status-badge.online{color:var(--green);border-color:rgba(57,255,138,0.28);
  background:var(--green-dim);box-shadow:0 0 12px rgba(57,255,138,0.08)}
.status-badge.offline{color:var(--text-3);border-color:var(--border)}
.status-led{width:6px;height:6px;border-radius:50%;flex-shrink:0}
.status-badge.online .status-led{background:var(--green);box-shadow:0 0 7px var(--green-glow);animation:lb 1.8s ease-in-out infinite}
.status-badge.offline .status-led{background:var(--text-3)}
@keyframes lb{0%,100%{opacity:1}50%{opacity:.3}}

/* ═══ BUTTONS ═══════════════════════════════════════════════════════════════ */
.btn{display:inline-flex;align-items:center;justify-content:center;gap:5px;
  padding:7px 14px;border-radius:var(--radius);font-size:10px;font-weight:700;
  letter-spacing:1.5px;text-transform:uppercase;border:none;cursor:pointer;
  transition:all .15s var(--ease-out);font-family:var(--font-mono);
  position:relative;overflow:hidden;white-space:nowrap;touch-action:manipulation;
  -webkit-tap-highlight-color:transparent}
.btn:active{transform:scale(0.95)!important}
.btn:disabled{opacity:.4;cursor:not-allowed;pointer-events:none}
.btn-amber{background:rgba(255,154,0,0.14);color:var(--amber);border:1px solid rgba(255,154,0,0.32);box-shadow:0 0 12px rgba(255,154,0,0.08)}
.btn-amber:hover{background:rgba(255,154,0,0.24);border-color:rgba(255,154,0,0.55);transform:translateY(-1px);box-shadow:0 4px 18px rgba(255,154,0,0.18)}
.btn-green{background:var(--green-dim);color:var(--green);border:1px solid rgba(57,255,138,0.28)}
.btn-green:hover{background:rgba(57,255,138,0.16);border-color:rgba(57,255,138,0.5);transform:translateY(-1px);box-shadow:0 4px 14px rgba(57,255,138,0.14)}
.btn-red{background:var(--red-dim);color:var(--red);border:1px solid rgba(255,58,58,0.28)}
.btn-red:hover{background:rgba(255,58,58,0.2);border-color:rgba(255,58,58,0.5);transform:translateY(-1px);box-shadow:0 4px 14px rgba(255,58,58,0.14)}
.btn-orange{background:var(--orange-dim);color:var(--orange);border:1px solid rgba(255,92,0,0.28)}
.btn-orange:hover{background:rgba(255,92,0,0.2);border-color:rgba(255,92,0,0.5);transform:translateY(-1px)}
.btn-ghost{background:rgba(255,255,255,0.04);color:var(--text);border:1px solid var(--border-mid)}
.btn-ghost:hover{background:rgba(255,255,255,0.08);border-color:var(--border-hi);transform:translateY(-1px)}
.btn-sm{padding:5px 11px;font-size:9px;letter-spacing:1px}
.btn-row{display:flex;gap:7px;flex-wrap:wrap;align-items:center}

.icon-btn{width:28px;height:28px;display:inline-flex;align-items:center;justify-content:center;
  border-radius:4px;border:1px solid var(--border-mid);background:rgba(255,255,255,0.04);
  color:var(--text-2);cursor:pointer;transition:all .14s;font-size:12px;
  flex-shrink:0;padding:0;touch-action:manipulation}
.icon-btn:hover{border-color:var(--border-hi);color:var(--text);transform:translateY(-1px)}
.icon-btn.ib-amber{color:var(--amber);border-color:rgba(255,154,0,0.28);background:var(--amber-dim)}
.icon-btn.ib-amber:hover{background:rgba(255,154,0,0.2);border-color:rgba(255,154,0,0.5)}
.icon-btn.ib-red{color:var(--red);border-color:rgba(255,58,58,0.28);background:var(--red-dim)}
.icon-btn.ib-red:hover{background:rgba(255,58,58,0.2);border-color:rgba(255,58,58,0.5)}
.icon-btn.ib-cyan{color:var(--amber);border-color:rgba(255,154,0,0.22);background:var(--amber-dim)}
.icon-btn.ib-cyan:hover{background:rgba(255,154,0,0.18);border-color:rgba(255,154,0,0.5)}

/* ═══ PAGES ═════════════════════════════════════════════════════════════════ */
.page{flex:1;min-height:0;overflow-y:auto;-webkit-overflow-scrolling:touch;padding:20px;display:none}
.page.active{display:block;animation:pgIn .28s var(--ease-out)}
@keyframes pgIn{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}

/* ═══ STAT CARDS ════════════════════════════════════════════════════════════ */
.stats-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:16px}
.stat-card{background:var(--panel);border:1px solid var(--border);border-radius:var(--radius-lg);
  padding:16px;position:relative;overflow:hidden;transition:all .22s var(--ease-out)}
.stat-card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:var(--card-accent,var(--amber));opacity:.9}
.stat-card::after{content:'';position:absolute;top:0;left:0;right:0;bottom:0;
  background:radial-gradient(ellipse at top left,var(--card-glow,rgba(255,154,0,0.04)) 0%,transparent 65%);pointer-events:none}
.stat-card:hover{border-color:var(--border-mid);transform:translateY(-2px);box-shadow:0 8px 30px rgba(0,0,0,0.5)}
.sc-a{--card-accent:var(--amber);--card-glow:rgba(255,154,0,0.05)}
.sc-g{--card-accent:var(--green);--card-glow:rgba(57,255,138,0.04)}
.sc-o{--card-accent:var(--orange);--card-glow:rgba(255,92,0,0.04)}
.sc-b{--card-accent:var(--blue);--card-glow:rgba(77,166,255,0.04)}
.stat-label{font-family:var(--font-mono);font-size:9px;font-weight:700;letter-spacing:3px;
  color:var(--text-2);margin-bottom:9px;text-transform:uppercase}
.stat-value{font-family:var(--font-disp);font-size:34px;line-height:1;margin-bottom:5px}
.sv-a{color:var(--amber)} .sv-g{color:var(--green)} .sv-r{color:var(--red)} .sv-b{color:var(--blue)}
.stat-sub{font-family:var(--font-mono);font-size:9px;color:var(--text-3)}

/* ═══ PANELS ════════════════════════════════════════════════════════════════ */
.panel{background:var(--panel);border:1px solid var(--border);border-radius:var(--radius-lg);
  margin-bottom:14px;overflow:hidden;box-shadow:0 4px 20px rgba(0,0,0,0.4)}
.panel-head{display:flex;align-items:center;justify-content:space-between;
  padding:12px 18px;border-bottom:1px solid var(--border);
  background:linear-gradient(90deg,rgba(0,0,0,0.35),rgba(0,0,0,0.15));flex-wrap:wrap;gap:8px}
.panel-title{display:flex;align-items:center;gap:9px;font-family:var(--font-disp);font-size:14px;
  letter-spacing:3px;color:var(--text);text-transform:uppercase}
.panel-icon{width:24px;height:24px;border-radius:4px;background:var(--elevated);
  border:1px solid var(--border-mid);display:flex;align-items:center;justify-content:center;font-size:11px}
.panel-tag{font-family:var(--font-mono);font-size:9px;font-weight:700;letter-spacing:2px;
  text-transform:uppercase;color:var(--orange);padding:3px 8px;
  border:1px solid rgba(255,92,0,0.28);border-radius:3px;background:var(--orange-dim)}
.panel-body{padding:18px}

.section-hint{font-family:var(--font-mono);font-size:9px;color:var(--text-3);
  padding:8px 16px;background:rgba(0,0,0,0.22);border-bottom:1px solid var(--border);
  letter-spacing:.3px;line-height:1.6}

/* ═══ TERMINAL ══════════════════════════════════════════════════════════════ */
.term-chrome{background:rgba(0,0,0,0.5);border-bottom:1px solid var(--border);
  padding:9px 14px;display:flex;align-items:center;gap:9px}
.term-dots{display:flex;gap:5px}
.term-dot{width:10px;height:10px;border-radius:50%}
.term-title-txt{flex:1;text-align:center;font-family:var(--font-mono);font-size:9px;
  font-weight:700;letter-spacing:4px;color:var(--text-3);text-transform:uppercase}
.terminal{background:rgba(0,0,0,0.5);padding:14px;overflow-y:auto;
  -webkit-overflow-scrolling:touch;font-family:var(--font-mono);font-size:12px;line-height:1.75}
.log-row{display:flex;align-items:baseline;gap:10px;padding:2px 3px;
  border-radius:3px;transition:background .1s}
.log-row:hover{background:rgba(255,154,0,0.04)}
.log-ts{font-size:10px;color:var(--text-3);flex-shrink:0;min-width:54px}
.log-tag{font-size:8px;padding:2px 5px;border-radius:3px;flex-shrink:0;
  text-transform:uppercase;font-weight:700;letter-spacing:1.5px}
.log-tag.sys{background:var(--blue-dim);color:var(--blue);border:1px solid rgba(77,166,255,0.25)}
.log-tag.err{background:var(--red-dim);color:var(--red);border:1px solid rgba(255,58,58,0.28)}
.log-tag.ok{background:var(--green-dim);color:var(--green);border:1px solid rgba(57,255,138,0.25)}
.log-tag.warn{background:var(--amber-dim);color:var(--amber);border:1px solid rgba(255,154,0,0.28)}
.log-tag.out{background:rgba(255,255,255,0.04);color:var(--text-2);border:1px solid var(--border)}
.log-tag.in{background:rgba(255,154,0,0.08);color:var(--amber);border:1px solid rgba(255,154,0,0.28)}
.log-msg{flex:1;word-break:break-all}
.log-msg.sys{color:var(--blue)} .log-msg.err{color:var(--red)} .log-msg.ok{color:var(--green)}
.log-msg.warn{color:var(--amber)} .log-msg.out{color:var(--text-2)} .log-msg.in{color:var(--amber);opacity:.85}
.term-input-wrap{display:flex;align-items:center;gap:10px;
  background:rgba(0,0,0,0.45);border-top:1px solid var(--border);
  padding:10px 16px;transition:all .18s}
.term-input-wrap:focus-within{background:rgba(255,154,0,0.03);border-top-color:rgba(255,154,0,0.22)}
.term-prompt{font-family:var(--font-mono);font-size:14px;color:var(--amber);flex-shrink:0}
.term-input{flex:1;background:none;border:none;outline:none;
  font-family:var(--font-mono);font-size:12px;color:var(--amber);caret-color:var(--amber)}
.term-input::placeholder{color:var(--text-3)}

/* ═══ FORMS ═════════════════════════════════════════════════════════════════ */
.form-group{margin-bottom:16px}
.form-label{display:flex;align-items:center;gap:9px;font-family:var(--font-mono);font-size:9px;
  font-weight:700;letter-spacing:3px;color:var(--text-2);text-transform:uppercase;margin-bottom:8px}
.form-label::after{content:'';flex:1;height:1px;background:var(--border)}
.form-input,.form-select,.form-textarea{width:100%;background:rgba(0,0,0,0.45);
  border:1px solid var(--border);border-left:2px solid var(--border-mid);
  border-radius:var(--radius);padding:10px 13px;font-size:12px;color:var(--text);
  outline:none;font-family:var(--font-mono);transition:all .18s}
.form-input:focus,.form-select:focus,.form-textarea:focus{
  border-color:var(--border-hi);border-left-color:var(--amber);
  background:rgba(255,154,0,0.03);box-shadow:0 0 0 3px rgba(255,154,0,0.06)}
.form-input::placeholder,.form-textarea::placeholder{color:var(--text-3)}
.form-select option{background:var(--base)}
.form-textarea{resize:vertical;min-height:110px;line-height:1.7}
.form-row-2{display:grid;grid-template-columns:1fr 1fr;gap:16px}
.help-text{font-family:var(--font-mono);font-size:9px;color:var(--text-3);margin-top:5px;line-height:1.5}
.help-text.info{color:rgba(255,154,0,0.5);padding:6px 10px;background:rgba(255,154,0,0.04);
  border-left:2px solid rgba(255,154,0,0.2);border-radius:0 4px 4px 0;margin-top:7px}
.section-divider{font-family:var(--font-disp);font-size:13px;letter-spacing:3px;
  color:var(--amber);border-bottom:1px solid var(--border);padding-bottom:8px;
  margin:22px 0 14px;text-transform:uppercase}

/* ═══ ENV ════════════════════════════════════════════════════════════════════ */
.env-row{display:grid;grid-template-columns:1fr 1.5fr auto;gap:6px;margin-bottom:6px;align-items:center}
.env-field{background:rgba(0,0,0,0.45);border:1px solid var(--border);border-radius:4px;
  padding:8px 10px;font-family:var(--font-mono);font-size:11px;color:var(--text);
  outline:none;width:100%;transition:border-color .15s}
.env-field:focus{border-color:var(--border-hi)}
.env-key{color:var(--amber)}

/* ═══ FILE TABLE ════════════════════════════════════════════════════════════ */
.file-table-wrap{overflow-x:auto;-webkit-overflow-scrolling:touch;width:100%}
.file-table{width:100%;border-collapse:collapse}
.file-table th{font-family:var(--font-mono);font-size:9px;text-transform:uppercase;
  letter-spacing:3px;color:var(--text-3);padding:9px 13px;
  border-bottom:1px solid var(--border-mid);text-align:left;font-weight:700;
  background:rgba(0,0,0,0.28);white-space:nowrap}
.file-table td{padding:8px 13px;border-bottom:1px solid var(--border);
  vertical-align:middle;font-family:var(--font-mono);font-size:12px}
.file-table tr:last-child td{border-bottom:none}
.file-table tr:hover td{background:rgba(255,154,0,0.025)}
.fn-cell{display:flex;align-items:center;gap:7px;min-width:0}
.fn-icon{font-size:13px;opacity:.75;flex-shrink:0;line-height:1}
.fn-link{flex:1;min-width:0;color:var(--amber);cursor:pointer;transition:all .15s;
  overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.fn-link:hover{color:var(--gold);text-shadow:0 0 8px var(--amber-glow)}
.fn-rename{display:none;flex:1;align-items:center;gap:5px;min-width:0}
.fn-rename.on{display:flex}
.fn-rename-input{flex:1;min-width:0;background:rgba(0,0,0,0.65);border:1px solid var(--border-hi);
  border-bottom:2px solid var(--amber);border-radius:4px;padding:4px 8px;
  font-family:var(--font-mono);font-size:11px;color:var(--amber);outline:none}
.fn-rename-ok{background:var(--amber-dim);border:1px solid rgba(255,154,0,0.4);border-radius:3px;
  color:var(--amber);font-family:var(--font-mono);font-size:9px;font-weight:700;
  padding:3px 7px;cursor:pointer;white-space:nowrap;flex-shrink:0;transition:all .14s}
.fn-rename-ok:hover{background:rgba(255,154,0,0.22)}
.fn-rename-cancel{background:transparent;border:1px solid var(--border);border-radius:3px;
  color:var(--text-3);font-family:var(--font-mono);font-size:9px;padding:3px 6px;
  cursor:pointer;flex-shrink:0;transition:all .14s}
.fn-rename-cancel:hover{color:var(--text);border-color:var(--border-hi)}
.file-ext-badge{font-size:8px;letter-spacing:1.5px;text-transform:uppercase;padding:2px 5px;
  border-radius:3px;border:1px solid var(--border-mid);color:var(--text-2);
  background:rgba(0,0,0,0.35);white-space:nowrap}
.file-actions{display:flex;gap:3px;align-items:center}

/* ═══ UPLOAD ════════════════════════════════════════════════════════════════ */
.drop-zone{border:2px dashed rgba(255,154,0,0.2);padding:32px 18px;text-align:center;
  transition:all .28s;background:rgba(0,0,0,0.2);border-radius:var(--radius-lg);cursor:pointer}
.drop-zone.dragging{border-color:var(--amber);background:rgba(255,154,0,0.04);
  box-shadow:0 0 30px rgba(255,154,0,0.08)}
.drop-icon{font-size:36px;margin-bottom:10px;display:block;color:var(--text-3);
  transition:all .28s;pointer-events:none}
.drop-zone.dragging .drop-icon{color:var(--amber);transform:translateY(-4px)}
.drop-title{font-family:var(--font-disp);font-size:18px;letter-spacing:4px;
  color:var(--text);margin-bottom:6px;pointer-events:none}
.drop-sub{font-family:var(--font-mono);font-size:9px;letter-spacing:1.5px;
  color:var(--text-3);margin-bottom:16px;pointer-events:none}
.upload-row{display:flex;align-items:center;gap:8px;padding:8px 12px;
  background:rgba(0,0,0,0.45);border:1px solid var(--border);border-radius:5px;
  margin-top:4px;font-family:var(--font-mono);font-size:10px;color:var(--text-2)}
.upload-bar-wrap{flex:1;height:3px;background:rgba(255,154,0,0.1);border-radius:2px;overflow:hidden}
.upload-bar-fill{height:100%;background:linear-gradient(90deg,var(--amber),var(--orange));
  border-radius:2px;transition:width .1s linear}

/* ═══ RESOURCES ═════════════════════════════════════════════════════════════ */
.res-item{margin-bottom:20px}
.res-header{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:8px}
.res-label{font-family:var(--font-mono);font-size:9px;letter-spacing:3px;
  text-transform:uppercase;color:var(--text-2)}
.res-value{font-family:var(--font-disp);font-size:22px}
.res-track{height:4px;background:rgba(0,0,0,0.55);border:1px solid var(--border);
  border-radius:2px;overflow:hidden}
.res-fill{height:100%;border-radius:2px;transition:width 1s ease}

/* ═══ MODAL ══════════════════════════════════════════════════════════════════ */
.modal-veil{display:none;position:fixed;inset:0;background:rgba(0,0,0,0.82);
  z-index:10000;align-items:center;justify-content:center;
  backdrop-filter:blur(8px);-webkit-backdrop-filter:blur(8px);padding:12px}
.modal-veil.open{display:flex;animation:vi .22s ease}
@keyframes vi{from{opacity:0}to{opacity:1}}
.modal-box{background:linear-gradient(160deg,var(--elevated),var(--base));
  border:1px solid var(--border-mid);border-top:2px solid var(--amber);
  border-radius:var(--radius-lg);padding:28px;width:100%;max-width:540px;
  max-height:90vh;overflow-y:auto;-webkit-overflow-scrolling:touch;
  box-shadow:0 20px 70px rgba(0,0,0,0.9),0 0 50px rgba(255,154,0,0.05);
  animation:mi .3s var(--ease-spring)}
.modal-box.wide{max-width:960px}
@keyframes mi{from{transform:scale(0.94) translateY(18px);opacity:0}to{transform:scale(1) translateY(0);opacity:1}}
.modal-title{font-family:var(--font-disp);font-size:24px;color:var(--text);
  margin-bottom:22px;letter-spacing:4px;display:flex;align-items:center;gap:9px}
.modal-title-accent{color:var(--amber)}
.modal-footer{display:flex;justify-content:flex-end;gap:9px;margin-top:22px;
  padding-top:18px;border-top:1px solid var(--border)}

/* ═══ LOGIN ══════════════════════════════════════════════════════════════════ */
#loginOverlay{position:fixed;inset:0;background:var(--void);z-index:99999;
  display:flex;align-items:center;justify-content:center;padding:12px}
#loginOverlay::before{content:'';position:absolute;inset:0;pointer-events:none;
  background:radial-gradient(ellipse 60% 50% at 30% 30%,rgba(255,120,0,0.08) 0%,transparent 60%),
             radial-gradient(ellipse 50% 60% at 80% 70%,rgba(255,60,0,0.06) 0%,transparent 60%)}
.login-card{background:linear-gradient(160deg,rgba(28,23,16,0.94),rgba(17,14,8,0.97));
  backdrop-filter:blur(40px);-webkit-backdrop-filter:blur(40px);
  border:1px solid var(--border-mid);border-top:2px solid var(--amber);
  padding:44px 38px;width:100%;max-width:400px;border-radius:var(--radius-lg);
  box-shadow:0 20px 70px rgba(0,0,0,0.9),0 0 50px rgba(255,154,0,0.06);
  animation:lu .65s var(--ease-spring);position:relative;z-index:1}
@keyframes lu{from{transform:translateY(36px) scale(0.96);opacity:0}to{transform:translateY(0) scale(1);opacity:1}}
.login-logo{font-family:var(--font-disp);font-size:52px;letter-spacing:10px;text-align:center;
  margin-bottom:4px;background:linear-gradient(135deg,var(--gold),var(--amber) 55%,var(--orange));
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;
  filter:drop-shadow(0 0 25px rgba(255,154,0,0.28))}
.login-tagline{font-family:var(--font-mono);color:var(--text-3);font-size:9px;letter-spacing:6px;
  text-transform:uppercase;text-align:center;margin-bottom:24px}
.auth-tabs{display:flex;background:rgba(0,0,0,0.4);border-radius:5px;padding:3px;
  margin-bottom:18px;border:1px solid var(--border)}
.auth-tab{flex:1;text-align:center;padding:7px 10px;font-family:var(--font-mono);font-size:10px;
  font-weight:700;letter-spacing:2px;color:var(--text-3);cursor:pointer;
  text-transform:uppercase;transition:all .22s;border-radius:3px}
.auth-tab.active{color:var(--amber);background:var(--amber-dim);
  border:1px solid rgba(255,154,0,0.3)}

.remember-row{display:flex;align-items:center;gap:9px;margin-bottom:13px;
  cursor:pointer;user-select:none;-webkit-user-select:none}
.remember-check{width:16px;height:16px;border:1px solid var(--border-mid);border-radius:3px;
  background:rgba(0,0,0,0.45);display:flex;align-items:center;justify-content:center;
  transition:all .18s;flex-shrink:0}
.remember-check.on{background:var(--amber-dim);border-color:rgba(255,154,0,0.5)}
.remember-check.on::after{content:'✓';font-size:9px;color:var(--amber);font-weight:700}
.remember-label{font-family:var(--font-mono);font-size:9px;letter-spacing:1.5px;
  color:var(--text-3);text-transform:uppercase;transition:color .15s}
.remember-row:hover .remember-label{color:var(--text-2)}

/* ═══ CODE EDITOR ═══════════════════════════════════════════════════════════ */
.code-editor{width:100%;min-height:460px;background:rgba(0,0,0,0.65);
  border:1px solid var(--border);border-left:3px solid var(--orange);
  border-radius:var(--radius);padding:16px;font-family:var(--font-mono);
  font-size:12px;color:var(--text);outline:none;resize:vertical;
  line-height:1.75;caret-color:var(--amber);transition:border-color .18s}
.code-editor:focus{border-left-color:var(--amber);border-color:var(--border-hi)}

/* ═══ DANGER ZONE ═══════════════════════════════════════════════════════════ */
.danger-zone{border:1px solid rgba(255,58,58,0.22);border-left:3px solid var(--red);
  background:linear-gradient(90deg,rgba(255,58,58,0.05),transparent);
  padding:18px;margin-top:16px;border-radius:var(--radius-lg)}

/* ═══ SUBUSERS ══════════════════════════════════════════════════════════════ */
.subuser-row{display:flex;justify-content:space-between;align-items:center;
  padding:9px 12px;background:rgba(0,0,0,0.3);border:1px solid var(--border);
  border-radius:5px;margin-bottom:4px;font-family:var(--font-mono);font-size:11px}

/* ═══ TOASTS ════════════════════════════════════════════════════════════════ */
.toast-tray{position:fixed;bottom:22px;right:22px;z-index:20000;
  display:flex;flex-direction:column;gap:8px;pointer-events:none}
.toast{background:var(--elevated);border:1px solid var(--border-mid);border-radius:var(--radius-lg);
  padding:11px 14px;font-size:12px;font-weight:500;color:var(--text);font-family:var(--font-sans);
  box-shadow:0 6px 26px rgba(0,0,0,0.7);display:flex;align-items:center;gap:9px;
  pointer-events:all;min-width:220px;animation:ti .3s var(--ease-spring);position:relative;overflow:hidden}
.toast::before{content:'';position:absolute;left:0;top:0;bottom:0;width:3px;border-radius:3px 0 0 3px}
.toast.success::before{background:var(--green)}
.toast.error::before{background:var(--red)}
.toast.info::before{background:var(--amber)}
.toast-icon{width:18px;height:18px;border-radius:3px;display:flex;align-items:center;
  justify-content:center;font-size:10px;flex-shrink:0}
.toast.success .toast-icon{background:var(--green-dim);color:var(--green)}
.toast.error .toast-icon{background:var(--red-dim);color:var(--red)}
.toast.info .toast-icon{background:var(--amber-dim);color:var(--amber)}
.toast-close{margin-left:auto;cursor:pointer;color:var(--text-3);font-size:12px;transition:color .15s}
.toast-close:hover{color:var(--text)}
@keyframes ti{from{transform:translateX(16px);opacity:0}to{transform:translateX(0);opacity:1}}

/* ═══ RESPONSIVE ═════════════════════════════════════════════════════════════ */
@media(max-width:860px){
  html,body{overflow:hidden;height:100%;height:100dvh}
  #app{height:100%;height:100dvh}
  .sidebar{position:fixed;left:0;top:0;bottom:0;transform:translateX(-110%);
    width:262px;z-index:9600;overflow-y:auto}
  .sidebar.open{transform:translateX(0)}
  .sidebar-overlay{display:block}
  .sidebar .nav{display:none}
  .sidebar-close-btn{display:flex}
  .mobile-bottom-nav{display:flex}
  .mobile-proc-bar{display:flex}
  .mobile-menu-btn{display:flex}
  /* Hide desktop process buttons on mobile — use the proc bar instead */
  .topbar-right .btn-group-proc{display:none}
  .main{height:100%;height:100dvh;padding-bottom:156px}
  .page{padding:8px;overflow-y:auto;-webkit-overflow-scrolling:touch}
  .topbar{height:auto;min-height:52px;padding:9px 12px;flex-direction:column;
    align-items:stretch;gap:8px;flex-shrink:0}
  .topbar-left{width:100%;justify-content:space-between}
  .breadcrumb{flex:1;margin-left:7px}
  .bc-page{font-size:17px}
  .bc-brand,.bc-sep:first-of-type{display:none}
  .bc-bot{max-width:100px;font-size:9px}
  .topbar-right{display:flex;flex-wrap:nowrap;overflow-x:auto;width:100%;gap:5px;
    border-top:1px solid var(--border);padding-top:8px;-webkit-overflow-scrolling:touch}
  .topbar-right::-webkit-scrollbar{display:none}
  .runtime-btn{padding:4px 6px;font-size:8px}
  .runtime-btn .runtime-icon{display:none}
  .host-btn{padding:4px 7px;font-size:8px}
  .stats-grid{grid-template-columns:1fr 1fr;gap:8px;margin-bottom:10px}
  .stat-value{font-size:26px}
  .form-row-2{grid-template-columns:1fr;gap:10px}
  .panel-head{padding:10px 12px;flex-direction:column;align-items:flex-start;gap:7px}
  .panel-body{padding:12px}
  .file-table th:nth-child(4),.file-table td:nth-child(4){display:none}
  .file-table th:nth-child(2),.file-table td:nth-child(2){display:none}
  .drop-zone{padding:20px 10px}
  .toast-tray{bottom:164px;right:8px;left:8px}
  .toast{min-width:0;width:100%}
  .login-card{padding:26px 14px}
}
@media(max-width:480px){
  .stats-grid{grid-template-columns:1fr}
  .stat-value{font-size:22px}
  .login-card{padding:24px 12px}
  .login-logo{font-size:42px;letter-spacing:7px}
  .modal-box{padding:16px 10px}
  .modal-title{font-size:18px;margin-bottom:14px}
  .page{padding:6px}
  .terminal{font-size:10px}
  .mobile-proc-bar{flex-wrap:wrap;gap:6px}
  .mpb-btns{flex:0 0 100%;justify-content:space-between}
  .mpb-btn{flex:1;justify-content:center}
}
</style>
</head>
<body>
<div id="amb"></div>

<!-- LOGIN -->
<div id="loginOverlay">
  <div class="login-card">
    <div class="login-logo">VORTEX</div>
    <div class="login-tagline">Hosting Platform v12.0</div>
    <div class="auth-tabs">
      <div class="auth-tab active" id="tabLogin" onclick="switchAuthMode('login')">Login</div>
      <div class="auth-tab" id="tabRegister" onclick="switchAuthMode('register')">Register</div>
    </div>
    <div class="form-group" style="margin-bottom:10px">
      <input class="form-input" id="authUsername" placeholder="Username" autocomplete="username"
        style="text-align:center;letter-spacing:2px" onkeydown="if(event.key==='Enter')submitAuth()">
    </div>
    <div class="form-group" style="margin-bottom:13px">
      <input type="password" class="form-input" id="authPassword" placeholder="Password"
        autocomplete="current-password" style="text-align:center;letter-spacing:2px"
        onkeydown="if(event.key==='Enter')submitAuth()">
    </div>
    <div class="remember-row" onclick="toggleRememberMe()">
      <div class="remember-check" id="rememberCheck"></div>
      <span class="remember-label">Remember me for 30 days</span>
    </div>
    <button class="btn btn-amber" id="authBtn"
      style="width:100%;padding:13px;font-size:11px;letter-spacing:4px" onclick="submitAuth()">
      AUTHENTICATE
    </button>
    <p style="font-family:var(--font-mono);font-size:8px;color:var(--text-3);text-align:center;
      margin-top:11px;letter-spacing:1px">Your instances and files persist across server restarts</p>
  </div>
</div>

<div class="sidebar-overlay" id="sidebarOverlay" onclick="toggleSidebar()"></div>

<!-- SOCKET.IO GUARD -->
<script>
if(typeof io==='undefined'){
  document.body.innerHTML='<div style="color:#FF9A00;font-family:monospace;padding:40px;background:#0C0A06;height:100vh;display:flex;align-items:center;justify-content:center;flex-direction:column;gap:14px;text-align:center"><div style="font-size:34px">⚠</div><div style="font-size:16px;letter-spacing:2px">SOCKET.IO FAILED TO LOAD</div><div style="font-size:11px;color:#3D3020;max-width:300px;margin-top:4px">Check your internet connection and reload. If using a content blocker, allow cdn.socket.io.</div><button onclick="location.reload()" style="background:rgba(255,154,0,0.12);border:1px solid rgba(255,154,0,0.35);color:#FF9A00;font-family:monospace;padding:9px 22px;border-radius:5px;cursor:pointer;font-size:11px;letter-spacing:2px;margin-top:6px">↺ RELOAD</button></div>';
}
</script>

<div id="app">
  <!-- SIDEBAR -->
  <aside class="sidebar" id="sidebar">
    <button class="sidebar-close-btn" onclick="toggleSidebar()">✕</button>
    <div class="logo" style="position:relative">
      <div class="logo-wordmark">VORTEX</div>
      <div class="logo-sub">Hosting Platform // v12.0</div>
      <div class="logo-line"></div>
    </div>
    <nav class="nav">
      <div class="nav-section">Monitor</div>
      <div class="nav-item active" data-page="dashboard" onclick="navTo('dashboard',this)">
        <span class="nav-icon">◈</span> Dashboard
      </div>
      <div class="nav-item" data-page="console" onclick="navTo('console',this)">
        <span class="nav-icon">_</span> Console
      </div>
      <div class="nav-item" data-page="resources" onclick="navTo('resources',this)">
        <span class="nav-icon">▣</span> Resources
      </div>
      <div class="nav-section" style="margin-top:8px">Manage</div>
      <div class="nav-item" data-page="files" onclick="navTo('files',this)">
        <span class="nav-icon">≡</span> File Manager
      </div>
      <div class="nav-item" data-page="env" onclick="navTo('env',this)">
        <span class="nav-icon">⊛</span> Environment
      </div>
      <div class="nav-item" data-page="settings" onclick="navTo('settings',this)">
        <span class="nav-icon">⚙</span> Settings
      </div>
    </nav>
    <div class="instances-header">
      <span class="instances-label">My Instances</span>
      <span class="instances-count" id="botCount">0</span>
    </div>
    <div class="new-bot-btn" onclick="openCreateModal()">
      <div class="new-bot-btn-plus">+</div> Deploy New Instance
    </div>
    <div class="bot-list" id="botList"></div>
    <div class="sidebar-footer">
      <button class="logout-btn" onclick="logout()">⏻ Logout</button>
      <div class="sidebar-clock-wrap">
        <span class="sidebar-clock" id="clock">00:00:00</span>
        <span class="sidebar-date" id="clockDate">---</span>
        <span class="sidebar-tz" id="clockTz">---</span>
      </div>
    </div>
  </aside>

  <!-- MAIN -->
  <main class="main">
    <div class="topbar">
      <div class="topbar-left">
        <button class="mobile-menu-btn" onclick="toggleSidebar()">☰</button>
        <div class="breadcrumb">
          <span class="bc-brand">VORTEX</span>
          <span class="bc-sep">/</span>
          <span class="bc-page" id="tbPage">DASHBOARD</span>
          <span class="bc-sep">·</span>
          <span class="bc-bot" id="tbBot">— SELECT —</span>
        </div>
      </div>
      <div class="topbar-right">
        <div class="runtime-switcher" id="runtimeSwitcher">
          <button class="runtime-btn rt-py active" onclick="setRuntime('py')" title="Python (.py)"><span class="runtime-icon">🐍</span> PY</button>
          <div class="runtime-sep"></div>
          <button class="runtime-btn rt-js" onclick="setRuntime('js')" title="JavaScript (.js)"><span class="runtime-icon">⚡</span> JS</button>
          <div class="runtime-sep"></div>
          <button class="runtime-btn rt-ts" onclick="setRuntime('ts')" title="TypeScript (.ts)"><span class="runtime-icon">⟨⟩</span> TS</button>
          <div class="runtime-sep"></div>
          <button class="runtime-btn rt-rs" onclick="setRuntime('rs')" title="Rust (.rs)"><span class="runtime-icon">⚙</span> RS</button>
          <div class="runtime-sep"></div>
          <button class="runtime-btn rt-cs" onclick="setRuntime('cs')" title="C# (.cs)"><span class="runtime-icon">◆</span> C#</button>
        </div>
        <div class="host-sep" style="height:18px;margin:0 1px"></div>
        <div class="host-switcher" id="hostSwitcher"></div>
        <div class="host-sep" style="height:18px;margin:0 1px"></div>
        <div class="status-badge offline" id="statusTag">
          <div class="status-led"></div>
          <span id="statusText">OFFLINE</span>
        </div>
        <!-- Desktop process buttons — hidden on mobile, replaced by mobile-proc-bar -->
        <div class="btn-group-proc" style="display:flex;gap:6px">
          <button class="btn btn-green btn-sm" onclick="startBot()" title="Start process">▶ START</button>
          <button class="btn btn-red btn-sm" onclick="stopBot()" title="Stop process">■ STOP</button>
          <button class="btn btn-amber btn-sm" onclick="restartBot()" title="Restart process">↺ RESTART</button>
        </div>
      </div>
    </div>

    <!-- DASHBOARD -->
    <div class="page active" id="page-dashboard">
      <div class="stats-grid">
        <div class="stat-card sc-g">
          <div class="stat-label">Process Status</div>
          <div class="stat-value sv-r" id="sStat">OFFLINE</div>
          <div class="stat-sub" id="sStatSub">Process not running</div>
        </div>
        <div class="stat-card sc-a">
          <div class="stat-label">Uptime</div>
          <div class="stat-value sv-a" id="sUptime">—</div>
          <div class="stat-sub">HH:MM:SS since last start</div>
        </div>
        <div class="stat-card sc-o">
          <div class="stat-label">CPU Usage</div>
          <div class="stat-value sv-a" id="sCpu">—</div>
          <div class="stat-sub">System-wide load</div>
        </div>
        <div class="stat-card sc-b">
          <div class="stat-label">Memory Used</div>
          <div class="stat-value sv-b" id="sMem">—</div>
          <div class="stat-sub">RAM consumed</div>
        </div>
      </div>
      <div class="panel">
        <div class="panel-head">
          <div class="panel-title"><div class="panel-icon">▶</div> Launch Control</div>
          <span class="panel-tag">Process Manager</span>
        </div>
        <div class="section-hint">Select an instance from the sidebar, set the startup file, then press Start.</div>
        <div class="panel-body">
          <div style="max-width:460px;margin-bottom:16px">
            <div class="form-group" style="margin:0">
              <label class="form-label">Startup File <span style="font-size:8px;color:var(--text-3);font-weight:400;letter-spacing:1px">(entry point)</span></label>
              <input class="form-input" id="sfInput" value="main.py"
                placeholder="main.py / index.js / src/main.rs / Program.cs">
              <p class="help-text">The file Vortex executes when you press Start. Change the Runtime button to match your language.</p>
            </div>
          </div>
          <div class="btn-row">
            <button class="btn btn-green" onclick="startBot()">▶ Start Process</button>
            <button class="btn btn-red" onclick="stopBot()">■ Stop</button>
            <button class="btn btn-amber" onclick="restartBot()">↺ Restart</button>
            <button class="btn btn-ghost" onclick="killBot()"
              style="margin-left:auto;color:var(--red)">✕ Force Kill</button>
          </div>
        </div>
      </div>
      <div class="panel">
        <div class="panel-head">
          <div class="panel-title"><div class="panel-icon">≡</div> Live Output</div>
          <button class="btn btn-ghost btn-sm" onclick="navTo('console',null)">Full Console →</button>
        </div>
        <div class="term-chrome">
          <div class="term-dots">
            <div class="term-dot" style="background:#FF5F57"></div>
            <div class="term-dot" style="background:#FFBD2E"></div>
            <div class="term-dot" style="background:#28CA42"></div>
          </div>
          <div class="term-title-txt">stdout // live feed</div>
        </div>
        <div class="terminal" id="miniTerm" style="height:200px"></div>
      </div>
    </div>

    <!-- CONSOLE -->
    <div class="page" id="page-console">
      <div class="panel">
        <div class="panel-head">
          <div class="panel-title"><div class="panel-icon">_</div> Process Console</div>
          <div class="btn-row">
            <button class="btn btn-ghost btn-sm" onclick="clearConsole()">⊘ Clear</button>
            <button class="btn btn-ghost btn-sm" onclick="exportLogs()">↓ Export Logs</button>
          </div>
        </div>
        <div class="section-hint">All stdout/stderr from your process appears here. Send text to stdin using the input below.</div>
        <div class="term-chrome">
          <div class="term-dots">
            <div class="term-dot" style="background:#FF5F57"></div>
            <div class="term-dot" style="background:#FFBD2E"></div>
            <div class="term-dot" style="background:#28CA42"></div>
          </div>
          <div class="term-title-txt" id="termTitle">NO INSTANCE SELECTED</div>
        </div>
        <div class="terminal" id="mainTerm" style="height:430px"></div>
        <div class="term-input-wrap">
          <span class="term-prompt">❯</span>
          <input class="term-input" id="termIn"
            placeholder="Type a command and press Enter to send to stdin…"
            onkeydown="if(event.key==='Enter')sendInput()">
          <button class="btn btn-amber btn-sm" onclick="sendInput()">Send</button>
        </div>
      </div>
    </div>

    <!-- FILES -->
    <div class="page" id="page-files">
      <input type="file" multiple id="fileUploadInput" style="display:none"
        onchange="handleUpload(this.files,false)">
      <input type="file" webkitdirectory directory multiple id="folderUploadInput" style="display:none"
        onchange="handleUpload(this.files,true)">
      <div class="panel">
        <div class="panel-head">
          <div class="panel-title"><div class="panel-icon">≡</div> File Manager</div>
          <div class="btn-row">
            <button class="btn btn-ghost btn-sm" onclick="openNewFileModal()">+ New File</button>
            <button class="btn btn-amber btn-sm" onclick="loadFiles()">↻ Refresh</button>
          </div>
        </div>
        <div class="section-hint">Files inside this instance's directory. Click a name to edit. Upload a ZIP to extract automatically.</div>
        <div class="file-table-wrap">
          <table class="file-table">
            <colgroup><col><col style="width:64px"><col style="width:58px"><col style="width:140px"><col style="width:120px"></colgroup>
            <thead><tr><th>Filename</th><th>Type</th><th>Size</th><th>Modified</th><th>Actions</th></tr></thead>
            <tbody id="fileList"></tbody>
          </table>
        </div>
        <div style="padding:12px 14px;border-top:1px solid var(--border)">
          <div class="drop-zone" id="dropZone">
            <span class="drop-icon">⇪</span>
            <div class="drop-title">DROP FILES HERE</div>
            <div class="drop-sub">ZIP auto-extracted · folder structure preserved</div>
            <div style="display:flex;gap:8px;justify-content:center;flex-wrap:wrap;position:relative;z-index:5">
              <button class="btn btn-amber btn-sm"
                onclick="event.stopPropagation();document.getElementById('fileUploadInput').click()">⇪ Upload Files</button>
              <button class="btn btn-orange btn-sm"
                onclick="event.stopPropagation();document.getElementById('folderUploadInput').click()">📁 Upload Folder</button>
            </div>
          </div>
          <div id="uploadProgress" style="margin-top:7px"></div>
        </div>
      </div>
    </div>

    <!-- ENV -->
    <div class="page" id="page-env">
      <div class="panel">
        <div class="panel-head">
          <div class="panel-title"><div class="panel-icon">⊛</div> Environment Variables</div>
          <button class="btn btn-amber btn-sm" onclick="saveEnv()">💾 Save Variables</button>
        </div>
        <div class="section-hint">Key=value pairs injected into your process at startup. Common uses: API tokens, database URLs, port numbers.</div>
        <div class="panel-body">
          <div class="env-row" style="margin-bottom:8px">
            <span style="font-family:var(--font-mono);font-size:8px;letter-spacing:3px;color:var(--text-3);text-transform:uppercase">KEY</span>
            <span style="font-family:var(--font-mono);font-size:8px;letter-spacing:3px;color:var(--text-3);text-transform:uppercase">VALUE</span>
            <span></span>
          </div>
          <div id="envRows"></div>
          <button class="btn btn-ghost btn-sm" onclick="addEnvRow('','')" style="margin-top:10px">+ Add Variable</button>
          <p class="help-text info">Restart your instance after saving for changes to take effect.</p>
        </div>
      </div>
    </div>

    <!-- SETTINGS -->
    <div class="page" id="page-settings">
      <div class="panel">
        <div class="panel-head">
          <div class="panel-title"><div class="panel-icon">⚙</div> Instance Configuration</div>
        </div>
        <div class="section-hint">Configure this instance's name, entry point file, and crash recovery behaviour.</div>
        <div class="panel-body">
          <div class="form-row-2">
            <div class="form-group">
              <label class="form-label">Instance Name</label>
              <input class="form-input" id="stName" placeholder="e.g. Discord Bot, API Server">
              <p class="help-text">Friendly label shown in the sidebar.</p>
            </div>
            <div class="form-group">
              <label class="form-label">Startup File</label>
              <input class="form-input" id="stStartup" placeholder="main.py / index.ts / main.rs">
              <p class="help-text">Relative path from the instance root.</p>
            </div>
          </div>
          <div class="form-group">
            <label class="form-label">Crash Recovery</label>
            <select class="form-select" id="stAR">
              <option value="false">Disabled — stop on crash</option>
              <option value="true">Auto-restart on non-zero exit</option>
            </select>
            <p class="help-text">Uses exponential backoff: 3s → 6s → 12s… up to 60s between retries.</p>
          </div>
          <button class="btn btn-amber" onclick="saveSettings()">💾 Save Configuration</button>
          <div class="section-divider" id="accessMgmtTitle">Access Management</div>
          <div id="accessMgmtSection">
            <div class="form-group">
              <label class="form-label">Grant Access</label>
              <div style="display:flex;gap:8px">
                <input class="form-input" id="newSubuser" placeholder="Enter exact username…">
                <button class="btn btn-orange" onclick="addSubuser()">Grant</button>
              </div>
              <p class="help-text">Shared users can start/stop and view logs, but cannot delete the instance or change access.</p>
            </div>
            <div id="subuserList"></div>
          </div>
        </div>
      </div>
      <div class="danger-zone" id="dangerZoneSection">
        <div style="font-family:var(--font-mono);font-size:9px;font-weight:700;letter-spacing:2px;
          color:var(--red);margin-bottom:6px;text-transform:uppercase">⚠ Danger Zone</div>
        <div style="font-size:12px;color:var(--text-2);margin-bottom:12px">
          Permanently destroys this instance, stops the running process, and <strong>deletes all files</strong>. Cannot be undone.</div>
        <button class="btn btn-red" onclick="deleteBot()">✕ Destroy Instance</button>
      </div>
    </div>

    <!-- RESOURCES -->
    <div class="page" id="page-resources">
      <div class="panel">
        <div class="panel-head">
          <div class="panel-title"><div class="panel-icon">▣</div> System Resources</div>
          <span class="panel-tag" style="color:var(--amber);border-color:rgba(255,154,0,0.28);background:var(--amber-dim)">Live · Every 4s</span>
        </div>
        <div class="section-hint">System-wide usage for the machine running Vortex — not per-instance.</div>
        <div class="panel-body">
          <div class="res-item">
            <div class="res-header"><span class="res-label">CPU Utilization</span><span class="res-value sv-a" id="rCpu">—</span></div>
            <div class="res-track"><div class="res-fill" id="pCpu" style="width:0%;background:linear-gradient(90deg,var(--amber),var(--orange))"></div></div>
          </div>
          <div class="res-item">
            <div class="res-header"><span class="res-label">Memory Usage</span><span class="res-value sv-b" id="rMem">—</span></div>
            <div class="res-track"><div class="res-fill" id="pMem" style="width:0%;background:linear-gradient(90deg,var(--blue),var(--amber))"></div></div>
          </div>
          <div class="res-item">
            <div class="res-header"><span class="res-label">Disk Usage</span><span class="res-value sv-a" id="rDsk">—</span></div>
            <div class="res-track"><div class="res-fill" id="pDsk" style="width:0%;background:linear-gradient(90deg,var(--orange),var(--amber))"></div></div>
            <p class="help-text" style="margin-top:7px">Root partition. Instance files live in <code style="color:var(--amber)">vortex_bots/</code>.</p>
          </div>
        </div>
      </div>
    </div>
  </main>

  <!-- MOBILE BOTTOM NAV -->
  <nav class="mobile-bottom-nav">
    <div class="m-nav-item" onclick="toggleSidebar()"><span class="m-nav-icon">▤</span><span class="m-nav-label">Bots</span></div>
    <div class="m-nav-item active" data-page="dashboard" onclick="navTo('dashboard',this)"><span class="m-nav-icon">◈</span><span class="m-nav-label">Dash</span></div>
    <div class="m-nav-item" data-page="console" onclick="navTo('console',this)"><span class="m-nav-icon">_</span><span class="m-nav-label">Console</span></div>
    <div class="m-nav-item" data-page="files" onclick="navTo('files',this)"><span class="m-nav-icon">≡</span><span class="m-nav-label">Files</span></div>
    <div class="m-nav-item" data-page="settings" onclick="navTo('settings',this)"><span class="m-nav-icon">⚙</span><span class="m-nav-label">Config</span></div>
  </nav>

  <!-- MOBILE PROCESS CONTROL BAR (above bottom nav) -->
  <div class="mobile-proc-bar" id="mobileProcBar">
    <div class="mpb-status">
      <div class="mpb-dot" id="mpbDot"></div>
      <span class="mpb-name" id="mpbName">No instance selected</span>
    </div>
    <span class="mpb-state" id="mpbState">OFFLINE</span>
    <div class="mpb-btns">
      <button class="mpb-btn start" onclick="startBot()">▶ Start</button>
      <button class="mpb-btn stop" onclick="stopBot()">■ Stop</button>
      <button class="mpb-btn restart" onclick="restartBot()">↺</button>
    </div>
  </div>
</div>

<div class="toast-tray" id="toastTray"></div>

<!-- CREATE MODAL -->
<div class="modal-veil" id="mCreate">
  <div class="modal-box">
    <div class="modal-title">DEPLOY <span class="modal-title-accent">INSTANCE</span></div>
    <div class="form-group">
      <label class="form-label">Instance Name</label>
      <input class="form-input" id="mName" placeholder="My Discord Bot"
        onkeydown="if(event.key==='Enter')createBot()">
    </div>
    <div class="form-group">
      <label class="form-label">Startup File</label>
      <input class="form-input" id="mFile" value="main.py"
        placeholder="main.py / index.ts / main.rs / Program.cs"
        onkeydown="if(event.key==='Enter')createBot()">
      <p class="help-text">The file Vortex runs when you press Start. Changeable later in Settings.</p>
    </div>
    <p class="help-text info">After creating, go to <strong>File Manager</strong> to upload your code.</p>
    <div class="modal-footer">
      <button class="btn btn-ghost" onclick="closeModal('mCreate')">Cancel</button>
      <button class="btn btn-amber" onclick="createBot()">Initialize Instance</button>
    </div>
  </div>
</div>

<!-- EDITOR MODAL -->
<div class="modal-veil" id="mEditor">
  <div class="modal-box wide">
    <div class="modal-title">EDIT <span class="modal-title-accent" id="edName">FILE</span></div>
    <textarea class="code-editor" id="edContent"></textarea>
    <div class="modal-footer">
      <button class="btn btn-ghost" onclick="closeModal('mEditor')">Discard</button>
      <button class="btn btn-amber" onclick="saveFile()">💾 Save Changes</button>
    </div>
  </div>
</div>

<!-- NEW FILE MODAL -->
<div class="modal-veil" id="mNewFile">
  <div class="modal-box">
    <div class="modal-title">CREATE <span class="modal-title-accent">FILE</span></div>
    <div class="form-group">
      <label class="form-label">Filename</label>
      <input class="form-input" id="nfName" placeholder="e.g. src/app.py or config.json">
      <p class="help-text">Use slashes for subdirectories: <code style="color:var(--amber)">cogs/admin.py</code></p>
    </div>
    <div class="form-group">
      <label class="form-label">Initial Content</label>
      <textarea class="form-textarea" id="nfContent" placeholder="# Start writing…" style="height:130px"></textarea>
    </div>
    <div class="modal-footer">
      <button class="btn btn-ghost" onclick="closeModal('mNewFile')">Cancel</button>
      <button class="btn btn-amber" onclick="createNewFile()">Create File</button>
    </div>
  </div>
</div>

<!-- ADD HOST MODAL -->
<div class="modal-veil" id="mAddHost">
  <div class="modal-box">
    <div class="modal-title">ADD <span class="modal-title-accent">HOST</span></div>
    <div class="form-group">
      <label class="form-label">Host URL</label>
      <input class="form-input" id="ahUrl" placeholder="https://my-vortex-server.com"
        onkeydown="if(event.key==='Enter')submitAddHost()">
      <p class="help-text">Full URL including protocol. Must be another machine running Vortex.</p>
    </div>
    <div class="form-group">
      <label class="form-label">Display Label</label>
      <input class="form-input" id="ahLabel" placeholder="Production / VPS-1 / Pi"
        onkeydown="if(event.key==='Enter')submitAddHost()">
    </div>
    <p class="help-text info">Right-click a host button in the toolbar to remove it.</p>
    <div class="modal-footer">
      <button class="btn btn-ghost" onclick="closeModal('mAddHost')">Cancel</button>
      <button class="btn btn-amber" onclick="submitAddHost()">Add Host</button>
    </div>
  </div>
</div>

<script>
/* ══ RUNTIME TOGGLE ═══════════════════════════════════════════════════════════ */
const RUNTIMES={
  py:{label:'Python',     ext:'py',defaultFile:'main.py',      icon:'🐍'},
  js:{label:'JavaScript', ext:'js',defaultFile:'index.js',     icon:'⚡'},
  ts:{label:'TypeScript', ext:'ts',defaultFile:'index.ts',     icon:'⟨⟩'},
  rs:{label:'Rust',       ext:'rs',defaultFile:'src/main.rs',  icon:'⚙'},
  cs:{label:'C#',         ext:'cs',defaultFile:'Program.cs',   icon:'◆'},
};
let currentRuntime='py';
function setRuntime(rt){
  if(!RUNTIMES[rt])return;
  currentRuntime=rt;
  document.querySelectorAll('.runtime-btn').forEach(b=>b.classList.remove('active'));
  const btn=document.querySelector(`.runtime-btn.rt-${rt}`);
  if(btn)btn.classList.add('active');
  const def=RUNTIMES[rt].defaultFile;
  ['sfInput','mFile','stStartup'].forEach(id=>{const el=document.getElementById(id);if(el)el.value=def;});
  toast(`Runtime: ${RUNTIMES[rt].label} · ${def}`,'info');
}
function detectRuntime(sf){
  if(!sf)return;
  const ext=sf.split('.').pop().toLowerCase();
  const map={py:'py',js:'js',ts:'ts',rs:'rs',cs:'cs',sh:'py',mjs:'js',cjs:'js'};
  const rt=map[ext];
  if(rt&&rt!==currentRuntime)setRuntime(rt);
}

/* ══ REMEMBER ME ══════════════════════════════════════════════════════════════ */
let rememberMe=false;
function toggleRememberMe(){
  rememberMe=!rememberMe;
  const el=document.getElementById('rememberCheck');
  if(el)el.classList.toggle('on',rememberMe);
}

/* ══ CLOCK ════════════════════════════════════════════════════════════════════ */
function updateClock(){
  const n=new Date();
  const pad=v=>String(v).padStart(2,'0');
  const c=document.getElementById('clock');
  if(c)c.textContent=`${pad(n.getHours())}:${pad(n.getMinutes())}:${pad(n.getSeconds())}`;
  const d=document.getElementById('clockDate');
  if(d)d.textContent=n.toLocaleDateString(undefined,{month:'short',day:'numeric',year:'numeric'});
  const tz=document.getElementById('clockTz');
  if(tz){try{const z=Intl.DateTimeFormat().resolvedOptions().timeZone;tz.textContent=z.split('/').pop().replace(/_/g,' ');}catch(e){}}
}
setInterval(updateClock,1000);updateClock();

/* ══ MULTI-HOST ════════════════════════════════════════════════════════════════ */
const HOSTS=[];let currentHostUrl='';
function addHost(url,label){
  url=url.replace(/\/+$/,'');
  if(HOSTS.find(h=>h.url===url)){toast('Host already added','info');return;}
  HOSTS.push({url,label:label||url});saveHosts();renderHostSwitcher();
}
function removeHost(url){
  const i=HOSTS.findIndex(h=>h.url===url);if(i!==-1)HOSTS.splice(i,1);
  if(currentHostUrl===url)switchHost('');saveHosts();renderHostSwitcher();
}
function saveHosts(){try{localStorage.setItem('vortex_hosts',JSON.stringify(HOSTS));}catch(e){}}
function loadHostsFromStorage(){
  try{const r=localStorage.getItem('vortex_hosts');if(r){const a=JSON.parse(r);a.forEach(h=>{if(!HOSTS.find(x=>x.url===h.url))HOSTS.push(h);});}}catch(e){}
}
function renderHostSwitcher(){
  const sw=document.getElementById('hostSwitcher');if(!sw)return;
  sw.innerHTML='';
  const local=document.createElement('button');
  local.className='host-btn'+(currentHostUrl===''?' active':'');
  local.textContent='LOCAL';local.title='This server';
  local.onclick=()=>switchHost('');sw.appendChild(local);
  HOSTS.forEach(h=>{
    const sep=document.createElement('div');sep.className='host-sep';sw.appendChild(sep);
    const btn=document.createElement('button');
    btn.className='host-btn'+(currentHostUrl===h.url?' active':'');
    btn.textContent=h.label;btn.title=h.url+'\n(right-click to remove)';
    btn.onclick=()=>switchHost(h.url);
    btn.oncontextmenu=e=>{e.preventDefault();if(confirm(`Remove host "${h.label}"?`))removeHost(h.url);};
    sw.appendChild(btn);
  });
  const sep2=document.createElement('div');sep2.className='host-sep';sw.appendChild(sep2);
  const add=document.createElement('button');add.className='host-btn';
  add.textContent='+';add.title='Add remote Vortex server';add.onclick=()=>openAddHostModal();sw.appendChild(add);
}
function switchHost(url){
  currentHostUrl=url;renderHostSwitcher();
  curBot=null;botRegistry={};startTimes={};
  document.getElementById('tbBot').textContent='— SELECT —';
  ['mainTerm','miniTerm'].forEach(i=>{const el=document.getElementById(i);if(el)el.innerHTML='';});
  applyStatus('offline');
  const bl=document.getElementById('botList');if(bl)bl.innerHTML='';
  document.getElementById('botCount').textContent='0';
  loadBots();
  toast(url?`Switched to ${HOSTS.find(h=>h.url===url)?.label||url}`:'Switched to local','info');
}

/* ══ API FETCH ════════════════════════════════════════════════════════════════ */
const _origFetch=window.fetch.bind(window);
async function apiFetch(url,opts={}){
  const fullUrl=currentHostUrl?currentHostUrl+url:url;
  try{
    const r=await _origFetch(fullUrl,opts);
    if(r.status===401){document.getElementById('loginOverlay').style.display='flex';return null;}
    return r;
  }catch(e){
    toast('Network error'+(currentHostUrl?` (${HOSTS.find(h=>h.url===currentHostUrl)?.label||currentHostUrl})`:''),'error');
    return null;
  }
}

/* ══ SOCKET ═══════════════════════════════════════════════════════════════════ */
let sock;
try{
  sock=io({transports:['websocket','polling'],reconnectionAttempts:10,reconnectionDelay:1500,timeout:10000,upgrade:true});
  sock.on('connect',()=>{console.log('[WS] connected');});
  sock.on('connect_error',err=>console.warn('[WS] connect_error',err.message));
  sock.on('console_log',({bot_id,msg,level})=>{if(bot_id===curBot)appendLog(msg,level);});
  sock.on('files_changed',({bot_id})=>{if(bot_id===curBot&&document.getElementById('page-files')?.classList.contains('active'))loadFiles();});
  sock.on('status_update',({bot_id,status,start_time})=>{
    if(botRegistry[bot_id])botRegistry[bot_id].status=status;
    renderBotList();
    if(bot_id===curBot){applyStatus(status);updateMobileProcBar();}
    if(status==='online'&&start_time){startTimes[bot_id]=start_time*1000;startUptime();}
    else delete startTimes[bot_id];
  });
}catch(e){console.error('[WS] init failed',e);sock={emit:()=>{},on:()=>{}};}

let curBot=null,botRegistry={},startTimes={},uptimeIv=null,resIv=null;
let currentUser='',authMode='login';
const _renameMap={};

/* ══ SIDEBAR TOGGLE ═══════════════════════════════════════════════════════════ */
function toggleSidebar(){
  const sb=document.getElementById('sidebar'),o=document.getElementById('sidebarOverlay');
  if(!sb||!o)return;
  const open=sb.classList.toggle('open');
  if(open){o.style.display='block';void o.offsetWidth;o.classList.add('open');document.body.style.overflow='hidden';}
  else{o.classList.remove('open');setTimeout(()=>{o.style.display='none';},300);document.body.style.overflow='';}
}

/* ══ NAVIGATION ═══════════════════════════════════════════════════════════════ */
const PAGE_NAMES={dashboard:'DASHBOARD',console:'CONSOLE',files:'FILE MANAGER',env:'ENVIRONMENT',settings:'SETTINGS',resources:'RESOURCES'};
function navTo(name,el){
  document.querySelectorAll('.sidebar .nav-item').forEach(n=>n.classList.remove('active'));
  const d=document.querySelector(`.sidebar .nav-item[data-page="${name}"]`);if(d)d.classList.add('active');
  document.querySelectorAll('.mobile-bottom-nav .m-nav-item').forEach(n=>n.classList.remove('active'));
  const mv=document.querySelector(`.mobile-bottom-nav .m-nav-item[data-page="${name}"]`);if(mv)mv.classList.add('active');
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
  const p=document.getElementById('page-'+name);if(p)p.classList.add('active');
  document.getElementById('tbPage').textContent=PAGE_NAMES[name]||name.toUpperCase();
  if(name==='files')loadFiles();
  if(name==='env')loadEnv();
  if(name==='settings')loadSettings();
  if(name==='resources')startRes();else stopRes();
  if(window.innerWidth<=860){const sb=document.getElementById('sidebar');if(sb&&sb.classList.contains('open'))toggleSidebar();}
}

/* ══ MOBILE PROC BAR ══════════════════════════════════════════════════════════ */
function updateMobileProcBar(){
  const b=curBot?botRegistry[curBot]:null;
  const dot=document.getElementById('mpbDot'),name=document.getElementById('mpbName'),state=document.getElementById('mpbState');
  if(!dot||!name||!state)return;
  if(!b){name.textContent='No instance selected';state.textContent='OFFLINE';dot.className='mpb-dot';return;}
  const on=b.status==='online';
  dot.className='mpb-dot'+(on?' online':'');
  name.textContent=b.name||curBot;
  state.textContent=on?'ONLINE':'OFFLINE';
  state.className='mpb-state'+(on?' online':'');
}

/* ══ AUTH ══════════════════════════════════════════════════════════════════════ */
async function checkAuth(){
  try{
    const r=await fetch('/api/me');
    if(r.status===401){document.getElementById('loginOverlay').style.display='flex';return false;}
    const d=await r.json();currentUser=d.username;
    document.getElementById('loginOverlay').style.display='none';return true;
  }catch(e){document.getElementById('loginOverlay').style.display='flex';return false;}
}
function switchAuthMode(mode){
  authMode=mode;
  document.getElementById('tabLogin').classList.toggle('active',mode==='login');
  document.getElementById('tabRegister').classList.toggle('active',mode==='register');
  document.getElementById('authBtn').textContent=mode==='login'?'AUTHENTICATE':'CREATE ACCOUNT';
}
async function submitAuth(){
  const u=document.getElementById('authUsername').value.trim(),p=document.getElementById('authPassword').value;
  if(!u||!p){toast('Username and password required','error');return;}
  const ep=authMode==='login'?'/api/login':'/api/register';
  try{
    const r=await fetch(ep,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:u,password:p,remember_me:rememberMe})});
    const res=await r.json();
    if(r.ok)location.reload();else toast(res.error||'Authentication failed','error');
  }catch(e){toast('Network error — could not reach server','error');}
}
async function logout(){try{await fetch('/api/logout',{method:'POST'});}catch(e){}location.reload();}

/* ══ BOT LIST ══════════════════════════════════════════════════════════════════ */
async function loadBots(){
  const r=await apiFetch('/api/bots');if(!r)return;
  botRegistry=await r.json();
  Object.entries(botRegistry).forEach(([id,b])=>{if(b.status==='online'&&b.start_time)startTimes[id]=b.start_time*1000;});
  renderBotList();
  document.getElementById('botCount').textContent=Object.keys(botRegistry).length;
  if(Object.keys(botRegistry).length>0&&!curBot)selectBot(Object.keys(botRegistry)[0]);
}
function renderBotList(){
  const el=document.getElementById('botList');if(!el)return;
  el.innerHTML='';
  const entries=Object.entries(botRegistry);
  if(!entries.length){
    el.innerHTML='<div style="padding:16px;text-align:center;color:var(--text-3);font-family:var(--font-mono);font-size:9px;letter-spacing:2px;line-height:1.9">NO INSTANCES YET<br><span style="font-size:8px;opacity:.6">Click "+ Deploy" above</span></div>';
    return;
  }
  entries.forEach(([id,b])=>{
    const d=document.createElement('div');
    d.className='bot-item'+(id===curBot?' active':'');
    const s=b.status||'offline';
    const sh=b.is_shared?'<span class="bot-shared-tag">shared</span>':'';
    d.innerHTML=`<div class="bot-dot ${s}"></div><div style="flex:1;min-width:0"><div class="bot-name">${escH(b.name||id)}</div><div class="bot-status ${s}">${s}</div></div>${sh}`;
    d.onclick=()=>{selectBot(id);if(window.innerWidth<=860&&document.getElementById('sidebar')?.classList.contains('open'))toggleSidebar();};
    el.appendChild(d);
  });
}
function selectBot(id){
  curBot=id;
  const b=botRegistry[id];
  document.getElementById('tbBot').textContent=b?.name||id;
  const sf=b?.startup_file||'main.py';
  document.getElementById('sfInput').value=sf;
  document.getElementById('termTitle').textContent=(b?.name||id).toUpperCase()+' // STDOUT';
  ['mainTerm','miniTerm'].forEach(i=>{const el=document.getElementById(i);if(el)el.innerHTML='';});
  applyStatus(b?.status||'offline');
  updateMobileProcBar();
  renderBotList();
  loadBotLogs();
  startUptime();
  detectRuntime(sf);
  const ap=document.querySelector('.page.active');
  if(ap){const pg=ap.id.replace('page-','');if(pg==='files')loadFiles();if(pg==='env')loadEnv();if(pg==='settings')loadSettings();}
  const shared=b&&b.is_shared;
  document.getElementById('accessMgmtTitle').style.display=shared?'none':'';
  document.getElementById('accessMgmtSection').style.display=shared?'none':'';
  document.getElementById('dangerZoneSection').style.display=shared?'none':'';
}
async function loadBotLogs(){
  if(!curBot)return;
  const r=await apiFetch(`/api/bot/${curBot}/logs`);if(!r)return;
  const logs=await r.json();
  ['mainTerm','miniTerm'].forEach(id=>{const el=document.getElementById(id);if(el)el.innerHTML='';});
  logs.forEach(({msg,level,time:ts})=>appendLog(msg,level,ts));
}
function applyStatus(s){
  const on=s==='online';
  document.getElementById('statusTag').className='status-badge '+(on?'online':'offline');
  document.getElementById('statusText').textContent=on?'ONLINE':'OFFLINE';
  document.getElementById('sStat').textContent=on?'ONLINE':'OFFLINE';
  document.getElementById('sStat').className='stat-value '+(on?'sv-g':'sv-r');
  document.getElementById('sStatSub').textContent=on?'Process running normally':'Process not running';
  if(!on)document.getElementById('sUptime').textContent='—';
}

/* ══ MODALS ════════════════════════════════════════════════════════════════════ */
function openCreateModal(){
  document.getElementById('mFile').value=RUNTIMES[currentRuntime]?.defaultFile||'main.py';
  document.getElementById('mCreate').classList.add('open');
  setTimeout(()=>document.getElementById('mName')?.focus(),80);
}
function closeModal(id){const el=document.getElementById(id);if(el)el.classList.remove('open');}
document.querySelectorAll('.modal-veil').forEach(m=>m.addEventListener('click',e=>{if(e.target===m)m.classList.remove('open');}));

/* ══ BOT ACTIONS ═══════════════════════════════════════════════════════════════ */
async function createBot(){
  const n=document.getElementById('mName').value.trim();
  const f=document.getElementById('mFile').value.trim()||RUNTIMES[currentRuntime]?.defaultFile||'main.py';
  if(!n){toast('Instance name required','error');return;}
  const r=await apiFetch('/api/bots',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:n,startup_file:f})});
  if(!r)return;
  if(!r.ok){const e=await r.json();toast(e.error||'Failed to create','error');return;}
  const b=await r.json();botRegistry[b.id]=b;closeModal('mCreate');document.getElementById('mName').value='';
  renderBotList();document.getElementById('botCount').textContent=Object.keys(botRegistry).length;
  selectBot(b.id);toast(`"${n}" deployed — upload your files next`,'success');
}
async function startBot(){
  if(!curBot){toast('Select an instance first','error');return;}
  const sf=document.getElementById('sfInput').value.trim()||RUNTIMES[currentRuntime]?.defaultFile||'main.py';
  const r=await apiFetch(`/api/bot/${curBot}/start`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({startup_file:sf})});
  if(r&&!r.ok){const e=await r.json();toast(e.error||'Start failed','error');return;}
  toast('Booting process…','info');
}
async function stopBot(){
  if(!curBot){toast('Select an instance first','error');return;}
  await apiFetch(`/api/bot/${curBot}/stop`,{method:'POST'});toast('Process stopped','success');
}
async function restartBot(){
  if(!curBot){toast('Select an instance first','error');return;}
  const r=await apiFetch(`/api/bot/${curBot}/stop`,{method:'POST'});
  if(r){toast('Restarting…','info');setTimeout(startBot,1200);}
}
async function killBot(){
  if(!curBot)return;
  if(!confirm('Force kill this process? Use only if Stop is unresponsive.'))return;
  await apiFetch(`/api/bot/${curBot}/kill`,{method:'POST'});toast('Force killed','error');
}
async function deleteBot(){
  if(!curBot||!confirm('Permanently destroy this instance and ALL its files? Cannot be undone.'))return;
  const r=await apiFetch(`/api/bot/${curBot}`,{method:'DELETE'});
  if(!r||!r.ok){toast('Delete failed','error');return;}
  delete botRegistry[curBot];curBot=null;
  document.getElementById('tbBot').textContent='— SELECT —';
  ['mainTerm','miniTerm'].forEach(i=>{const el=document.getElementById(i);if(el)el.innerHTML='';});
  applyStatus('offline');renderBotList();updateMobileProcBar();
  document.getElementById('botCount').textContent=Object.keys(botRegistry).length;
  toast('Instance destroyed','error');
}

/* ══ CONSOLE ═══════════════════════════════════════════════════════════════════ */
function appendLog(msg,level,ts){
  const tagMap={system:'sys',error:'err',success:'ok',warn:'warn',default:'out',stdin:'in'};
  const tag=tagMap[level]||'out',t=ts||new Date().toTimeString().slice(0,8);
  const row=`<div class="log-row"><span class="log-ts">${escH(t)}</span><span class="log-tag ${tag}">${tag}</span><span class="log-msg ${tag}">${escH(msg)}</span></div>`;
  ['mainTerm','miniTerm'].forEach(id=>{const el=document.getElementById(id);if(!el)return;el.innerHTML+=row;el.scrollTop=el.scrollHeight;});
}
function clearConsole(){['mainTerm','miniTerm'].forEach(id=>{const el=document.getElementById(id);if(el)el.innerHTML='';});toast('Console cleared','info');}
function exportLogs(){
  const el=document.getElementById('mainTerm');if(!el)return;
  const lines=Array.from(el.querySelectorAll('.log-row')).map(r=>r.textContent.trim()).join('\n');
  const a=document.createElement('a');a.href='data:text/plain;charset=utf-8,'+encodeURIComponent(lines);
  a.download=`${curBot||'vortex'}-${Date.now()}.log`;a.click();toast('Logs exported','success');
}
async function sendInput(){
  if(!curBot)return;
  const inp=document.getElementById('termIn');if(!inp)return;
  let v=inp.value;inp.value='';
  v=v.replace(/[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f]/g,'');
  if(v===''&&v!=='0')return;
  await apiFetch(`/api/bot/${curBot}/input`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({input:v})});
}

/* ══ FILE MANAGER ══════════════════════════════════════════════════════════════ */
const EXT_COLORS={py:'#39FF8A',js:'#FFD060',json:'#4DA6FF',md:'#FF9A00',txt:'#7A6645',sh:'#4DA6FF',zip:'#FF3A3A',env:'#FFD060',ts:'#4DA6FF',html:'#FF6B35',css:'#4FFFB0',jsx:'#4DA6FF',tsx:'#4DA6FF',rs:'#FF5C00',cs:'#C090FF',toml:'#FFD060',csproj:'#C090FF',lock:'#7A6645'};
const EXT_ICONS={py:'🐍',js:'⚡',jsx:'⚛',tsx:'⚛',ts:'⟨⟩',json:'{}',txt:'≡',md:'#',zip:'⊞',env:'⊛',sh:'$',html:'<>',css:'◐',rs:'⚙',cs:'◆',toml:'⚙',csproj:'◆'};

async function loadFiles(){
  const tb=document.getElementById('fileList');
  if(!curBot){if(tb)tb.innerHTML=`<tr><td colspan="5" style="padding:32px;text-align:center;color:var(--text-3);font-family:var(--font-mono);font-size:10px">SELECT AN INSTANCE FIRST</td></tr>`;return;}
  if(tb)tb.innerHTML=`<tr><td colspan="5" style="padding:32px;text-align:center;color:var(--text-3);font-family:var(--font-mono);font-size:10px">Loading…</td></tr>`;
  const r=await apiFetch(`/api/bot/${curBot}/files`);if(!r)return;
  const files=await r.json();
  if(!files.length){if(tb)tb.innerHTML=`<tr><td colspan="5" style="padding:32px;text-align:center;color:var(--text-3);font-family:var(--font-mono);font-size:10px">NO FILES YET — UPLOAD SOME</td></tr>`;return;}
  for(const k in _renameMap)delete _renameMap[k];
  if(tb)tb.innerHTML='';
  files.forEach(f=>{
    const ext=(f.name.split('.').pop()||'').toLowerCase();
    const c=EXT_COLORS[ext]||'#7A6645',ic=EXT_ICONS[ext]||'□';
    const rid='r'+Math.random().toString(36).slice(2,10);
    _renameMap[rid]=f.name;
    const tr=document.createElement('tr');
    const tdName=document.createElement('td');
    const fnCell=document.createElement('div');fnCell.className='fn-cell';
    const fnIcon=document.createElement('span');fnIcon.className='fn-icon';fnIcon.textContent=ic;
    const fnLink=document.createElement('span');fnLink.className='fn-link';fnLink.id='fnl_'+rid;
    const dn=f.display||f.name.split('/').pop();
    fnLink.title=f.name;fnLink.textContent=dn;fnLink.onclick=()=>editFile(f.name);
    const fnRename=document.createElement('div');fnRename.className='fn-rename';fnRename.id='fnr_'+rid;
    const fnInput=document.createElement('input');fnInput.className='fn-rename-input';fnInput.id='fni_'+rid;fnInput.value=dn;
    fnInput.onkeydown=e=>{if(e.key==='Enter')doRename(rid);if(e.key==='Escape')cancelRename(rid);};
    const fnOk=document.createElement('button');fnOk.className='fn-rename-ok';fnOk.textContent='✓';fnOk.onclick=()=>doRename(rid);
    const fnCancel=document.createElement('button');fnCancel.className='fn-rename-cancel';fnCancel.textContent='✕';fnCancel.onclick=()=>cancelRename(rid);
    fnRename.append(fnInput,fnOk,fnCancel);fnCell.append(fnIcon,fnLink,fnRename);tdName.appendChild(fnCell);
    const tdType=document.createElement('td');
    const badge=document.createElement('span');badge.className='file-ext-badge';badge.style.cssText=`color:${c};border-color:${c}33`;badge.textContent='.'+(ext||'—');tdType.appendChild(badge);
    const tdSize=document.createElement('td');tdSize.style.color='var(--text-2)';tdSize.textContent=f.size;
    const tdMod=document.createElement('td');tdMod.style.cssText='color:var(--text-3);font-size:10px';tdMod.textContent=f.modified;
    const tdAct=document.createElement('td');
    const actDiv=document.createElement('div');actDiv.className='file-actions';
    const be=document.createElement('button');be.className='icon-btn ib-cyan';be.title='Edit';be.textContent='✏';be.onclick=()=>editFile(f.name);
    const br=document.createElement('button');br.className='icon-btn ib-amber';br.id='rnb_'+rid;br.title='Rename';br.textContent='⟳';br.onclick=()=>toggleRename(rid);
    const bd=document.createElement('button');bd.className='icon-btn';bd.title='Download';bd.textContent='↓';bd.onclick=()=>dlFile(f.name);
    const bx=document.createElement('button');bx.className='icon-btn ib-red';bx.title='Delete';bx.textContent='✕';bx.onclick=()=>delFile(f.name);
    actDiv.append(be,br,bd,bx);tdAct.appendChild(actDiv);
    tr.append(tdName,tdType,tdSize,tdMod,tdAct);if(tb)tb.appendChild(tr);
  });
}
function toggleRename(rid){
  const lnk=document.getElementById('fnl_'+rid),rnw=document.getElementById('fnr_'+rid),btn=document.getElementById('rnb_'+rid);
  if(!lnk||!rnw||!btn)return;
  if(rnw.classList.contains('on')){cancelRename(rid);}else{
    lnk.style.display='none';rnw.classList.add('on');btn.textContent='✕';
    const inp=document.getElementById('fni_'+rid);if(inp){inp.focus();const v=inp.value,dot=v.lastIndexOf('.');inp.setSelectionRange(0,dot>0?dot:v.length);}
  }
}
function cancelRename(rid){
  const lnk=document.getElementById('fnl_'+rid),rnw=document.getElementById('fnr_'+rid),btn=document.getElementById('rnb_'+rid);
  if(lnk)lnk.style.display='';if(rnw)rnw.classList.remove('on');if(btn){btn.textContent='⟳';btn.title='Rename';}
}
async function doRename(rid){
  const old=_renameMap[rid];if(!old){toast('Rename context lost — refresh','error');return;}
  const inp=document.getElementById('fni_'+rid);if(!inp)return;
  const nb=inp.value.trim();if(!nb){toast('Filename cannot be empty','error');return;}
  const parts=old.split('/');parts[parts.length-1]=nb;
  const nn=parts.join('/');if(nn===old){cancelRename(rid);return;}
  const r=await apiFetch(`/api/bot/${curBot}/file/${encodeURIComponent(old)}/rename`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({new_name:nn})});
  if(!r)return;const res=await r.json();if(res.error){toast(res.error,'error');return;}
  toast(`Renamed → ${nb}`,'success');loadFiles();
}
async function editFile(name){
  if(!curBot)return;
  const r=await apiFetch(`/api/bot/${curBot}/file/${encodeURIComponent(name)}`);if(!r)return;
  const d=await r.json();
  document.getElementById('edName').textContent=name;
  document.getElementById('edContent').value=d.content==='[Binary — cannot display]'?'':d.content;
  document.getElementById('edContent').dataset.fn=name;
  if(d.content==='[Binary — cannot display]')toast('Binary file — editor shows empty','info');
  document.getElementById('mEditor').classList.add('open');
}
async function saveFile(){
  const name=document.getElementById('edContent').dataset.fn;if(!name)return;
  const r=await apiFetch(`/api/bot/${curBot}/file/${encodeURIComponent(name)}`,{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({content:document.getElementById('edContent').value})});
  if(!r||!r.ok){toast('Save failed','error');return;}
  closeModal('mEditor');loadFiles();toast(`${name} saved`,'success');
}
function openNewFileModal(){if(!curBot){toast('Select an instance first','error');return;}document.getElementById('mNewFile').classList.add('open');setTimeout(()=>document.getElementById('nfName')?.focus(),80);}
async function createNewFile(){
  const name=document.getElementById('nfName').value.trim();if(!name){toast('Filename required','error');return;}
  const r=await apiFetch(`/api/bot/${curBot}/file/${encodeURIComponent(name)}`,{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({content:document.getElementById('nfContent').value})});
  if(!r||!r.ok){toast('Create failed','error');return;}
  closeModal('mNewFile');document.getElementById('nfName').value='';document.getElementById('nfContent').value='';
  loadFiles();toast('File created','success');
}
async function delFile(name){
  if(!confirm(`Delete "${name}"? Cannot be undone.`))return;
  const r=await apiFetch(`/api/bot/${curBot}/file/${encodeURIComponent(name)}`,{method:'DELETE'});
  if(!r||!r.ok){toast('Delete failed','error');return;}
  loadFiles();toast('File deleted','success');
}
function dlFile(name){window.location.href=(currentHostUrl||'')+`/api/bot/${curBot}/file/${encodeURIComponent(name)}/download`;}

/* ══ DRAG & DROP ═══════════════════════════════════════════════════════════════ */
async function handleUpload(files,isFolder){
  const fileArr=files?Array.from(files):[];
  try{document.getElementById('fileUploadInput').value='';}catch(e){}
  try{document.getElementById('folderUploadInput').value='';}catch(e){}
  if(!curBot){toast('Select an instance first','error');return;}
  if(!fileArr.length){toast('No files selected','error');return;}
  const prog=document.getElementById('uploadProgress');
  let ok=0,fail=0;
  for(const file of fileArr){
    const relPath=(isFolder&&file.webkitRelativePath)?file.webkitRelativePath:file.name;
    const fd=new FormData();fd.append('file',file);fd.append('relative_path',relPath);fd.append('is_folder_upload',isFolder?'1':'0');
    const sid='up_'+Math.random().toString(36).slice(2);
    const wrap=document.createElement('div');wrap.className='upload-row';
    const sn=relPath.length>50?'…'+relPath.slice(-47):relPath;
    wrap.innerHTML=`<span style="color:var(--amber);flex-shrink:0">⇪</span><span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:10px">${escH(sn)}</span><div class="upload-bar-wrap"><div class="upload-bar-fill" id="${sid}" style="width:0%"></div></div><span id="${sid}st" style="font-size:9px;color:var(--text-3);flex-shrink:0;min-width:24px;text-align:right">0%</span>`;
    if(prog)prog.appendChild(wrap);
    try{
      await new Promise((res,rej)=>{
        const xhr=new XMLHttpRequest();
        xhr.open('POST',(currentHostUrl||'')+`/api/bot/${curBot}/upload`);
        xhr.upload.addEventListener('progress',e=>{if(e.lengthComputable){const pct=Math.round(e.loaded/e.total*95);const b=document.getElementById(sid),s=document.getElementById(sid+'st');if(b)b.style.width=pct+'%';if(s)s.textContent=pct+'%';}});
        xhr.addEventListener('load',()=>{if(xhr.status===401){document.getElementById('loginOverlay').style.display='flex';rej(new Error('Unauthorized'));return;}let resp={};try{resp=JSON.parse(xhr.responseText);}catch(e){}if(resp.error){rej(new Error(resp.error));return;}if(xhr.status>=200&&xhr.status<300)res();else rej(new Error(`HTTP ${xhr.status}`));});
        xhr.addEventListener('error',()=>rej(new Error('Network error')));
        xhr.addEventListener('abort',()=>rej(new Error('Aborted')));
        xhr.send(fd);
      });
      const b=document.getElementById(sid),s=document.getElementById(sid+'st');
      if(b){b.style.width='100%';b.style.background='var(--green)';}if(s){s.textContent='✓';s.style.color='var(--green)';}
      ok++;setTimeout(()=>wrap.remove(),2000);
    }catch(err){
      const b=document.getElementById(sid),s=document.getElementById(sid+'st');
      if(b){b.style.width='100%';b.style.background='var(--red)';}if(s){s.textContent='✕';s.style.color='var(--red)';}
      wrap.style.borderColor='rgba(255,58,58,0.28)';fail++;
      toast(`Upload failed: ${err.message}`,'error');setTimeout(()=>wrap.remove(),4000);
    }
  }
  loadFiles();
  if(ok>0&&fail===0)toast(`${ok} file${ok>1?'s':''} uploaded`,'success');
  else if(ok>0&&fail>0)toast(`${ok} uploaded, ${fail} failed`,'info');
}
let _dzDepth=0;
document.addEventListener('dragenter',e=>{e.preventDefault();_dzDepth++;document.getElementById('dropZone')?.classList.add('dragging');});
document.addEventListener('dragleave',e=>{if(--_dzDepth<=0){_dzDepth=0;document.getElementById('dropZone')?.classList.remove('dragging');}});
document.addEventListener('dragover',e=>e.preventDefault());
document.addEventListener('drop',e=>{e.preventDefault();_dzDepth=0;document.getElementById('dropZone')?.classList.remove('dragging');if(e.dataTransfer.files.length)handleUpload(e.dataTransfer.files,false);});

/* ══ ENV ════════════════════════════════════════════════════════════════════════ */
async function loadEnv(){
  if(!curBot)return;const r=await apiFetch(`/api/bot/${curBot}/env`);if(!r)return;
  const env=await r.json();const c=document.getElementById('envRows');if(!c)return;c.innerHTML='';
  const e=Object.entries(env);if(e.length)e.forEach(([k,v])=>addEnvRow(k,v));else addEnvRow('','');
}
function addEnvRow(k='',v=''){
  const d=document.createElement('div');d.className='env-row';
  const ki=document.createElement('input');ki.className='env-field env-key';ki.placeholder='VARIABLE_NAME';ki.value=k;
  const vi=document.createElement('input');vi.className='env-field';vi.placeholder='value';vi.value=v;
  const db=document.createElement('button');db.className='icon-btn ib-red';db.textContent='✕';db.style.cssText='width:26px;height:26px';db.onclick=()=>d.remove();
  d.append(ki,vi,db);document.getElementById('envRows').appendChild(d);
}
async function saveEnv(){
  if(!curBot)return;const env={};
  document.querySelectorAll('.env-row').forEach(r=>{const inputs=r.querySelectorAll('.env-field');const k=inputs[0]?.value.trim(),v=inputs[1]?.value;if(k)env[k]=v||'';});
  const r=await apiFetch(`/api/bot/${curBot}/env`,{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(env)});
  if(!r||!r.ok){toast('Save failed','error');return;}toast('Environment variables saved','success');
}

/* ══ SETTINGS ══════════════════════════════════════════════════════════════════ */
async function loadSettings(){
  if(!curBot)return;const b=botRegistry[curBot]||{};
  document.getElementById('stName').value=b.name||'';
  document.getElementById('stStartup').value=b.startup_file||RUNTIMES[currentRuntime]?.defaultFile||'main.py';
  document.getElementById('stAR').value=b.auto_restart?'true':'false';
  if(!b.is_shared){
    const r=await apiFetch(`/api/bot/${curBot}/subusers`);
    if(r){
      const users=await r.json();const c=document.getElementById('subuserList');if(!c)return;
      c.innerHTML='';
      if(!users.length){c.innerHTML='<div style="font-family:var(--font-mono);font-size:9px;color:var(--text-3);padding:7px 0">No shared users yet.</div>';return;}
      users.forEach(u=>{
        const div=document.createElement('div');div.className='subuser-row';
        const ns=document.createElement('span');ns.style.color='var(--text-2)';ns.textContent=u;
        const db=document.createElement('button');db.className='icon-btn ib-red';db.style.cssText='width:26px;height:26px;font-size:10px';db.textContent='✕';db.onclick=()=>removeSubuser(u);
        div.append(ns,db);c.appendChild(div);
      });
    }
  }
}
async function saveSettings(){
  if(!curBot)return;
  const data={name:document.getElementById('stName').value.trim(),startup_file:document.getElementById('stStartup').value.trim()||RUNTIMES[currentRuntime]?.defaultFile||'main.py',auto_restart:document.getElementById('stAR').value==='true'};
  const r=await apiFetch(`/api/bot/${curBot}/settings`,{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});
  if(!r){return;}if(!r.ok){const e=await r.json();toast(e.error||'Save failed','error');return;}
  const upd=await r.json();botRegistry[curBot]={...botRegistry[curBot],...upd};
  document.getElementById('tbBot').textContent=data.name||curBot;
  document.getElementById('sfInput').value=data.startup_file;
  renderBotList();toast('Configuration saved','success');
}
async function addSubuser(){
  if(!curBot)return;const u=document.getElementById('newSubuser').value.trim();if(!u)return;
  const r=await apiFetch(`/api/bot/${curBot}/subusers`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:u})});
  if(r&&r.ok){document.getElementById('newSubuser').value='';loadSettings();toast(`Access granted to ${u}`,'success');}
  else toast('User not found — they must register first','error');
}
async function removeSubuser(u){
  if(!curBot)return;
  const r=await apiFetch(`/api/bot/${curBot}/subusers/${encodeURIComponent(u)}`,{method:'DELETE'});
  if(r&&r.ok){loadSettings();toast(`Access revoked for ${u}`,'success');}
}

/* ══ UPTIME ════════════════════════════════════════════════════════════════════ */
function startUptime(){
  clearInterval(uptimeIv);
  uptimeIv=setInterval(()=>{
    if(curBot&&startTimes[curBot]){
      const s=Math.floor((Date.now()-startTimes[curBot])/1000);
      const h=Math.floor(s/3600),m=Math.floor(s%3600/60),sec=s%60;
      const el=document.getElementById('sUptime');
      const pad=v=>String(v).padStart(2,'0');
      if(el)el.textContent=`${pad(h)}:${pad(m)}:${pad(sec)}`;
    }
  },1000);
}

/* ══ RESOURCES ═════════════════════════════════════════════════════════════════ */
function startRes(){stopRes();fetchRes();resIv=setInterval(fetchRes,4000);}
function stopRes(){clearInterval(resIv);}
async function fetchRes(){
  try{
    const r=await fetch('/api/resources');if(!r.ok)return;const d=await r.json();
    const set=(id,v)=>{const el=document.getElementById(id);if(el)el.textContent=v;};
    const sw=(id,v)=>{const el=document.getElementById(id);if(el)el.style.width=v;};
    set('rCpu',d.cpu+'%');sw('pCpu',d.cpu+'%');
    set('rMem',d.mem_used);sw('pMem',d.mem_pct+'%');
    set('rDsk',d.disk_pct+'%');sw('pDsk',d.disk_pct+'%');
    set('sCpu',d.cpu+'%');set('sMem',d.mem_used);
  }catch(e){}
}

/* ══ UTILS ══════════════════════════════════════════════════════════════════════ */
function escH(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');}
function toast(msg,type='success'){
  const tray=document.getElementById('toastTray');if(!tray)return;
  const icons={success:'✓',error:'✕',info:'i'};
  const t=document.createElement('div');t.className=`toast ${type}`;
  t.innerHTML=`<div class="toast-icon">${icons[type]||'·'}</div><span>${escH(msg)}</span><span class="toast-close" onclick="this.parentElement.remove()">✕</span>`;
  tray.appendChild(t);
  setTimeout(()=>{t.style.transition='all .3s';t.style.opacity='0';t.style.transform='translateX(14px)';setTimeout(()=>t.remove(),300);},3500);
}

/* ══ ADD HOST MODAL ════════════════════════════════════════════════════════════ */
function openAddHostModal(){
  document.getElementById('ahUrl').value='';document.getElementById('ahLabel').value='';
  document.getElementById('mAddHost').classList.add('open');setTimeout(()=>document.getElementById('ahUrl')?.focus(),80);
}
function submitAddHost(){
  let url=document.getElementById('ahUrl').value.trim(),lbl=document.getElementById('ahLabel').value.trim();
  if(!url){toast('Host URL required','error');return;}
  if(!/^https?:\/\//.test(url))url='http://'+url;
  addHost(url,lbl||url.replace(/^https?:\/\//,''));closeModal('mAddHost');toast('Remote host added','success');
}

/* ══ BOOT ═══════════════════════════════════════════════════════════════════════ */
checkAuth().then(ok=>{
  if(ok){
    loadHostsFromStorage();renderHostSwitcher();loadBots();fetchRes();setInterval(fetchRes,5000);
    if(sock&&sock.connected){sock.emit('join',{});}else if(sock){sock.on('connect',()=>sock.emit('join',{}));}
  }
});
</script>
</body>
</html>"""


# ─── routes ───────────────────────────────────────────────────────────────────

@socketio.on('connect')
def handle_connect():
    user = session.get('username')
    if user:
        join_room(user)
        log.info(f'WS connected: {user}')

@socketio.on('join')
def handle_join(data=None):
    """Explicit join room event - mobile sometimes misses auto-join on connect."""
    user = session.get('username')
    if user:
        join_room(user)

@app.route('/')
def index():
    return render_template_string(HTML)

# ── auth ──────────────────────────────────────────────────────────────────────

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
    # Upgrade plaintext password to hash on first login
    if stored == password:
        users[username]['pwd'] = _hash_pw(password)
        save_users(users)
    session['username'] = username
    session.permanent = remember_me  # sets 30-day cookie lifetime when True
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

# ── bots ──────────────────────────────────────────────────────────────────────

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
    bid = f"bot_{int(time.time() * 1000)}"
    cfg[bid] = {
        'name': name, 'startup_file': startup_file,
        'auto_restart': False, 'env': {},
        'owner': user, 'shared_with': [],
    }
    save_config(cfg)
    get_bot_dir(bid)
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

# ── subusers ──────────────────────────────────────────────────────────────────

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

# ── process control ───────────────────────────────────────────────────────────

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
    inp = (request.json or {}).get('input', '')
    if len(inp) > 4096:
        return jsonify({'error': 'Input too long'}), 400
    if bid in bots and bots[bid].get('process'):
        p = bots[bid]['process']
        if p.poll() is None:
            try:
                if not inp.endswith('\n'):
                    inp += '\n'
                data = inp.encode('utf-8', errors='replace')
                stdin_fd = bots[bid].get('stdin_fd')
                if stdin_fd is not None:
                    os.write(stdin_fd, data)
                elif p.stdin:
                    p.stdin.write(inp)
                    p.stdin.flush()
                emit_log(bid, f'> {inp.rstrip()}', 'stdin')
            except OSError as e:
                emit_log(bid, f'[Error] stdin write failed: {e}', 'error')
            except Exception as e:
                emit_log(bid, f'[Error] stdin error: {e}', 'error')
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

# ── files ─────────────────────────────────────────────────────────────────────

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
                sz = 0
                mtime = '—'
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
        return jsonify({'content': '[Binary — cannot display]'})

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
    if not src:
        return jsonify({'error': 'invalid source path'}), 403
    if not dst:
        return jsonify({'error': 'invalid destination path'}), 403
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
        return jsonify({'error': 'no file field in request'}), 400
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
        return jsonify({'error': 'invalid filename after sanitisation'}), 400

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
            log.exception('ZIP save failed')
            return jsonify({'error': f'Save failed: {e}'}), 500

        extracted, blocked = 0, 0
        try:
            with zipfile.ZipFile(tmp_zip, 'r') as zf:
                for info in zf.infolist():
                    if info.flag_bits & 0x1:
                        emit_log(bid, '[Error] ZIP is password-protected', 'error')
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
                        log.warning(f'Blocked zip-slip: {member.filename}')
                        blocked += 1
                        continue

                    dest_file_dir = os.path.dirname(dest_file)
                    if dest_file_dir:
                        os.makedirs(dest_file_dir, exist_ok=True)

                    with zf.open(member) as src_f, open(dest_file, 'wb') as dst_f:
                        dst_f.write(src_f.read())

                    extracted += 1

            os.remove(tmp_zip)
            msg = f'[System] Extracted {extracted} file(s) from {fname}'
            if blocked:
                msg += f' ({blocked} path(s) blocked)'
            emit_log(bid, msg, 'success' if extracted else 'warn')
            cfg2 = load_config().get(bid, {})
            _listeners = list({u for u in [cfg2.get('owner')] + cfg2.get('shared_with', []) if u})
            for _u in _listeners:
                with contextlib.suppress(Exception):
                    socketio.emit('files_changed', {'bot_id': bid}, room=_u)

        except zipfile.BadZipFile:
            emit_log(bid, f'[Error] {fname} is not a valid ZIP', 'error')
            if os.path.exists(tmp_zip):
                os.remove(tmp_zip)
            return jsonify({'error': 'bad zip'}), 400
        except Exception as e:
            emit_log(bid, f'[Error] ZIP extract failed: {e}', 'error')
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
            _ul = list({u for u in [cfg3.get('owner')] + cfg3.get('shared_with', []) if u})
            for _u in _ul:
                with contextlib.suppress(Exception):
                    socketio.emit('files_changed', {'bot_id': bid}, room=_u)
        except Exception as e:
            log.exception('Upload save failed')
            return jsonify({'error': f'Save failed: {e}'}), 500

    return jsonify({'ok': True, 'filename': rel_path})

# ── env & settings ────────────────────────────────────────────────────────────

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
    bc['name']         = name
    bc['startup_file'] = startup_file
    bc['auto_restart'] = bool(data.get('auto_restart', False))
    save_config(cfg)
    if bid in bots:
        bots[bid]['auto_restart'] = bc['auto_restart']
    return jsonify(bc)

# ── system resources ──────────────────────────────────────────────────────────

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
        time.sleep(3)

threading.Thread(target=_update_cpu, daemon=True).start()

@app.route('/api/resources')
def resources():
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


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    print('\n' + '━' * 60)
    print(f'  VORTEX HOSTING v12.0  ·  mode={_ASYNC_MODE}  ·  port={port}')
    print(f'  Data: {CONFIG_FILE}')
    print(f'  Users: {USERS_FILE}')
    print('━' * 60 + '\n')
    if _ASYNC_MODE == 'eventlet':
        socketio.run(app, host='0.0.0.0', port=port, debug=False)
    else:
        socketio.run(app, host='0.0.0.0', port=port, debug=False, allow_unsafe_werkzeug=True)
