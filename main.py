"""
Vortex Hosting v11.5 — Runtime Toggle + Fixed ZIP Extraction
Install: pip install flask flask-socketio psutil werkzeug eventlet
Run:     python main.py
Supported runtimes: Python, Node.js, TypeScript (ts-node), Bash, Rust (cargo), C# (dotnet)
"""
try:
    import eventlet
    eventlet.monkey_patch()
    _ASYNC_MODE = 'eventlet'
except ImportError:
    _ASYNC_MODE = 'threading'

import contextlib, hashlib, json, logging, os, re, shutil, subprocess, sys
import threading, time, zipfile, psutil
from logging import Formatter, StreamHandler, getLogger
from flask import Flask, jsonify, render_template_string, request, send_file, session
from flask_socketio import SocketIO, join_room
from werkzeug.utils import secure_filename

log = getLogger('vortexhost')
log.setLevel(logging.INFO)
_h = StreamHandler()
_h.setFormatter(Formatter('%(asctime)s %(levelname)s %(message)s'))
log.addHandler(_h)

app = Flask(__name__)
app.secret_key = os.environ.get('VORTEX_SECRET_KEY', os.urandom(32).hex())

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

# ─── helpers ──────────────────────────────────────────────────────────────────

def _hash_pw(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()

def load_users():
    with _users_lock:
        if os.path.exists(USERS_FILE):
            try:
                with open(USERS_FILE) as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

def save_users(u):
    with _users_lock:
        tmp = USERS_FILE + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(u, f, indent=2)
        os.replace(tmp, USERS_FILE)

def load_config():
    with _config_lock:
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE) as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

def save_config(cfg):
    with _config_lock:
        tmp = CONFIG_FILE + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(cfg, f, indent=2)
        os.replace(tmp, CONFIG_FILE)

def get_bot_dir(bot_id):
    p = os.path.join(BOTS_DIR, bot_id)
    os.makedirs(p, exist_ok=True)
    return p

def safe_path(bot_id, fn):
    """Return absolute path only if it stays inside the bot directory."""
    bd = os.path.abspath(get_bot_dir(bot_id))
    clean_fn = os.path.normpath('/' + fn.replace('\\', '/')).lstrip('/')
    if not clean_fn:
        return None
    fp = os.path.abspath(os.path.join(bd, clean_fn))
    if fp == bd or fp.startswith(bd + os.sep):
        return fp
    return None

SUPPORTED_EXTENSIONS = ('.py', '.js', '.ts', '.sh', '.rs', '.cs')

def safe_startup_file(startup_file: str) -> str | None:
    """Validate startup_file is a simple relative path with no traversal."""
    sf = startup_file.strip().replace('\\', '/')
    if not sf or '..' in sf or sf.startswith('/'):
        return None
    if re.search(r'[;&|`$<>!]', sf):
        return None
    if not sf.endswith(SUPPORTED_EXTENSIONS):
        return None
    return sf

def check_owner(bot_id):
    user = session.get('username')
    return bool(user and load_config().get(bot_id, {}).get('owner') == user)

def check_access(bot_id):
    user = session.get('username')
    if not user:
        return False
    cfg = load_config().get(bot_id, {})
    return cfg.get('owner') == user or user in cfg.get('shared_with', [])

def emit_log(bot_id, msg, level='default'):
    cfg = load_config().get(bot_id, {})
    listeners = [cfg.get('owner')] + cfg.get('shared_with', [])
    for u in set(listeners):
        if u:
            with contextlib.suppress(Exception):
                socketio.emit('console_log',
                    {'bot_id': bot_id, 'msg': msg, 'level': level}, room=u)
    entry = {'msg': msg, 'level': level, 'time': time.strftime('%H:%M:%S')}
    bots.setdefault(bot_id, {}).setdefault('logs', []).append(entry)
    if len(bots[bot_id]['logs']) > 500:
        bots[bot_id]['logs'] = bots[bot_id]['logs'][-500:]
    try:
        with open(os.path.join(get_bot_dir(bot_id), 'system.log'), 'a', encoding='utf-8') as f:
            f.write(json.dumps(entry) + '\n')
    except Exception:
        pass

def is_running(bot_id):
    return (bot_id in bots
            and bots[bot_id].get('process') is not None
            and bots[bot_id]['process'].poll() is None)

def broadcast_status(bot_id, status, start_t=None):
    cfg = load_config().get(bot_id, {})
    listeners = [cfg.get('owner')] + cfg.get('shared_with', [])
    payload = {'bot_id': bot_id, 'status': status}
    if start_t:
        payload['start_time'] = start_t
    for u in set(listeners):
        if u:
            with contextlib.suppress(Exception):
                socketio.emit('status_update', payload, room=u)

def start_bot(bot_id, startup_file=None):
    cfg     = load_config()
    bot_cfg = cfg.get(bot_id, {})
    bot_dir = get_bot_dir(bot_id)

    raw_sf = startup_file or bot_cfg.get('startup_file', 'main.py')
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

    # ── streaming helper ─────────────────────────────────────────────────────
    def _stream(cmd, cwd=None, timeout=300):
        """Run cmd, pipe stdout+stderr live to console. Return exit code."""
        run_env = os.environ.copy()
        run_env['PYTHONUNBUFFERED'] = '1'
        run_env['PIP_NO_COLOR'] = '1'
        run_env['PIP_DISABLE_PIP_VERSION_CHECK'] = '1'
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,   # merge stderr into stdout
                stdin=subprocess.DEVNULL,
                text=True,
                bufsize=1,
                cwd=cwd,
                env=run_env,
            )
        except FileNotFoundError as e:
            emit_log(bot_id, f'[Error] Command not found: {e}', 'error')
            return -1

        deadline = time.time() + timeout
        try:
            while True:
                line = proc.stdout.readline()
                if line:
                    emit_log(bot_id, line.rstrip(), 'default')
                else:
                    # readline() returned empty → process ended
                    break
                if time.time() > deadline:
                    proc.kill()
                    emit_log(bot_id, '[Error] Install timed out.', 'error')
                    return -1
        except Exception as e:
            emit_log(bot_id, f'[Error] Stream read error: {e}', 'error')
            try: proc.kill()
            except: pass
            return -1

        proc.wait()
        return proc.returncode

    # ── dependency install / build ───────────────────────────────────────────
    if ext == 'py':
        req = os.path.join(bot_dir, 'requirements.txt')
        if os.path.exists(req):
            emit_log(bot_id, '[System] Installing Python requirements…', 'system')
            rc = _stream([
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

    elif ext in ('js', 'ts'):
        pkg = os.path.join(bot_dir, 'package.json')
        if os.path.exists(pkg):
            emit_log(bot_id, '[System] Running npm install…', 'system')
            rc = _stream(['npm', 'install', '--no-progress', '--no-audit', '--no-fund'],
                         cwd=bot_dir)
            if rc != 0:
                emit_log(bot_id, '[Error] npm install failed — see output above.', 'error')
                return
            emit_log(bot_id, '[System] npm packages installed.', 'success')
        if ext == 'ts' and not shutil.which('ts-node') and not shutil.which('npx'):
            emit_log(bot_id, '[Error] ts-node / npx not found. Install Node.js and ts-node.', 'error')
            return

    elif ext == 'rs':
        if not os.path.exists(os.path.join(bot_dir, 'Cargo.toml')):
            emit_log(bot_id, '[Error] Cargo.toml not found.', 'error')
            return
        if not shutil.which('cargo'):
            emit_log(bot_id, '[Error] cargo not found. Install Rust toolchain.', 'error')
            return
        emit_log(bot_id, '[System] Building Rust project…', 'system')
        rc = _stream(['cargo', 'build', '--release'], cwd=bot_dir, timeout=600)
        if rc != 0:
            emit_log(bot_id, '[Error] cargo build failed — see output above.', 'error')
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
        rc = _stream(['dotnet', 'build', '--configuration', 'Release'], cwd=bot_dir, timeout=600)
        if rc != 0:
            emit_log(bot_id, '[Error] dotnet build failed — see output above.', 'error')
            return
        emit_log(bot_id, '[System] C# build successful.', 'success')

    # ── build run command ────────────────────────────────────────────────────
    if ext == 'py':
        cmd = [sys.executable, '-u', full_path]
    elif ext == 'js':
        cmd = ['node', full_path]
    elif ext == 'ts':
        cmd = ['ts-node', full_path] if shutil.which('ts-node') else ['npx', 'ts-node', full_path]
    elif ext == 'sh':
        cmd = ['bash', full_path]
    elif ext == 'rs':
        pkg_name = None
        try:
            with open(os.path.join(bot_dir, 'Cargo.toml')) as f:
                _ct = f.read()
            m = re.search(r'^\s*name\s*=\s*"([^"]+)"', _ct, re.MULTILINE)
            if m:
                pkg_name = m.group(1)
        except Exception:
            pass
        bin_dir = os.path.join(bot_dir, 'target', 'release')
        binary  = None
        if pkg_name:
            for suffix in ('', '.exe'):
                candidate = os.path.join(bin_dir, pkg_name + suffix)
                if os.path.isfile(candidate):
                    binary = candidate
                    break
        if not binary and os.path.isdir(bin_dir):
            skip = ('.d', '.rlib', '.pdb', '.exp', '.lib', '.so', '.dylib', '.dll')
            candidates = [
                f for f in os.listdir(bin_dir)
                if os.path.isfile(os.path.join(bin_dir, f))
                and not f.endswith(skip) and not f.startswith('.')
            ]
            if candidates:
                binary = os.path.join(bin_dir, candidates[0])
        if not binary:
            emit_log(bot_id, '[Error] Could not locate compiled Rust binary.', 'error')
            return
        cmd = [binary]
        emit_log(bot_id, f'[System] Running binary: {binary}', 'system')
    elif ext == 'cs':
        cmd = ['dotnet', 'run', '--configuration', 'Release', '--project', bot_dir]
    else:
        emit_log(bot_id, f'[Error] Unsupported extension: .{ext}', 'error')
        return

    # ── launch process ───────────────────────────────────────────────────────
    use_text = (ext != 'rs')
    run_env  = os.environ.copy()
    run_env.update(bot_cfg.get('env', {}))
    run_env['PYTHONUNBUFFERED'] = '1'

    emit_log(bot_id, f'[System] Starting {startup_file}…', 'system')
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdin=subprocess.PIPE,
            text=use_text,
            bufsize=1,
            cwd=bot_dir,
            env=run_env,
        )
        start_t = time.time()
        bots.setdefault(bot_id, {}).update({
            'process':      proc,
            'startup_file': startup_file,
            'start_time':   start_t,
            'auto_restart': bot_cfg.get('auto_restart', False),
        })
        bot_cfg['startup_file'] = startup_file
        cfg[bot_id] = bot_cfg
        save_config(cfg)
        broadcast_status(bot_id, 'online', start_t)

        def _read():
            try:
                if use_text:
                    for line in iter(proc.stdout.readline, ''):
                        emit_log(bot_id, line.rstrip(), 'default')
                else:
                    for raw in iter(proc.stdout.readline, b''):
                        emit_log(bot_id, raw.decode('utf-8', errors='replace').rstrip(), 'default')
            except Exception:
                pass
            proc.wait()
            broadcast_status(bot_id, 'offline')
            emit_log(bot_id, f'[System] Exited with code {proc.returncode}.', 'system')
            if bots.get(bot_id, {}).get('auto_restart') and proc.returncode != 0:
                emit_log(bot_id, '[System] Auto-restart in 3s…', 'system')
                time.sleep(3)
                if bots.get(bot_id, {}).get('auto_restart') and not is_running(bot_id):
                    start_bot(bot_id, startup_file)

        threading.Thread(target=_read, daemon=True).start()

    except Exception as e:
        emit_log(bot_id, f'[Error] Failed to start: {e}', 'error')

def stop_bot(bot_id, disable_auto_restart=True):
    if bot_id in bots:
        if disable_auto_restart:
            bots[bot_id]['auto_restart'] = False
        proc = bots[bot_id].get('process')
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            emit_log(bot_id, '[System] Stopped.', 'system')
            broadcast_status(bot_id, 'offline')


# ─── HTML ─────────────────────────────────────────────────────────────────────

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>VORTEX HOSTING</title>
<script src="https://cdn.socket.io/4.6.1/socket.io.min.js"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@400;500;600;700;900&family=Rajdhani:wght@400;500;600;700&family=Fira+Code:wght@300;400;500;700&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
:root{
  --void:#03040A;--deep:#07090F;--base:#0A0D16;--panel:#0E1220;--elevated:#141828;
  --border:rgba(0,200,255,0.07);--border-mid:rgba(0,200,255,0.14);--border-hi:rgba(0,200,255,0.28);
  --cyan:#00E5FF;--cyan-dim:rgba(0,229,255,0.08);--cyan-glow:rgba(0,229,255,0.3);
  --purple:#A020F0;--purple-dim:rgba(160,32,240,0.1);--purple-glow:rgba(160,32,240,0.4);
  --blue:#0066FF;--blue-dim:rgba(0,102,255,0.1);
  --green:#00FF7F;--green-dim:rgba(0,255,127,0.1);--green-glow:rgba(0,255,127,0.4);
  --amber:#FFB800;--amber-dim:rgba(255,184,0,0.1);
  --red:#FF2055;--red-dim:rgba(255,32,85,0.1);--red-glow:rgba(255,32,85,0.4);
  --text:#DCF0FF;--text-2:#5A7A9A;--text-3:#2A3E5A;
  --font-disp:'Orbitron',sans-serif;--font-sans:'Rajdhani',sans-serif;--font-mono:'Fira Code',monospace;
  --ease-spring:cubic-bezier(0.34,1.56,0.64,1);--ease-out:cubic-bezier(0.16,1,0.3,1);
}
html,body{width:100%;height:100%;overflow:hidden;background:var(--void);color:var(--text);font-family:var(--font-sans);-webkit-font-smoothing:antialiased}
body::before{content:'';position:fixed;inset:0;z-index:0;pointer-events:none;background-image:url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='noise'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23noise)' opacity='0.03'/%3E%3C/svg%3E");opacity:0.4}
body::after{content:'';position:fixed;inset:0;z-index:0;pointer-events:none;background-image:radial-gradient(circle,rgba(0,180,255,0.06) 1px,transparent 1px);background-size:32px 32px}
#ambientGlow{position:fixed;inset:0;z-index:0;pointer-events:none;background:radial-gradient(ellipse 60% 40% at 0% 0%,rgba(0,100,255,0.06) 0%,transparent 70%),radial-gradient(ellipse 50% 50% at 100% 100%,rgba(160,32,240,0.07) 0%,transparent 70%),radial-gradient(ellipse 40% 60% at 50% 50%,rgba(0,229,255,0.03) 0%,transparent 80%)}
#app{display:flex;width:100%;height:100%;position:relative;z-index:1}
::-webkit-scrollbar{width:4px;height:4px}::-webkit-scrollbar-track{background:transparent}::-webkit-scrollbar-thumb{background:rgba(0,200,255,0.2);border-radius:10px}::-webkit-scrollbar-thumb:hover{background:rgba(0,200,255,0.4)}

/* SIDEBAR */
.sidebar{width:264px;min-width:264px;height:100%;display:flex;flex-direction:column;position:relative;z-index:9500;background:linear-gradient(180deg,rgba(10,13,22,0.98) 0%,rgba(7,9,15,0.98) 100%);backdrop-filter:blur(40px);border-right:1px solid var(--border-mid);box-shadow:1px 0 40px rgba(0,0,0,0.8);transition:transform .4s var(--ease-out)}
.sidebar::before{content:'';position:absolute;top:0;left:0;right:0;height:1px;background:linear-gradient(90deg,transparent,var(--cyan),var(--purple),transparent);box-shadow:0 0 20px var(--cyan-glow);z-index:1}
.sidebar::after{content:'';position:absolute;top:0;right:0;width:1px;height:100%;background:linear-gradient(180deg,var(--cyan-glow),transparent 40%,var(--purple-glow) 80%,transparent);opacity:0.5;pointer-events:none}
.sidebar-close-btn{display:none;position:absolute;right:14px;top:22px;background:var(--elevated);border:1px solid var(--border-mid);color:var(--text-2);font-size:13px;cursor:pointer;transition:all .2s;z-index:10;width:28px;height:28px;border-radius:4px;align-items:center;justify-content:center}
.sidebar-close-btn:hover{color:var(--cyan);border-color:var(--border-hi)}
.logo{padding:26px 22px 20px;border-bottom:1px solid var(--border);position:relative;overflow:hidden}
.logo-wordmark{font-family:var(--font-disp);font-size:28px;font-weight:900;letter-spacing:6px;line-height:1;background:linear-gradient(135deg,#ffffff 0%,var(--cyan) 60%,var(--purple) 100%);-webkit-background-clip:text;-webkit-text-fill-color:transparent;filter:drop-shadow(0 0 20px var(--cyan-glow))}
.logo-sub{font-family:var(--font-mono);font-size:9px;letter-spacing:5px;color:var(--text-3);text-transform:uppercase;margin-top:5px}
.logo-line{position:absolute;bottom:0;left:0;right:0;height:2px;background:linear-gradient(90deg,var(--cyan),var(--purple));box-shadow:0 0 12px var(--cyan-glow)}
.nav{padding:18px 12px 8px;flex-shrink:0}
.nav-section{font-family:var(--font-mono);font-size:9px;font-weight:700;letter-spacing:4px;color:var(--text-3);text-transform:uppercase;padding:8px 10px 6px;display:flex;align-items:center;gap:10px}
.nav-section::after{content:'';flex:1;height:1px;background:var(--border)}
.nav-item{display:flex;align-items:center;gap:12px;padding:10px 13px;border-radius:6px;font-size:14px;font-weight:600;color:var(--text-2);cursor:pointer;transition:all .22s var(--ease-out);margin-bottom:3px;border:1px solid transparent;position:relative;overflow:hidden;font-family:var(--font-sans);letter-spacing:.5px}
.nav-item::before{content:'';position:absolute;left:0;top:0;bottom:0;width:2px;background:var(--cyan);box-shadow:0 0 10px var(--cyan-glow);transform:scaleY(0);transition:transform .22s var(--ease-spring);border-radius:0 2px 2px 0}
.nav-item:hover{background:rgba(0,229,255,0.05);color:var(--text);border-color:var(--border);transform:translateX(3px)}
.nav-item.active{background:linear-gradient(90deg,rgba(0,229,255,0.1) 0%,rgba(0,229,255,0.03) 100%);color:var(--cyan);border-color:var(--border-mid)}
.nav-item.active::before{transform:scaleY(1)}
.nav-icon{width:20px;text-align:center;font-size:15px;flex-shrink:0;opacity:.6;transition:all .2s}
.nav-item:hover .nav-icon,.nav-item.active .nav-icon{opacity:1}
.nav-item.active .nav-icon{color:var(--cyan);text-shadow:0 0 10px var(--cyan-glow)}
.instances-header{display:flex;align-items:center;justify-content:space-between;padding:14px 22px 10px;flex-shrink:0;border-top:1px solid var(--border)}
.instances-label{font-family:var(--font-mono);font-size:9px;letter-spacing:4px;text-transform:uppercase;color:var(--text-3)}
.instances-count{font-family:var(--font-mono);font-size:11px;font-weight:700;color:var(--cyan);background:var(--cyan-dim);border:1px solid var(--border-mid);border-radius:4px;padding:2px 8px}
.new-bot-btn{margin:0 12px 12px;display:flex;align-items:center;justify-content:center;gap:9px;padding:11px;border:1px dashed rgba(0,229,255,0.25);border-radius:6px;font-family:var(--font-disp);font-size:11px;font-weight:600;letter-spacing:2px;color:var(--text-3);background:transparent;cursor:pointer;transition:all .25s var(--ease-out);text-transform:uppercase}
.new-bot-btn:hover{border-color:var(--purple);color:var(--purple);background:var(--purple-dim);transform:translateY(-1px);box-shadow:0 4px 20px rgba(160,32,240,0.15)}
.new-bot-btn-plus{width:20px;height:20px;border-radius:4px;background:var(--elevated);display:flex;align-items:center;justify-content:center;font-size:15px;transition:all .2s;font-family:var(--font-sans)}
.bot-list{flex:1;overflow-y:auto;padding:0 12px 12px}
.bot-item{display:flex;align-items:center;gap:10px;padding:11px 13px;border-radius:6px;cursor:pointer;transition:all .2s;margin-bottom:5px;border:1px solid var(--border);background:rgba(0,0,0,0.25);position:relative;overflow:hidden}
.bot-item:hover{border-color:var(--border-mid);background:rgba(0,229,255,0.04)}
.bot-item.active{background:rgba(0,229,255,0.07);border-color:rgba(0,229,255,0.25);box-shadow:0 0 20px rgba(0,229,255,0.06)}
.bot-dot{width:8px;height:8px;border-radius:50%;flex-shrink:0}
.bot-dot.online{background:var(--green);box-shadow:0 0 10px var(--green-glow);animation:dotPulse 2.2s ease-in-out infinite}
@keyframes dotPulse{0%,100%{box-shadow:0 0 10px var(--green-glow)}50%{box-shadow:0 0 18px var(--green-glow),0 0 8px var(--green)}}
.bot-dot.offline{background:var(--text-3)}
.bot-name{font-family:var(--font-sans);font-size:14px;font-weight:600;color:var(--text-2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;transition:color .2s;flex:1}
.bot-item.active .bot-name,.bot-item:hover .bot-name{color:var(--text)}
.bot-status{font-family:var(--font-mono);font-size:9px;letter-spacing:2px;text-transform:uppercase;color:var(--text-3);margin-top:2px}
.bot-item.active .bot-status.online{color:var(--green)}
.bot-shared-tag{font-family:var(--font-mono);font-size:9px;color:var(--purple);background:var(--purple-dim);border:1px solid rgba(160,32,240,0.25);border-radius:3px;padding:1px 5px;flex-shrink:0}
.sidebar-footer{padding:14px 22px;border-top:1px solid var(--border);display:flex;justify-content:space-between;align-items:center;background:rgba(0,0,0,0.5);flex-shrink:0}
.logout-btn{font-family:var(--font-mono);font-size:10px;letter-spacing:2px;color:var(--text-3);cursor:pointer;transition:all .2s;text-transform:uppercase;background:none;border:none}
.logout-btn:hover{color:var(--red)}
.sidebar-clock{font-family:var(--font-mono);font-size:13px;color:var(--cyan);font-weight:500}

/* MOBILE */
.sidebar-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,0.75);z-index:9400;opacity:0;transition:opacity .3s;cursor:pointer;backdrop-filter:blur(4px)}
.sidebar-overlay.open{opacity:1}
.mobile-bottom-nav{display:none;position:fixed;bottom:14px;left:12px;right:12px;height:64px;background:rgba(10,13,22,0.92);backdrop-filter:blur(30px);border:1px solid var(--border-mid);border-radius:18px;z-index:9000;justify-content:space-around;align-items:center;padding:0 8px;box-shadow:0 8px 40px rgba(0,0,0,0.8)}
.m-nav-item{display:flex;flex-direction:column;align-items:center;gap:3px;color:var(--text-3);cursor:pointer;transition:all .25s var(--ease-spring);padding:7px 10px;border-radius:12px;flex:1}
.m-nav-icon{font-size:18px;transition:transform .25s var(--ease-spring)}
.m-nav-label{font-family:var(--font-mono);font-size:9px;font-weight:500;letter-spacing:1px;text-transform:uppercase}
.m-nav-item.active{color:var(--cyan)}
.m-nav-item.active .m-nav-icon{transform:translateY(-3px) scale(1.1);filter:drop-shadow(0 0 6px var(--cyan-glow))}

/* TOPBAR */
.main{flex:1;min-width:0;height:100%;display:flex;flex-direction:column;position:relative;z-index:10}
.topbar{height:64px;min-height:64px;background:linear-gradient(90deg,rgba(10,13,22,0.96) 0%,rgba(8,10,18,0.96) 100%);backdrop-filter:blur(20px);border-bottom:1px solid var(--border-mid);display:flex;align-items:center;justify-content:space-between;padding:0 24px;gap:14px;flex-shrink:0;box-shadow:0 4px 30px rgba(0,0,0,0.6);position:relative}
.topbar::after{content:'';position:absolute;bottom:-1px;left:0;right:0;height:1px;background:linear-gradient(90deg,transparent,rgba(0,229,255,0.15),transparent)}
.topbar-left{display:flex;align-items:center;gap:12px;min-width:0}
.mobile-menu-btn{display:none;background:var(--elevated);border:1px solid var(--border-mid);color:var(--text-2);padding:7px 11px;border-radius:6px;font-size:15px;cursor:pointer;transition:all .2s;flex-shrink:0}
.mobile-menu-btn:hover{border-color:var(--border-hi);color:var(--cyan)}
.breadcrumb{display:flex;align-items:center;gap:9px;min-width:0}
.bc-brand{font-family:var(--font-mono);font-size:11px;letter-spacing:3px;color:var(--text-3);text-transform:uppercase}
.bc-sep{color:rgba(0,200,255,0.3);font-size:14px;opacity:.6}
.bc-page{font-family:var(--font-disp);font-size:17px;font-weight:700;letter-spacing:3px;color:var(--text);text-transform:uppercase}
.bc-bot{font-family:var(--font-mono);font-size:11px;color:var(--purple);background:var(--purple-dim);padding:4px 10px;border-radius:4px;border:1px solid rgba(160,32,240,0.25);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:160px}
.topbar-right{display:flex;align-items:center;gap:8px;flex-shrink:0}

/* RUNTIME TOGGLE */
.runtime-switcher{display:flex;align-items:center;background:rgba(0,0,0,0.5);border:1px solid var(--border-mid);border-radius:6px;padding:3px;gap:2px;flex-shrink:0}
.runtime-btn{font-family:var(--font-mono);font-size:10px;font-weight:700;letter-spacing:1px;text-transform:uppercase;padding:5px 10px;border-radius:4px;border:none;background:transparent;color:var(--text-3);cursor:pointer;transition:all .2s;white-space:nowrap;display:flex;align-items:center;gap:5px}
.runtime-btn:hover{color:var(--text);background:rgba(255,255,255,0.07)}
.runtime-btn.active.rt-py{background:rgba(0,255,127,0.12);color:var(--green);border:1px solid rgba(0,255,127,0.35);box-shadow:0 0 10px rgba(0,255,127,0.1)}
.runtime-btn.active.rt-ts{background:rgba(49,120,198,0.18);color:#61B8FF;border:1px solid rgba(49,120,198,0.45);box-shadow:0 0 10px rgba(49,120,198,0.15)}
.runtime-btn.active.rt-rs{background:rgba(247,76,0,0.14);color:#FF8050;border:1px solid rgba(247,76,0,0.4);box-shadow:0 0 10px rgba(247,76,0,0.12)}
.runtime-btn.active.rt-cs{background:rgba(155,79,202,0.14);color:#C080FF;border:1px solid rgba(155,79,202,0.4);box-shadow:0 0 10px rgba(155,79,202,0.12)}
.runtime-sep{width:1px;height:14px;background:var(--border-mid);flex-shrink:0}
.runtime-icon{font-size:12px;line-height:1}

.host-switcher{display:flex;align-items:center;background:rgba(0,0,0,0.5);border:1px solid var(--border-mid);border-radius:6px;padding:3px;gap:2px}
.host-btn{font-family:var(--font-mono);font-size:10px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;padding:5px 11px;border-radius:4px;border:none;background:transparent;color:var(--text-3);cursor:pointer;transition:all .2s;white-space:nowrap}
.host-btn:hover{color:var(--text);background:rgba(255,255,255,0.07)}
.host-btn.active{background:linear-gradient(135deg,rgba(0,229,255,0.18) 0%,rgba(0,150,200,0.12) 100%);color:var(--cyan);border:1px solid rgba(0,229,255,0.3);box-shadow:0 0 10px rgba(0,229,255,0.1)}
.host-sep{width:1px;height:14px;background:var(--border-mid);flex-shrink:0}
.status-badge{display:flex;align-items:center;gap:7px;padding:6px 13px;border-radius:20px;font-family:var(--font-mono);font-size:11px;font-weight:600;letter-spacing:1px;transition:all .3s;border:1px solid var(--border);background:rgba(0,0,0,0.4);text-transform:uppercase}
.status-badge.online{color:var(--green);border-color:rgba(0,255,127,0.3);background:var(--green-dim);box-shadow:0 0 14px rgba(0,255,127,0.1)}
.status-badge.offline{color:var(--text-3);border-color:var(--border)}
.status-led{width:7px;height:7px;border-radius:50%;flex-shrink:0}
.status-badge.online .status-led{background:var(--green);box-shadow:0 0 8px var(--green-glow);animation:ledBlink 1.8s ease-in-out infinite}
.status-badge.offline .status-led{background:var(--text-3)}
@keyframes ledBlink{0%,100%{opacity:1}50%{opacity:.3}}

/* BUTTONS */
.btn{display:inline-flex;align-items:center;justify-content:center;gap:6px;padding:8px 16px;border-radius:6px;font-size:11px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;border:none;cursor:pointer;transition:all .18s var(--ease-out);font-family:var(--font-disp);position:relative;overflow:hidden;white-space:nowrap}
.btn::after{content:'';position:absolute;inset:0;opacity:0;background:linear-gradient(135deg,rgba(255,255,255,0.1) 0%,transparent 100%);transition:opacity .18s}
.btn:hover::after{opacity:1}
.btn:active{transform:scale(0.95)!important}
.btn:disabled{opacity:.45;cursor:not-allowed;pointer-events:none}
.btn-cyan{background:linear-gradient(135deg,rgba(0,229,255,0.2) 0%,rgba(0,150,200,0.15) 100%);color:var(--cyan);border:1px solid rgba(0,229,255,0.35);box-shadow:0 0 15px rgba(0,229,255,0.1)}
.btn-cyan:hover{background:linear-gradient(135deg,rgba(0,229,255,0.3) 0%,rgba(0,150,200,0.2) 100%);border-color:rgba(0,229,255,0.6);transform:translateY(-1px);box-shadow:0 4px 20px rgba(0,229,255,0.2)}
.btn-green{background:var(--green-dim);color:var(--green);border:1px solid rgba(0,255,127,0.3)}
.btn-green:hover{background:rgba(0,255,127,0.18);border-color:rgba(0,255,127,0.55);transform:translateY(-1px);box-shadow:0 4px 15px rgba(0,255,127,0.15)}
.btn-red{background:var(--red-dim);color:var(--red);border:1px solid rgba(255,32,85,0.3)}
.btn-red:hover{background:rgba(255,32,85,0.2);border-color:rgba(255,32,85,0.55);transform:translateY(-1px);box-shadow:0 4px 15px rgba(255,32,85,0.15)}
.btn-amber{background:var(--amber-dim);color:var(--amber);border:1px solid rgba(255,184,0,0.3)}
.btn-amber:hover{background:rgba(255,184,0,0.2);border-color:rgba(255,184,0,0.55);transform:translateY(-1px);box-shadow:0 4px 15px rgba(255,184,0,0.15)}
.btn-purple{background:var(--purple-dim);color:var(--purple);border:1px solid rgba(160,32,240,0.3)}
.btn-purple:hover{background:rgba(160,32,240,0.2);border-color:rgba(160,32,240,0.55);transform:translateY(-1px);box-shadow:0 4px 15px rgba(160,32,240,0.15)}
.btn-ghost{background:rgba(255,255,255,0.04);color:var(--text);border:1px solid var(--border-mid)}
.btn-ghost:hover{background:rgba(255,255,255,0.08);border-color:var(--border-hi);transform:translateY(-1px)}
.btn-sm{padding:6px 12px;font-size:10px;letter-spacing:1px}
.btn-row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}

.icon-btn{width:30px;height:30px;display:inline-flex;align-items:center;justify-content:center;border-radius:5px;border:1px solid var(--border-mid);background:rgba(255,255,255,0.04);color:var(--text-2);cursor:pointer;transition:all .15s;font-size:13px;flex-shrink:0;padding:0}
.icon-btn:hover{border-color:var(--border-hi);color:var(--text);transform:translateY(-1px)}
.icon-btn:active{transform:scale(0.9)}
.icon-btn.ib-amber{color:var(--amber);border-color:rgba(255,184,0,0.3);background:var(--amber-dim)}
.icon-btn.ib-amber:hover{background:rgba(255,184,0,0.2);border-color:rgba(255,184,0,0.6)}
.icon-btn.ib-red{color:var(--red);border-color:rgba(255,32,85,0.3);background:var(--red-dim)}
.icon-btn.ib-red:hover{background:rgba(255,32,85,0.2);border-color:rgba(255,32,85,0.6)}
.icon-btn.ib-cyan{color:var(--cyan);border-color:rgba(0,229,255,0.25);background:var(--cyan-dim)}
.icon-btn.ib-cyan:hover{background:rgba(0,229,255,0.15);border-color:rgba(0,229,255,0.6)}

/* PAGES */
.page{flex:1;min-height:0;overflow-y:auto;padding:24px;display:none}
.page.active{display:block;animation:pageIn .3s var(--ease-out)}
@keyframes pageIn{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:translateY(0)}}

/* STAT CARDS */
.stats-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:20px}
.stat-card{background:var(--panel);border:1px solid var(--border);border-radius:8px;padding:20px;position:relative;overflow:hidden;transition:all .25s var(--ease-out)}
.stat-card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:var(--card-accent,var(--cyan));opacity:.8}
.stat-card::after{content:'';position:absolute;top:0;left:0;right:0;bottom:0;background:radial-gradient(ellipse at top left,var(--card-glow,rgba(0,229,255,0.04)) 0%,transparent 70%);pointer-events:none}
.stat-card:hover{border-color:var(--border-mid);transform:translateY(-2px);box-shadow:0 10px 35px rgba(0,0,0,0.6)}
.stat-card-cyan{--card-accent:var(--cyan);--card-glow:rgba(0,229,255,0.05)}
.stat-card-purple{--card-accent:var(--purple);--card-glow:rgba(160,32,240,0.04)}
.stat-label{font-family:var(--font-mono);font-size:10px;font-weight:700;letter-spacing:3px;color:var(--text-2);margin-bottom:10px;text-transform:uppercase}
.stat-value{font-family:var(--font-disp);font-size:36px;font-weight:700;line-height:1;margin-bottom:6px}
.sv-cyan{color:var(--cyan);text-shadow:0 0 25px rgba(0,229,255,0.4)}
.sv-green{color:var(--green);text-shadow:0 0 25px rgba(0,255,127,0.4)}
.sv-red{color:var(--red);text-shadow:0 0 20px rgba(255,32,85,0.3)}
.sv-purple{color:var(--purple);text-shadow:0 0 25px rgba(160,32,240,0.4)}
.stat-sub{font-family:var(--font-mono);font-size:10px;color:var(--text-3)}

/* PANELS */
.panel{background:var(--panel);border:1px solid var(--border);border-radius:8px;margin-bottom:18px;overflow:hidden;box-shadow:0 6px 25px rgba(0,0,0,0.5)}
.panel-head{display:flex;align-items:center;justify-content:space-between;padding:15px 20px;border-bottom:1px solid var(--border);background:linear-gradient(90deg,rgba(0,0,0,0.4) 0%,rgba(0,0,0,0.2) 100%);flex-wrap:wrap;gap:10px}
.panel-title{display:flex;align-items:center;gap:10px;font-family:var(--font-disp);font-size:13px;font-weight:700;letter-spacing:3px;color:var(--text);text-transform:uppercase}
.panel-icon{width:26px;height:26px;border-radius:5px;background:var(--elevated);border:1px solid var(--border-mid);display:flex;align-items:center;justify-content:center;font-size:12px}
.panel-tag{font-family:var(--font-mono);font-size:10px;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:var(--purple);padding:3px 9px;border:1px solid rgba(160,32,240,0.3);border-radius:4px;background:var(--purple-dim)}
.panel-body{padding:20px}

/* TERMINAL */
.term-chrome{background:var(--deep);border-bottom:1px solid var(--border);padding:10px 16px;display:flex;align-items:center;gap:10px}
.term-dots{display:flex;gap:6px}
.term-dot{width:11px;height:11px;border-radius:50%}
.term-title{flex:1;text-align:center;font-family:var(--font-mono);font-size:10px;font-weight:700;letter-spacing:4px;color:var(--text-3);text-transform:uppercase}
.terminal{background:rgba(0,0,0,0.6);padding:16px;overflow-y:auto;font-family:var(--font-mono);font-size:13px;line-height:1.7}
.log-row{display:flex;align-items:baseline;gap:12px;padding:2px 4px;border-radius:4px;transition:background .15s}
.log-row:hover{background:rgba(0,200,255,0.04)}
.log-ts{font-size:11px;color:var(--text-3);flex-shrink:0;min-width:58px}
.log-tag{font-size:9px;padding:2px 6px;border-radius:3px;flex-shrink:0;text-transform:uppercase;font-weight:700;letter-spacing:2px}
.log-tag.sys{background:var(--blue-dim);color:var(--blue);border:1px solid rgba(0,102,255,0.3)}
.log-tag.err{background:var(--red-dim);color:var(--red);border:1px solid rgba(255,32,85,0.3)}
.log-tag.ok{background:var(--green-dim);color:var(--green);border:1px solid rgba(0,255,127,0.3)}
.log-tag.warn{background:var(--amber-dim);color:var(--amber);border:1px solid rgba(255,184,0,0.3)}
.log-tag.out{background:rgba(255,255,255,0.05);color:var(--text-2);border:1px solid var(--border)}
.log-msg{flex:1;word-break:break-all}
.log-msg.sys{color:var(--blue)}.log-msg.err{color:var(--red)}.log-msg.ok{color:var(--green)}.log-msg.warn{color:var(--amber)}.log-msg.out{color:var(--text-2)}
.term-input-wrap{display:flex;align-items:center;gap:12px;background:rgba(0,0,0,0.5);border-top:1px solid var(--border);padding:12px 18px;transition:all .2s}
.term-input-wrap:focus-within{background:rgba(0,229,255,0.03);border-top-color:rgba(0,229,255,0.25)}
.term-prompt{font-family:var(--font-mono);font-size:15px;color:var(--cyan);flex-shrink:0;text-shadow:0 0 8px var(--cyan-glow)}
.term-input{flex:1;background:none;border:none;outline:none;font-family:var(--font-mono);font-size:13px;color:var(--cyan);caret-color:var(--cyan)}
.term-input::placeholder{color:var(--text-3)}

/* FORMS */
.form-group{margin-bottom:18px}
.form-label{display:flex;align-items:center;gap:10px;font-family:var(--font-mono);font-size:10px;font-weight:700;letter-spacing:3px;color:var(--text-2);text-transform:uppercase;margin-bottom:9px}
.form-label::after{content:'';flex:1;height:1px;background:var(--border)}
.form-input,.form-select,.form-textarea{width:100%;background:rgba(0,0,0,0.5);border:1px solid var(--border);border-left:2px solid var(--border-mid);border-radius:6px;padding:12px 15px;font-size:13px;color:var(--text);outline:none;font-family:var(--font-mono);transition:all .2s}
.form-input:focus,.form-select:focus,.form-textarea:focus{border-color:var(--border-hi);border-left-color:var(--cyan);background:rgba(0,229,255,0.03);box-shadow:0 0 0 3px rgba(0,229,255,0.06)}
.form-input::placeholder,.form-textarea::placeholder{color:var(--text-3)}
.form-select option{background:var(--base)}
.form-textarea{resize:vertical;min-height:120px;line-height:1.7}
.form-row-2{display:grid;grid-template-columns:1fr 1fr;gap:18px}
.section-divider{font-family:var(--font-disp);font-size:12px;font-weight:700;letter-spacing:3px;color:var(--cyan);border-bottom:1px solid var(--border);padding-bottom:9px;margin:24px 0 16px;text-transform:uppercase;text-shadow:0 0 15px var(--cyan-glow)}

/* ENV */
.env-row{display:grid;grid-template-columns:1fr 1.5fr auto;gap:7px;margin-bottom:7px;align-items:center}
.env-field{background:rgba(0,0,0,0.5);border:1px solid var(--border);border-radius:5px;padding:9px 12px;font-family:var(--font-mono);font-size:12px;color:var(--text);outline:none;width:100%;transition:border-color .2s}
.env-field:focus{border-color:var(--border-hi)}
.env-key{color:var(--amber)}

/* FILE TABLE */
.file-table-wrap{overflow-x:auto;-webkit-overflow-scrolling:touch;width:100%}
.file-table{width:100%;border-collapse:collapse}
.file-table th{font-family:var(--font-mono);font-size:10px;text-transform:uppercase;letter-spacing:3px;color:var(--text-3);padding:10px 14px;border-bottom:1px solid var(--border-mid);text-align:left;font-weight:700;background:rgba(0,0,0,0.3);white-space:nowrap}
.file-table td{padding:9px 14px;border-bottom:1px solid var(--border);vertical-align:middle;font-family:var(--font-mono);font-size:12.5px}
.file-table tr:last-child td{border-bottom:none}
.file-table tr:hover td{background:rgba(0,229,255,0.025)}
.file-table colgroup col:nth-child(1){width:auto}
.file-table colgroup col:nth-child(2){width:68px}
.file-table colgroup col:nth-child(3){width:62px}
.file-table colgroup col:nth-child(4){width:146px}
.file-table colgroup col:nth-child(5){width:126px}

.fn-cell{display:flex;align-items:center;gap:8px;min-width:0}
.fn-icon{font-size:14px;opacity:.8;flex-shrink:0;line-height:1}
.fn-link{flex:1;min-width:0;color:var(--cyan);cursor:pointer;transition:all .18s;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:12.5px}
.fn-link:hover{color:#fff;text-shadow:0 0 10px var(--cyan-glow)}
.fn-rename{display:none;flex:1;align-items:center;gap:5px;min-width:0}
.fn-rename.on{display:flex}
.fn-rename-input{flex:1;min-width:0;background:rgba(0,0,0,0.7);border:1px solid var(--border-hi);border-bottom:2px solid var(--amber);border-radius:5px;padding:4px 9px;font-family:var(--font-mono);font-size:12px;color:var(--amber);outline:none}
.fn-rename-ok{background:var(--amber-dim);border:1px solid rgba(255,184,0,0.45);border-radius:4px;color:var(--amber);font-family:var(--font-mono);font-size:10px;font-weight:700;padding:3px 8px;cursor:pointer;white-space:nowrap;transition:all .15s;flex-shrink:0}
.fn-rename-ok:hover{background:rgba(255,184,0,0.25)}
.fn-rename-cancel{background:transparent;border:1px solid var(--border);border-radius:4px;color:var(--text-3);font-family:var(--font-mono);font-size:10px;padding:3px 7px;cursor:pointer;transition:all .15s;flex-shrink:0}
.fn-rename-cancel:hover{color:var(--text);border-color:var(--border-hi)}

.file-ext-badge{font-size:9px;letter-spacing:1.5px;text-transform:uppercase;padding:2px 6px;border-radius:4px;border:1px solid var(--border-mid);color:var(--text-2);background:rgba(0,0,0,0.4);white-space:nowrap}
.file-actions{display:flex;gap:4px;align-items:center;flex-wrap:nowrap}

/* UPLOAD */
.drop-zone{border:2px dashed rgba(0,229,255,0.2);padding:36px 20px;text-align:center;transition:all .3s;background:rgba(0,0,0,0.25);border-radius:8px}
.drop-zone.dragging{border-color:var(--cyan);background:rgba(0,229,255,0.04);box-shadow:0 0 40px rgba(0,229,255,0.08)}
.drop-icon{font-size:40px;margin-bottom:12px;display:block;color:var(--text-3);transition:all .3s;pointer-events:none}
.drop-zone.dragging .drop-icon{color:var(--cyan);transform:translateY(-5px);filter:drop-shadow(0 0 10px var(--cyan-glow))}
.drop-title{font-family:var(--font-disp);font-size:18px;letter-spacing:4px;color:var(--text);margin-bottom:7px;pointer-events:none}
.drop-sub{font-family:var(--font-mono);font-size:10px;letter-spacing:2px;color:var(--text-3);margin-bottom:20px;pointer-events:none}
.upload-row{display:flex;align-items:center;gap:9px;padding:9px 13px;background:rgba(0,0,0,0.5);border:1px solid var(--border);border-radius:6px;margin-top:5px;font-family:var(--font-mono);font-size:11px;color:var(--text-2)}
.upload-bar-wrap{flex:1;height:3px;background:rgba(0,200,255,0.1);border-radius:2px;overflow:hidden}
.upload-bar-fill{height:100%;background:linear-gradient(90deg,var(--cyan),var(--purple));border-radius:2px;transition:width .12s linear}

/* RESOURCES */
.res-item{margin-bottom:22px}
.res-header{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:9px}
.res-label{font-family:var(--font-mono);font-size:10px;letter-spacing:3px;text-transform:uppercase;color:var(--text-2)}
.res-value{font-family:var(--font-disp);font-size:22px;font-weight:700}
.res-track{height:5px;background:rgba(0,0,0,0.6);border:1px solid var(--border);border-radius:3px;overflow:hidden}
.res-fill{height:100%;border-radius:3px;transition:width 1s ease}

/* MODAL */
.modal-veil{display:none;position:fixed;inset:0;background:rgba(0,0,0,0.75);z-index:10000;align-items:center;justify-content:center;backdrop-filter:blur(8px)}
.modal-veil.open{display:flex;animation:veilIn .25s ease}
@keyframes veilIn{from{opacity:0}to{opacity:1}}
.modal-box{background:linear-gradient(160deg,var(--elevated) 0%,var(--base) 100%);border:1px solid var(--border-mid);border-top:2px solid var(--cyan);border-radius:10px;padding:32px;width:95%;max-width:560px;max-height:90vh;overflow-y:auto;box-shadow:0 24px 80px rgba(0,0,0,0.9),0 0 60px rgba(0,229,255,0.06);animation:modalIn .32s var(--ease-spring)}
.modal-box.wide{max-width:980px}
@keyframes modalIn{from{transform:scale(0.93) translateY(22px);opacity:0}to{transform:scale(1) translateY(0);opacity:1}}
.modal-title{font-family:var(--font-disp);font-size:26px;font-weight:700;color:var(--text);margin-bottom:24px;letter-spacing:4px;display:flex;align-items:center;gap:10px}
.modal-title-accent{color:var(--cyan);text-shadow:0 0 20px var(--cyan-glow)}
.modal-footer{display:flex;justify-content:flex-end;gap:10px;margin-top:24px;padding-top:20px;border-top:1px solid var(--border)}

/* LOGIN */
#loginOverlay{position:fixed;inset:0;background:var(--void);z-index:99999;display:flex;align-items:center;justify-content:center}
#loginOverlay::before{content:'';position:absolute;inset:0;pointer-events:none;background:radial-gradient(ellipse 60% 50% at 30% 30%,rgba(0,229,255,0.08) 0%,transparent 60%),radial-gradient(ellipse 50% 60% at 80% 70%,rgba(160,32,240,0.08) 0%,transparent 60%)}
.login-card{background:linear-gradient(160deg,rgba(14,18,32,0.92) 0%,rgba(10,13,22,0.96) 100%);backdrop-filter:blur(40px);border:1px solid var(--border-mid);border-top:2px solid var(--purple);padding:48px 42px;width:90%;max-width:420px;border-radius:12px;box-shadow:0 24px 80px rgba(0,0,0,0.9),0 0 60px rgba(160,32,240,0.08);animation:loginUp .7s var(--ease-spring);position:relative;z-index:1}
@keyframes loginUp{from{transform:translateY(40px) scale(0.96);opacity:0}to{transform:translateY(0) scale(1);opacity:1}}
.login-logo{font-family:var(--font-disp);font-size:48px;font-weight:900;letter-spacing:8px;text-align:center;margin-bottom:4px;background:linear-gradient(135deg,#fff 0%,var(--cyan) 50%,var(--purple) 100%);-webkit-background-clip:text;-webkit-text-fill-color:transparent;filter:drop-shadow(0 0 30px rgba(0,229,255,0.3))}
.login-tagline{font-family:var(--font-mono);color:var(--text-3);font-size:10px;letter-spacing:6px;text-transform:uppercase;text-align:center;margin-bottom:26px}
.auth-tabs{display:flex;background:rgba(0,0,0,0.4);border-radius:6px;padding:3px;margin-bottom:20px;border:1px solid var(--border)}
.auth-tab{flex:1;text-align:center;padding:8px 10px;font-family:var(--font-mono);font-size:12px;font-weight:700;letter-spacing:2px;color:var(--text-3);cursor:pointer;text-transform:uppercase;transition:all .25s;border-radius:4px}
.auth-tab.active{color:var(--purple);background:var(--purple-dim);border:1px solid rgba(160,32,240,0.3);text-shadow:0 0 10px rgba(160,32,240,0.4)}

/* CODE EDITOR */
.code-editor{width:100%;min-height:500px;background:rgba(0,0,0,0.7);border:1px solid var(--border);border-left:3px solid var(--blue);border-radius:6px;padding:18px;font-family:var(--font-mono);font-size:13px;color:var(--text);outline:none;resize:vertical;line-height:1.7;caret-color:var(--cyan);transition:border-color .2s}
.code-editor:focus{border-left-color:var(--cyan);border-color:var(--border-hi)}

/* DANGER ZONE */
.danger-zone{border:1px solid rgba(255,32,85,0.25);border-left:3px solid var(--red);background:linear-gradient(90deg,rgba(255,32,85,0.05) 0%,transparent 100%);padding:20px;margin-top:18px;border-radius:8px}

/* TOASTS */
.toast-tray{position:fixed;bottom:24px;right:24px;z-index:20000;display:flex;flex-direction:column;gap:9px;pointer-events:none}
.toast{background:var(--elevated);border:1px solid var(--border-mid);border-radius:8px;padding:12px 16px;font-size:13px;font-weight:600;color:var(--text);font-family:var(--font-sans);letter-spacing:.5px;box-shadow:0 8px 30px rgba(0,0,0,0.7);display:flex;align-items:center;gap:10px;pointer-events:all;min-width:240px;animation:toastIn .32s var(--ease-spring);position:relative;overflow:hidden}
.toast::before{content:'';position:absolute;left:0;top:0;bottom:0;width:3px;border-radius:3px 0 0 3px}
.toast.success::before{background:var(--green);box-shadow:0 0 8px var(--green-glow)}
.toast.error::before{background:var(--red);box-shadow:0 0 8px var(--red-glow)}
.toast.info::before{background:var(--blue)}
.toast-icon{width:20px;height:20px;border-radius:4px;display:flex;align-items:center;justify-content:center;font-size:11px;flex-shrink:0}
.toast.success .toast-icon{background:var(--green-dim);color:var(--green)}
.toast.error .toast-icon{background:var(--red-dim);color:var(--red)}
.toast.info .toast-icon{background:var(--blue-dim);color:var(--blue)}
.toast-close{margin-left:auto;cursor:pointer;color:var(--text-3);font-size:13px;transition:color .2s}
.toast-close:hover{color:var(--text)}
@keyframes toastIn{from{transform:translateX(18px);opacity:0}to{transform:translateX(0);opacity:1}}

/* SUBUSERS */
.subuser-row{display:flex;justify-content:space-between;align-items:center;padding:10px 13px;background:rgba(0,0,0,0.35);border:1px solid var(--border);border-radius:6px;margin-bottom:5px;font-family:var(--font-mono);font-size:12px}

/* RESPONSIVE */
@media(max-width:860px){
  .sidebar{position:fixed;left:0;transform:translateX(-110%);width:272px;z-index:9600}
  .sidebar.open{transform:translateX(0)}
  .sidebar-overlay{display:block}
  .sidebar .nav{display:none}
  .sidebar-close-btn{display:flex}
  .mobile-bottom-nav{display:flex}
  .mobile-menu-btn{display:flex}
  .main{padding-bottom:88px}
  .topbar{height:auto;min-height:58px;padding:10px 14px;flex-direction:column;align-items:stretch;gap:9px}
  .topbar-left{width:100%;justify-content:space-between}
  .breadcrumb{flex:1;margin-left:8px}
  .bc-page{font-size:15px}
  .bc-brand,.bc-sep:first-of-type{display:none}
  .bc-bot{max-width:110px;font-size:10px}
  .topbar-right{display:flex;flex-wrap:nowrap;overflow-x:auto;width:100%;gap:6px;border-top:1px solid var(--border);padding-top:9px;-webkit-overflow-scrolling:touch}
  .topbar-right::-webkit-scrollbar{display:none}
  .runtime-btn{padding:5px 7px;font-size:9px}
  .runtime-btn .runtime-icon{display:none}
  .host-btn{padding:5px 8px;font-size:9px}
  .topbar-right .btn{white-space:nowrap;flex-shrink:0;padding:7px 11px;font-size:10px}
  .page{padding:10px}
  .stats-grid{grid-template-columns:1fr 1fr;gap:9px;margin-bottom:12px}
  .stat-value{font-size:28px}
  .form-row-2{grid-template-columns:1fr;gap:12px}
  .panel-head{padding:11px 13px;flex-direction:column;align-items:flex-start;gap:9px}
  .panel-body{padding:13px}
  .file-table th:nth-child(4),.file-table td:nth-child(4){display:none}
  .file-table th:nth-child(2),.file-table td:nth-child(2){display:none}
  .drop-zone{padding:24px 12px}
  .toast-tray{bottom:88px;right:8px;left:8px}
  .toast{min-width:0;width:100%}
}
@media(max-width:480px){
  .stats-grid{grid-template-columns:1fr}
  .stat-value{font-size:24px}
  .login-card{padding:28px 16px;width:96%}
  .login-logo{font-size:36px}
  .modal-box{padding:18px 12px}
  .modal-title{font-size:20px;margin-bottom:16px}
  .page{padding:7px}
}
</style>
</head>
<body>
<div id="ambientGlow"></div>

<!-- LOGIN -->
<div id="loginOverlay">
  <div class="login-card">
    <div class="login-logo">VORTEX</div>
    <div class="login-tagline">Hosting Platform</div>
    <div class="auth-tabs">
      <div class="auth-tab active" id="tabLogin" onclick="switchAuthMode('login')">Login</div>
      <div class="auth-tab" id="tabRegister" onclick="switchAuthMode('register')">Register</div>
    </div>
    <div class="form-group" style="margin-bottom:11px">
      <input class="form-input" id="authUsername" placeholder="Username" style="text-align:center;letter-spacing:2px" onkeydown="if(event.key==='Enter')submitAuth()">
    </div>
    <div class="form-group" style="margin-bottom:18px">
      <input type="password" class="form-input" id="authPassword" placeholder="Password" style="text-align:center;letter-spacing:2px" onkeydown="if(event.key==='Enter')submitAuth()">
    </div>
    <button class="btn btn-cyan" id="authBtn" style="width:100%;padding:14px;font-size:12px;letter-spacing:4px" onclick="submitAuth()">AUTHENTICATE</button>
  </div>
</div>

<div class="sidebar-overlay" id="sidebarOverlay" onclick="toggleSidebar()"></div>

<div id="app">
  <!-- SIDEBAR -->
  <aside class="sidebar" id="sidebar">
    <button class="sidebar-close-btn" onclick="toggleSidebar()">✕</button>
    <div class="logo">
      <div class="logo-wordmark">VORTEX</div>
      <div class="logo-sub">Hosting Platform // v11.5</div>
      <div class="logo-line"></div>
    </div>
    <nav class="nav">
      <div class="nav-section">System</div>
      <div class="nav-item active" data-page="dashboard" onclick="navTo('dashboard',this)"><span class="nav-icon">◈</span> Dashboard</div>
      <div class="nav-item" data-page="console" onclick="navTo('console',this)"><span class="nav-icon">_</span> Console</div>
      <div class="nav-item" data-page="files" onclick="navTo('files',this)"><span class="nav-icon">≡</span> File Manager</div>
      <div class="nav-section" style="margin-top:10px">Configure</div>
      <div class="nav-item" data-page="env" onclick="navTo('env',this)"><span class="nav-icon">⊛</span> Environment</div>
      <div class="nav-item" data-page="settings" onclick="navTo('settings',this)"><span class="nav-icon">⚙</span> Settings</div>
      <div class="nav-item" data-page="resources" onclick="navTo('resources',this)"><span class="nav-icon">▣</span> Resources</div>
    </nav>
    <div class="instances-header">
      <span class="instances-label">Instances</span>
      <span class="instances-count" id="botCount">0</span>
    </div>
    <div class="new-bot-btn" onclick="openCreateModal()">
      <div class="new-bot-btn-plus">+</div>
      Deploy New Instance
    </div>
    <div class="bot-list" id="botList"></div>
    <div class="sidebar-footer">
      <button class="logout-btn" onclick="logout()">Logout</button>
      <span class="sidebar-clock" id="clock">00:00:00</span>
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
          <span class="bc-bot" id="tbBot">— SELECT INSTANCE —</span>
        </div>
      </div>
      <div class="topbar-right">
        <!-- RUNTIME TOGGLE -->
        <div class="runtime-switcher" id="runtimeSwitcher">
          <button class="runtime-btn rt-py active" onclick="setRuntime('py')" title="Python">
            <span class="runtime-icon">🐍</span> PY
          </button>
          <div class="runtime-sep"></div>
          <button class="runtime-btn rt-ts" onclick="setRuntime('ts')" title="TypeScript">
            <span class="runtime-icon">⟨⟩</span> TS
          </button>
          <div class="runtime-sep"></div>
          <button class="runtime-btn rt-rs" onclick="setRuntime('rs')" title="Rust">
            <span class="runtime-icon">⚙</span> RS
          </button>
          <div class="runtime-sep"></div>
          <button class="runtime-btn rt-cs" onclick="setRuntime('cs')" title="C#">
            <span class="runtime-icon">◆</span> C#
          </button>
        </div>
        <div class="host-sep" style="height:20px;margin:0 2px"></div>
        <div class="host-switcher" id="hostSwitcher"></div>
        <div class="host-sep" style="height:20px;margin:0 2px"></div>
        <div class="status-badge offline" id="statusTag">
          <div class="status-led"></div>
          <span id="statusText">OFFLINE</span>
        </div>
        <button class="btn btn-green btn-sm" onclick="startBot()">▶ START</button>
        <button class="btn btn-red btn-sm" onclick="stopBot()">■ STOP</button>
        <button class="btn btn-amber btn-sm" onclick="restartBot()">↺ RESTART</button>
      </div>
    </div>

    <!-- DASHBOARD -->
    <div class="page active" id="page-dashboard">
      <div class="stats-grid">
        <div class="stat-card stat-card-cyan">
          <div class="stat-label">Status</div>
          <div class="stat-value sv-red" id="sStat">OFFLINE</div>
          <div class="stat-sub" id="sStatSub">No active process</div>
        </div>
        <div class="stat-card stat-card-purple">
          <div class="stat-label">Uptime</div>
          <div class="stat-value sv-purple" id="sUptime">—</div>
          <div class="stat-sub">HH:MM:SS</div>
        </div>
        <div class="stat-card stat-card-cyan">
          <div class="stat-label">CPU Usage</div>
          <div class="stat-value sv-cyan" id="sCpu">—</div>
          <div class="stat-sub">System Load</div>
        </div>
        <div class="stat-card stat-card-purple">
          <div class="stat-label">Memory</div>
          <div class="stat-value sv-purple" id="sMem">—</div>
          <div class="stat-sub">RAM Usage</div>
        </div>
      </div>
      <div class="panel">
        <div class="panel-head">
          <div class="panel-title"><div class="panel-icon">▶</div> Launch Control</div>
          <span class="panel-tag">Operations</span>
        </div>
        <div class="panel-body">
          <div style="max-width:420px;margin-bottom:18px">
            <div class="form-group" style="margin:0">
              <label class="form-label">Startup File</label>
              <input class="form-input" id="sfInput" value="main.py" placeholder="main.py / index.ts / main.rs / Program.cs">
            </div>
          </div>
          <div class="btn-row">
            <button class="btn btn-cyan" onclick="startBot()">▶ Start Process</button>
            <button class="btn btn-red" onclick="stopBot()">■ Stop</button>
            <button class="btn btn-amber" onclick="restartBot()">↺ Restart</button>
            <button class="btn btn-ghost" onclick="killBot()" style="margin-left:auto;color:var(--red)">✕ Force Kill</button>
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
          <div class="term-title">stdout // live</div>
        </div>
        <div class="terminal" id="miniTerm" style="height:220px"></div>
      </div>
    </div>

    <!-- CONSOLE -->
    <div class="page" id="page-console">
      <div class="panel">
        <div class="panel-head">
          <div class="panel-title"><div class="panel-icon">_</div> Process Console</div>
          <div class="btn-row">
            <button class="btn btn-ghost btn-sm" onclick="clearConsole()">⊘ Clear</button>
            <button class="btn btn-ghost btn-sm" onclick="exportLogs()">↓ Export</button>
          </div>
        </div>
        <div class="term-chrome">
          <div class="term-dots">
            <div class="term-dot" style="background:#FF5F57"></div>
            <div class="term-dot" style="background:#FFBD2E"></div>
            <div class="term-dot" style="background:#28CA42"></div>
          </div>
          <div class="term-title" id="termTitle">NO INSTANCE SELECTED</div>
        </div>
        <div class="terminal" id="mainTerm" style="height:450px"></div>
        <div class="term-input-wrap">
          <span class="term-prompt">❯</span>
          <input class="term-input" id="termIn" placeholder="Send to stdin…" onkeydown="if(event.key==='Enter')sendInput()">
          <button class="btn btn-cyan btn-sm" onclick="sendInput()">Send</button>
        </div>
      </div>
    </div>

    <!-- FILES -->
    <div class="page" id="page-files">
      <input type="file" multiple id="fileUploadInput" style="display:none" onchange="handleUpload(this.files,false)">
      <input type="file" webkitdirectory directory multiple id="folderUploadInput" style="display:none" onchange="handleUpload(this.files,true)">
      <div class="panel">
        <div class="panel-head">
          <div class="panel-title"><div class="panel-icon">≡</div> File Manager</div>
          <div class="btn-row">
            <button class="btn btn-ghost btn-sm" onclick="openNewFileModal()">+ New File</button>
            <button class="btn btn-cyan btn-sm" onclick="loadFiles()">↻ Refresh</button>
          </div>
        </div>
        <div class="file-table-wrap">
          <table class="file-table">
            <colgroup><col><col style="width:68px"><col style="width:62px"><col style="width:146px"><col style="width:126px"></colgroup>
            <thead>
              <tr><th>Filename</th><th>Type</th><th>Size</th><th>Modified</th><th>Actions</th></tr>
            </thead>
            <tbody id="fileList"></tbody>
          </table>
        </div>
        <div style="padding:14px 16px;border-top:1px solid var(--border)">
          <div class="drop-zone" id="dropZone">
            <span class="drop-icon">⇪</span>
            <div class="drop-title">DROP FILES HERE</div>
            <div class="drop-sub">ZIP auto-extracted · full folder structure preserved</div>
            <div style="display:flex;gap:9px;justify-content:center;flex-wrap:wrap;position:relative;z-index:5">
              <button class="btn btn-cyan btn-sm" onclick="event.stopPropagation();document.getElementById('fileUploadInput').click()">⇪ Upload Files</button>
              <button class="btn btn-purple btn-sm" onclick="event.stopPropagation();document.getElementById('folderUploadInput').click()">📁 Upload Folder</button>
            </div>
          </div>
          <div id="uploadProgress" style="margin-top:8px"></div>
        </div>
      </div>
    </div>

    <!-- ENV -->
    <div class="page" id="page-env">
      <div class="panel">
        <div class="panel-head">
          <div class="panel-title"><div class="panel-icon">⊛</div> Environment Variables</div>
          <button class="btn btn-cyan btn-sm" onclick="saveEnv()">Save Variables</button>
        </div>
        <div class="panel-body">
          <p style="font-family:var(--font-mono);font-size:11px;color:var(--text-2);margin-bottom:18px">Injected into the process environment at startup.</p>
          <div class="env-row" style="margin-bottom:9px">
            <span style="font-family:var(--font-mono);font-size:9px;letter-spacing:3px;color:var(--text-3);text-transform:uppercase">KEY</span>
            <span style="font-family:var(--font-mono);font-size:9px;letter-spacing:3px;color:var(--text-3);text-transform:uppercase">VALUE</span>
            <span></span>
          </div>
          <div id="envRows"></div>
          <button class="btn btn-ghost btn-sm" onclick="addEnvRow('','')" style="margin-top:12px">+ Add Variable</button>
        </div>
      </div>
    </div>

    <!-- SETTINGS -->
    <div class="page" id="page-settings">
      <div class="panel">
        <div class="panel-head">
          <div class="panel-title"><div class="panel-icon">⚙</div> Instance Configuration</div>
        </div>
        <div class="panel-body">
          <div class="form-row-2">
            <div class="form-group"><label class="form-label">Instance Name</label><input class="form-input" id="stName" placeholder="My Server"></div>
            <div class="form-group"><label class="form-label">Startup File</label><input class="form-input" id="stStartup" placeholder="main.py / index.ts / main.rs / Program.cs"></div>
          </div>
          <div class="form-group">
            <label class="form-label">Crash Recovery</label>
            <select class="form-select" id="stAR">
              <option value="false">Disabled</option>
              <option value="true">Auto-restart on crash</option>
            </select>
          </div>
          <button class="btn btn-cyan" onclick="saveSettings()">Save Configuration</button>
          <div class="section-divider" id="accessMgmtTitle">Access Management</div>
          <div id="accessMgmtSection">
            <div class="form-group">
              <label class="form-label">Grant Access</label>
              <div style="display:flex;gap:9px">
                <input class="form-input" id="newSubuser" placeholder="Enter username…">
                <button class="btn btn-purple" onclick="addSubuser()">Grant</button>
              </div>
            </div>
            <div id="subuserList"></div>
          </div>
        </div>
      </div>
      <div class="danger-zone" id="dangerZoneSection">
        <div style="font-family:var(--font-mono);font-size:10px;font-weight:700;letter-spacing:2px;color:var(--red);margin-bottom:7px;text-transform:uppercase">⚠ Danger Zone</div>
        <div style="font-size:13px;color:var(--text-2);margin-bottom:13px">Permanently destroys this instance and all associated files. This cannot be undone.</div>
        <button class="btn btn-red" onclick="deleteBot()">✕ Destroy Instance</button>
      </div>
    </div>

    <!-- RESOURCES -->
    <div class="page" id="page-resources">
      <div class="panel">
        <div class="panel-head">
          <div class="panel-title"><div class="panel-icon">▣</div> System Resources</div>
          <span class="panel-tag" style="color:var(--cyan);border-color:rgba(0,229,255,0.3);background:var(--cyan-dim)">Live Telemetry</span>
        </div>
        <div class="panel-body">
          <div class="res-item">
            <div class="res-header"><span class="res-label">CPU Utilization</span><span class="res-value sv-cyan" id="rCpu">—</span></div>
            <div class="res-track"><div class="res-fill" id="pCpu" style="width:0%;background:linear-gradient(90deg,var(--cyan),var(--blue))"></div></div>
          </div>
          <div class="res-item">
            <div class="res-header"><span class="res-label">Memory Usage</span><span class="res-value sv-purple" id="rMem">—</span></div>
            <div class="res-track"><div class="res-fill" id="pMem" style="width:0%;background:linear-gradient(90deg,var(--purple),var(--cyan))"></div></div>
          </div>
          <div class="res-item">
            <div class="res-header"><span class="res-label">Disk Usage</span><span class="res-value sv-cyan" id="rDsk">—</span></div>
            <div class="res-track"><div class="res-fill" id="pDsk" style="width:0%;background:linear-gradient(90deg,var(--cyan),var(--purple))"></div></div>
          </div>
        </div>
      </div>
    </div>
  </main>

  <!-- MOBILE BOTTOM NAV -->
  <nav class="mobile-bottom-nav">
    <div class="m-nav-item" onclick="toggleSidebar()"><span class="m-nav-icon">▤</span><span class="m-nav-label">Bots</span></div>
    <div class="m-nav-item active" data-page="dashboard" onclick="navTo('dashboard',this)"><span class="m-nav-icon">◈</span><span class="m-nav-label">Dash</span></div>
    <div class="m-nav-item" data-page="console" onclick="navTo('console',this)"><span class="m-nav-icon">_</span><span class="m-nav-label">Term</span></div>
    <div class="m-nav-item" data-page="files" onclick="navTo('files',this)"><span class="m-nav-icon">≡</span><span class="m-nav-label">Files</span></div>
    <div class="m-nav-item" data-page="settings" onclick="navTo('settings',this)"><span class="m-nav-icon">⚙</span><span class="m-nav-label">Config</span></div>
  </nav>
</div>

<div class="toast-tray" id="toastTray"></div>

<!-- CREATE MODAL -->
<div class="modal-veil" id="mCreate">
  <div class="modal-box">
    <div class="modal-title">DEPLOY <span class="modal-title-accent">INSTANCE</span></div>
    <div class="form-group"><label class="form-label">Instance Name</label><input class="form-input" id="mName" placeholder="Project Alpha" onkeydown="if(event.key==='Enter')createBot()"></div>
    <div class="form-group"><label class="form-label">Startup File</label><input class="form-input" id="mFile" value="main.py" placeholder="main.py / index.ts / main.rs / Program.cs" onkeydown="if(event.key==='Enter')createBot()"></div>
    <div class="modal-footer">
      <button class="btn btn-ghost" onclick="closeModal('mCreate')">Cancel</button>
      <button class="btn btn-cyan" onclick="createBot()">Initialize</button>
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
      <button class="btn btn-cyan" onclick="saveFile()">Save Changes</button>
    </div>
  </div>
</div>

<!-- NEW FILE MODAL -->
<div class="modal-veil" id="mNewFile">
  <div class="modal-box">
    <div class="modal-title">CREATE <span class="modal-title-accent">FILE</span></div>
    <div class="form-group"><label class="form-label">Filename</label><input class="form-input" id="nfName" placeholder="src/app.py"></div>
    <div class="form-group"><label class="form-label">Initial Content</label><textarea class="form-textarea" id="nfContent" placeholder="# Start writing…" style="height:140px"></textarea></div>
    <div class="modal-footer">
      <button class="btn btn-ghost" onclick="closeModal('mNewFile')">Cancel</button>
      <button class="btn btn-cyan" onclick="createNewFile()">Create</button>
    </div>
  </div>
</div>

<!-- ADD HOST MODAL -->
<div class="modal-veil" id="mAddHost">
  <div class="modal-box">
    <div class="modal-title">ADD <span class="modal-title-accent">HOST</span></div>
    <div class="form-group">
      <label class="form-label">Host URL</label>
      <input class="form-input" id="ahUrl" placeholder="https://my-vortex-server.com" onkeydown="if(event.key==='Enter')submitAddHost()">
    </div>
    <div class="form-group">
      <label class="form-label">Display Label</label>
      <input class="form-input" id="ahLabel" placeholder="Production" onkeydown="if(event.key==='Enter')submitAddHost()">
    </div>
    <p style="font-family:var(--font-mono);font-size:10px;color:var(--text-3);margin-bottom:4px">Right-click a host button to remove it.</p>
    <div class="modal-footer">
      <button class="btn btn-ghost" onclick="closeModal('mAddHost')">Cancel</button>
      <button class="btn btn-cyan" onclick="submitAddHost()">Add Host</button>
    </div>
  </div>
</div>

<script>
/* ══════════════════════════════════════════════════════
   RUNTIME TOGGLE
══════════════════════════════════════════════════════ */
const RUNTIMES = {
  py: { label: 'Python',     ext: 'py', defaultFile: 'main.py',     icon: '🐍' },
  ts: { label: 'TypeScript', ext: 'ts', defaultFile: 'index.ts',    icon: '⟨⟩' },
  rs: { label: 'Rust',       ext: 'rs', defaultFile: 'src/main.rs', icon: '⚙'  },
  cs: { label: 'C#',         ext: 'cs', defaultFile: 'Program.cs',  icon: '◆'  },
};
let currentRuntime = 'py';

function setRuntime(rt) {
  if (!RUNTIMES[rt]) return;
  currentRuntime = rt;
  // Update toggle button styles
  document.querySelectorAll('.runtime-btn').forEach(b => b.classList.remove('active'));
  const btn = document.querySelector(`.runtime-btn.rt-${rt}`);
  if (btn) btn.classList.add('active');
  // Auto-fill startup file inputs with the default for this runtime
  const def = RUNTIMES[rt].defaultFile;
  const sfInput  = document.getElementById('sfInput');
  const mFile    = document.getElementById('mFile');
  const stStartup= document.getElementById('stStartup');
  if (sfInput)   sfInput.value   = def;
  if (mFile)     mFile.value     = def;
  if (stStartup) stStartup.value = def;
  toast(`Runtime: ${RUNTIMES[rt].label} · ${def}`, 'info');
}

// Detect runtime from a startup file path
function detectRuntime(sf) {
  if (!sf) return;
  const ext = sf.split('.').pop().toLowerCase();
  const map = { py: 'py', ts: 'ts', js: 'ts', rs: 'rs', cs: 'cs', sh: 'py' };
  const rt = map[ext];
  if (rt && rt !== currentRuntime) setRuntime(rt);
}

/* ── globals ── */
const HOSTS = [];
let currentHostUrl = '';

function addHost(url, label) {
  url = url.replace(/\/+$/, '');
  if (HOSTS.find(h => h.url === url)) { toast('Host already added', 'info'); return; }
  HOSTS.push({ url, label: label || url });
  saveHosts(); renderHostSwitcher();
}
function removeHost(url) {
  const idx = HOSTS.findIndex(h => h.url === url);
  if (idx !== -1) HOSTS.splice(idx, 1);
  if (currentHostUrl === url) switchHost('');
  saveHosts(); renderHostSwitcher();
}
function saveHosts() { try { localStorage.setItem('vortex_hosts', JSON.stringify(HOSTS)); } catch(e) {} }
function loadHostsFromStorage() {
  try {
    const raw = localStorage.getItem('vortex_hosts');
    if (raw) { const arr = JSON.parse(raw); arr.forEach(h => { if (!HOSTS.find(x => x.url === h.url)) HOSTS.push(h); }); }
  } catch(e) {}
}
function renderHostSwitcher() {
  const sw = document.getElementById('hostSwitcher'); if (!sw) return;
  sw.innerHTML = '';
  const local = document.createElement('button');
  local.className = 'host-btn' + (currentHostUrl === '' ? ' active' : '');
  local.textContent = 'LOCAL'; local.title = 'This server';
  local.onclick = () => switchHost(''); sw.appendChild(local);
  HOSTS.forEach(h => {
    if (sw.children.length > 1) { const sep = document.createElement('div'); sep.className = 'host-sep'; sw.appendChild(sep); }
    const btn = document.createElement('button');
    btn.className = 'host-btn' + (currentHostUrl === h.url ? ' active' : '');
    btn.textContent = h.label; btn.title = h.url;
    btn.onclick = () => switchHost(h.url);
    btn.oncontextmenu = e => { e.preventDefault(); if (confirm('Remove host "' + h.label + '"?')) removeHost(h.url); };
    sw.appendChild(btn);
  });
  const sep2 = document.createElement('div'); sep2.className = 'host-sep'; sw.appendChild(sep2);
  const addBtn = document.createElement('button');
  addBtn.className = 'host-btn'; addBtn.textContent = '+'; addBtn.title = 'Add remote host';
  addBtn.onclick = () => openAddHostModal(); sw.appendChild(addBtn);
}
function switchHost(url) {
  currentHostUrl = url; renderHostSwitcher();
  curBot = null; botRegistry = {}; startTimes = {};
  document.getElementById('tbBot').textContent = '— SELECT INSTANCE —';
  ['mainTerm','miniTerm'].forEach(i => document.getElementById(i).innerHTML = '');
  applyStatus('offline');
  document.getElementById('botList').innerHTML = '';
  document.getElementById('botCount').textContent = '0';
  loadBots();
  toast(url ? 'Switched to ' + (HOSTS.find(h=>h.url===url)?.label||url) : 'Switched to local', 'info');
}
const _origFetch = window.fetch.bind(window);
async function apiFetch(url, opts = {}) {
  const fullUrl = currentHostUrl ? currentHostUrl + url : url;
  try {
    const r = await _origFetch(fullUrl, opts);
    if (r.status === 401) { document.getElementById('loginOverlay').style.display = 'flex'; return null; }
    return r;
  } catch (e) { toast('Network error' + (currentHostUrl ? ' (' + (HOSTS.find(h=>h.url===currentHostUrl)?.label||currentHostUrl) + ')' : ''), 'error'); return null; }
}

const sock = io({ transports: ['websocket', 'polling'] });
let curBot = null, botRegistry = {}, startTimes = {}, uptimeIv = null, resIv = null;
let currentUser = '', authMode = 'login';
const _renameMap = {};

setInterval(() => document.getElementById('clock').textContent = new Date().toTimeString().slice(0, 8), 1000);

sock.on('connect', () => console.log('[WS] Connected'));
sock.on('console_log', ({ bot_id, msg, level }) => { if (bot_id === curBot) appendLog(msg, level); });
sock.on('status_update', ({ bot_id, status, start_time }) => {
  if (botRegistry[bot_id]) botRegistry[bot_id].status = status;
  renderBotList();
  if (bot_id === curBot) applyStatus(status);
  if (status === 'online' && start_time) { startTimes[bot_id] = start_time * 1000; startUptime(); }
  else delete startTimes[bot_id];
});

/* ── sidebar ── */
function toggleSidebar() {
  const sb = document.getElementById('sidebar'), o = document.getElementById('sidebarOverlay');
  const open = sb.classList.toggle('open');
  if (open) { o.style.display = 'block'; void o.offsetWidth; o.classList.add('open'); }
  else { o.classList.remove('open'); setTimeout(() => o.style.display = 'none', 300); }
}

/* ── navigation ── */
const PAGE_NAMES = { dashboard:'DASHBOARD', console:'CONSOLE', files:'FILE MANAGER', env:'ENVIRONMENT', settings:'SETTINGS', resources:'RESOURCES' };
function navTo(name, el) {
  document.querySelectorAll('.sidebar .nav-item').forEach(n => n.classList.remove('active'));
  const d = document.querySelector(`.sidebar .nav-item[data-page="${name}"]`); if (d) d.classList.add('active');
  document.querySelectorAll('.mobile-bottom-nav .m-nav-item').forEach(n => n.classList.remove('active'));
  const m = document.querySelector(`.mobile-bottom-nav .m-nav-item[data-page="${name}"]`); if (m) m.classList.add('active');
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  const p = document.getElementById('page-' + name); if (p) p.classList.add('active');
  document.getElementById('tbPage').textContent = PAGE_NAMES[name] || name.toUpperCase();
  if (name === 'files') loadFiles();
  if (name === 'env') loadEnv();
  if (name === 'settings') loadSettings();
  if (name === 'resources') startRes(); else stopRes();
}

/* ── auth ── */
async function checkAuth() {
  const r = await fetch('/api/me');
  if (r.status === 401) { document.getElementById('loginOverlay').style.display = 'flex'; return false; }
  const d = await r.json(); currentUser = d.username;
  document.getElementById('loginOverlay').style.display = 'none'; return true;
}
function switchAuthMode(mode) {
  authMode = mode;
  document.getElementById('tabLogin').classList.toggle('active', mode === 'login');
  document.getElementById('tabRegister').classList.toggle('active', mode === 'register');
  document.getElementById('authBtn').textContent = mode === 'login' ? 'AUTHENTICATE' : 'CREATE ACCOUNT';
}
async function submitAuth() {
  const u = document.getElementById('authUsername').value.trim(), p = document.getElementById('authPassword').value;
  if (!u || !p) { toast('Credentials required', 'error'); return; }
  const ep = authMode === 'login' ? '/api/login' : '/api/register';
  const r = await fetch(ep, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({username:u,password:p}) });
  const res = await r.json();
  if (r.ok) location.reload(); else toast(res.error || 'Authentication failed', 'error');
}
async function logout() { await fetch('/api/logout', { method:'POST' }); location.reload(); }

/* ── bot list ── */
async function loadBots() {
  const r = await apiFetch('/api/bots'); if (!r) return;
  botRegistry = await r.json();
  Object.entries(botRegistry).forEach(([id, b]) => { if (b.status === 'online' && b.start_time) startTimes[id] = b.start_time * 1000; });
  renderBotList();
  document.getElementById('botCount').textContent = Object.keys(botRegistry).length;
  if (Object.keys(botRegistry).length > 0 && !curBot) selectBot(Object.keys(botRegistry)[0]);
}
function renderBotList() {
  const el = document.getElementById('botList'); el.innerHTML = '';
  const entries = Object.entries(botRegistry);
  if (!entries.length) {
    el.innerHTML = '<div style="padding:18px;text-align:center;color:var(--text-3);font-family:var(--font-mono);font-size:10px;letter-spacing:2px">NO INSTANCES</div>'; return;
  }
  entries.forEach(([id, b]) => {
    const d = document.createElement('div');
    d.className = 'bot-item' + (id === curBot ? ' active' : '');
    const s = b.status || 'offline', sh = b.is_shared ? '<span class="bot-shared-tag">shared</span>' : '';
    d.innerHTML = `<div class="bot-dot ${s}"></div><div style="flex:1;min-width:0"><div class="bot-name">${escH(b.name || id)}</div><div class="bot-status ${s}">${s}</div></div>${sh}`;
    d.onclick = () => { selectBot(id); if (window.innerWidth <= 860 && document.getElementById('sidebar').classList.contains('open')) toggleSidebar(); };
    el.appendChild(d);
  });
}
function selectBot(id) {
  curBot = id;
  const b = botRegistry[id];
  document.getElementById('tbBot').textContent = b?.name || id;
  const sf = b?.startup_file || 'main.py';
  document.getElementById('sfInput').value = sf;
  document.getElementById('termTitle').textContent = (b?.name || id).toUpperCase() + ' // STDOUT';
  ['mainTerm','miniTerm'].forEach(i => document.getElementById(i).innerHTML = '');
  applyStatus(b?.status || 'offline');
  renderBotList();
  loadBotLogs();
  startUptime();
  // Auto-detect runtime from startup file
  detectRuntime(sf);
  const activePage = document.querySelector('.page.active');
  if (activePage) {
    const pg = activePage.id.replace('page-', '');
    if (pg === 'files') loadFiles();
    if (pg === 'env') loadEnv();
    if (pg === 'settings') loadSettings();
  }
  if (b && b.is_shared) {
    document.getElementById('accessMgmtTitle').style.display = 'none';
    document.getElementById('accessMgmtSection').style.display = 'none';
    document.getElementById('dangerZoneSection').style.display = 'none';
  } else {
    document.getElementById('accessMgmtTitle').style.display = '';
    document.getElementById('accessMgmtSection').style.display = '';
    document.getElementById('dangerZoneSection').style.display = '';
  }
}
async function loadBotLogs() {
  if (!curBot) return;
  const r = await apiFetch(`/api/bot/${curBot}/logs`); if (!r) return;
  const logs = await r.json();
  ['mainTerm','miniTerm'].forEach(id => document.getElementById(id).innerHTML = '');
  logs.forEach(({ msg, level, time: ts }) => appendLog(msg, level, ts));
}
function applyStatus(s) {
  const on = s === 'online';
  document.getElementById('statusTag').className = 'status-badge ' + (on ? 'online' : 'offline');
  document.getElementById('statusText').textContent = on ? 'ONLINE' : 'OFFLINE';
  document.getElementById('sStat').textContent = on ? 'ONLINE' : 'OFFLINE';
  document.getElementById('sStat').className = 'stat-value ' + (on ? 'sv-green' : 'sv-red');
  document.getElementById('sStatSub').textContent = on ? 'Process running' : 'Process halted';
  if (!on) document.getElementById('sUptime').textContent = '—';
}

/* ── modals ── */
function openCreateModal() {
  // Pre-fill modal with current runtime defaults
  document.getElementById('mFile').value = RUNTIMES[currentRuntime]?.defaultFile || 'main.py';
  document.getElementById('mCreate').classList.add('open');
  setTimeout(() => document.getElementById('mName').focus(), 80);
}
function closeModal(id) { document.getElementById(id).classList.remove('open'); }
document.querySelectorAll('.modal-veil').forEach(m => m.addEventListener('click', e => { if (e.target === m) m.classList.remove('open'); }));

/* ── bot actions ── */
async function createBot() {
  const n = document.getElementById('mName').value.trim();
  const f = document.getElementById('mFile').value.trim() || RUNTIMES[currentRuntime]?.defaultFile || 'main.py';
  if (!n) { toast('Instance name required', 'error'); return; }
  const r = await apiFetch('/api/bots', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({name:n,startup_file:f}) });
  if (!r) return;
  if (!r.ok) { const e = await r.json(); toast(e.error || 'Failed', 'error'); return; }
  const b = await r.json();
  botRegistry[b.id] = b; closeModal('mCreate');
  document.getElementById('mName').value = '';
  renderBotList();
  document.getElementById('botCount').textContent = Object.keys(botRegistry).length;
  selectBot(b.id); toast('Instance deployed', 'success');
}
async function startBot() {
  if (!curBot) { toast('Select an instance first', 'error'); return; }
  const sf = document.getElementById('sfInput').value.trim() || RUNTIMES[currentRuntime]?.defaultFile || 'main.py';
  const r = await apiFetch(`/api/bot/${curBot}/start`, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({startup_file:sf}) });
  if (r && !r.ok) { const e = await r.json(); toast(e.error || 'Start failed', 'error'); return; }
  toast('Booting…', 'info');
}
async function stopBot() {
  if (!curBot) { toast('Select an instance first', 'error'); return; }
  await apiFetch(`/api/bot/${curBot}/stop`, { method:'POST' }); toast('Stopped', 'success');
}
async function restartBot() {
  if (!curBot) { toast('Select an instance first', 'error'); return; }
  const r = await apiFetch(`/api/bot/${curBot}/stop`, { method:'POST' });
  if (r) { toast('Rebooting…', 'info'); setTimeout(startBot, 1200); }
}
async function killBot() {
  if (!curBot) return;
  await apiFetch(`/api/bot/${curBot}/kill`, { method:'POST' }); toast('Force killed', 'error');
}
async function deleteBot() {
  if (!curBot || !confirm('Permanently destroy this instance and all its files?')) return;
  const r = await apiFetch(`/api/bot/${curBot}`, { method:'DELETE' });
  if (!r || !r.ok) { toast('Delete failed', 'error'); return; }
  delete botRegistry[curBot]; curBot = null;
  document.getElementById('tbBot').textContent = '— SELECT INSTANCE —';
  ['mainTerm','miniTerm'].forEach(i => document.getElementById(i).innerHTML = '');
  applyStatus('offline'); renderBotList();
  document.getElementById('botCount').textContent = Object.keys(botRegistry).length;
  toast('Instance destroyed', 'error');
}

/* ── console ── */
function appendLog(msg, level, ts) {
  const tagMap = { system:'sys', error:'err', success:'ok', warn:'warn', default:'out' };
  const tag = tagMap[level] || 'out', t = ts || new Date().toTimeString().slice(0, 8);
  const row = `<div class="log-row"><span class="log-ts">${escH(t)}</span><span class="log-tag ${tag}">${tag}</span><span class="log-msg ${tag}">${escH(msg)}</span></div>`;
  ['mainTerm','miniTerm'].forEach(id => { const el = document.getElementById(id); if (el) { el.innerHTML += row; el.scrollTop = el.scrollHeight; } });
}
function clearConsole() { ['mainTerm','miniTerm'].forEach(id => document.getElementById(id).innerHTML = ''); toast('Console cleared', 'info'); }
function exportLogs() {
  const lines = Array.from(document.getElementById('mainTerm').querySelectorAll('.log-row')).map(r => r.textContent.trim()).join('\n');
  const a = document.createElement('a'); a.href = 'data:text/plain;charset=utf-8,' + encodeURIComponent(lines);
  a.download = `${curBot || 'vortex'}-${Date.now()}.log`; a.click(); toast('Logs exported', 'success');
}
async function sendInput() {
  if (!curBot) return;
  let v = document.getElementById('termIn').value; document.getElementById('termIn').value = '';
  v = v.replace(/[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f]/g, '');
  if (!v.trim()) return;
  await apiFetch(`/api/bot/${curBot}/input`, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({input:v+'\n'}) });
}

/* ══════════════════════════════════════════════════════
   FILE MANAGER
══════════════════════════════════════════════════════ */
const EXT_COLORS = { py:'#00FF7F', js:'#FFB800', json:'#00E5FF', md:'#A020F0', txt:'#5A7A9A', sh:'#00E5FF', zip:'#FF2055', env:'#FFB800', ts:'#3178C6', html:'#FF7043', css:'#00BCD4', jsx:'#61DAFB', tsx:'#61DAFB', rs:'#F74C00', cs:'#9B4FCA', toml:'#FFB800', csproj:'#9B4FCA', lock:'#5A7A9A', cargo:'#F74C00' };
const EXT_ICONS  = { py:'🐍', js:'⚡', jsx:'⚛', tsx:'⚛', ts:'⟨⟩', json:'{}', txt:'≡', md:'#', zip:'⊞', env:'⊛', sh:'$', html:'<>', css:'◐', rs:'⚙', cs:'◆', toml:'⚙', csproj:'◆' };

async function loadFiles() {
  const tb = document.getElementById('fileList');
  if (!curBot) { tb.innerHTML = `<tr><td colspan="5" style="padding:36px;text-align:center;color:var(--text-3);font-family:var(--font-mono);font-size:11px">NO INSTANCE TARGETED</td></tr>`; return; }
  tb.innerHTML = `<tr><td colspan="5" style="padding:36px;text-align:center;color:var(--text-3);font-family:var(--font-mono);font-size:11px">Loading…</td></tr>`;
  const r = await apiFetch(`/api/bot/${curBot}/files`); if (!r) return;
  const files = await r.json();
  if (!files.length) { tb.innerHTML = `<tr><td colspan="5" style="padding:36px;text-align:center;color:var(--text-3);font-family:var(--font-mono);font-size:11px">DIRECTORY EMPTY</td></tr>`; return; }
  for (const k in _renameMap) delete _renameMap[k];
  tb.innerHTML = '';
  files.forEach(f => {
    const ext = (f.name.split('.').pop() || '').toLowerCase();
    const c = EXT_COLORS[ext] || '#5A7A9A', ic = EXT_ICONS[ext] || '□';
    const rid = 'r' + Math.random().toString(36).slice(2, 10);
    _renameMap[rid] = f.name;
    const tr = document.createElement('tr');
    // name cell
    const tdName = document.createElement('td');
    const fnCell = document.createElement('div'); fnCell.className = 'fn-cell';
    const fnIcon = document.createElement('span'); fnIcon.className = 'fn-icon'; fnIcon.textContent = ic;
    const fnLink = document.createElement('span');
    fnLink.className = 'fn-link'; fnLink.id = 'fnl_' + rid;
    const displayName = f.name.split('/').pop();
    fnLink.title = f.name; fnLink.textContent = displayName; fnLink.onclick = () => editFile(f.name);
    const fnRename = document.createElement('div'); fnRename.className = 'fn-rename'; fnRename.id = 'fnr_' + rid;
    const fnInput = document.createElement('input'); fnInput.className = 'fn-rename-input'; fnInput.id = 'fni_' + rid; fnInput.value = displayName;
    fnInput.onkeydown = e => { if (e.key === 'Enter') doRename(rid); if (e.key === 'Escape') cancelRename(rid); };
    const fnOk = document.createElement('button'); fnOk.className = 'fn-rename-ok'; fnOk.textContent = '✓'; fnOk.onclick = () => doRename(rid);
    const fnCancel = document.createElement('button'); fnCancel.className = 'fn-rename-cancel'; fnCancel.textContent = '✕'; fnCancel.onclick = () => cancelRename(rid);
    fnRename.append(fnInput, fnOk, fnCancel);
    fnCell.append(fnIcon, fnLink, fnRename);
    tdName.appendChild(fnCell);
    // type cell
    const tdType = document.createElement('td');
    const badge = document.createElement('span'); badge.className = 'file-ext-badge';
    badge.style.cssText = `color:${c};border-color:${c}33`; badge.textContent = '.' + (ext || '—'); tdType.appendChild(badge);
    // size cell
    const tdSize = document.createElement('td'); tdSize.style.color = 'var(--text-2)'; tdSize.textContent = f.size;
    // modified cell
    const tdMod = document.createElement('td'); tdMod.style.cssText = 'color:var(--text-3);font-size:11px'; tdMod.textContent = f.modified;
    // actions cell
    const tdAct = document.createElement('td');
    const actDiv = document.createElement('div'); actDiv.className = 'file-actions';
    const btnEdit = document.createElement('button'); btnEdit.className = 'icon-btn ib-cyan'; btnEdit.title = 'Edit'; btnEdit.textContent = '✏'; btnEdit.onclick = () => editFile(f.name);
    const btnRename = document.createElement('button'); btnRename.className = 'icon-btn ib-amber'; btnRename.id = 'rnb_' + rid; btnRename.title = 'Rename'; btnRename.textContent = '⟳'; btnRename.onclick = () => toggleRename(rid);
    const btnDl = document.createElement('button'); btnDl.className = 'icon-btn'; btnDl.title = 'Download'; btnDl.textContent = '↓'; btnDl.onclick = () => dlFile(f.name);
    const btnDel = document.createElement('button'); btnDel.className = 'icon-btn ib-red'; btnDel.title = 'Delete'; btnDel.textContent = '✕'; btnDel.onclick = () => delFile(f.name);
    actDiv.append(btnEdit, btnRename, btnDl, btnDel); tdAct.appendChild(actDiv);
    tr.append(tdName, tdType, tdSize, tdMod, tdAct); tb.appendChild(tr);
  });
}

function toggleRename(rid) {
  const lnk = document.getElementById('fnl_' + rid), rnw = document.getElementById('fnr_' + rid), btn = document.getElementById('rnb_' + rid);
  if (!lnk || !rnw || !btn) return;
  if (rnw.classList.contains('on')) { cancelRename(rid); } else {
    lnk.style.display = 'none'; rnw.classList.add('on'); btn.textContent = '✕'; btn.title = 'Cancel';
    const inp = document.getElementById('fni_' + rid);
    if (inp) { inp.focus(); const v = inp.value, dot = v.lastIndexOf('.'); inp.setSelectionRange(0, dot > 0 ? dot : v.length); }
  }
}
function cancelRename(rid) {
  const lnk = document.getElementById('fnl_' + rid), rnw = document.getElementById('fnr_' + rid), btn = document.getElementById('rnb_' + rid);
  if (lnk) lnk.style.display = ''; if (rnw) rnw.classList.remove('on'); if (btn) { btn.textContent = '⟳'; btn.title = 'Rename'; }
}
async function doRename(rid) {
  const oldName = _renameMap[rid];
  if (!oldName) { toast('Rename context lost — please refresh', 'error'); return; }
  const inp = document.getElementById('fni_' + rid); if (!inp) return;
  const newBase = inp.value.trim(); if (!newBase) { toast('Name cannot be empty', 'error'); return; }
  const parts = oldName.split('/'); parts[parts.length - 1] = newBase;
  const newName = parts.join('/');
  if (newName === oldName) { cancelRename(rid); return; }
  const r = await apiFetch(`/api/bot/${curBot}/file/${encodeURIComponent(oldName)}/rename`, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({new_name:newName}) });
  if (!r) return;
  const res = await r.json();
  if (res.error) { toast(res.error, 'error'); return; }
  toast(`Renamed → ${newBase}`, 'success'); loadFiles();
}

async function editFile(name) {
  if (!curBot) return;
  const r = await apiFetch(`/api/bot/${curBot}/file/${encodeURIComponent(name)}`); if (!r) return;
  const d = await r.json();
  document.getElementById('edName').textContent = name;
  const content = d.content;
  document.getElementById('edContent').value = content === '[Binary — cannot display]' ? '' : content;
  document.getElementById('edContent').dataset.fn = name;
  if (content === '[Binary — cannot display]') toast('Binary file — showing empty editor. Save will overwrite.', 'info');
  document.getElementById('mEditor').classList.add('open');
}
async function saveFile() {
  const name = document.getElementById('edContent').dataset.fn; if (!name) return;
  const r = await apiFetch(`/api/bot/${curBot}/file/${encodeURIComponent(name)}`, { method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify({content:document.getElementById('edContent').value}) });
  if (!r || !r.ok) { toast('Save failed', 'error'); return; }
  closeModal('mEditor'); loadFiles(); toast(`${name} saved`, 'success');
}
function openNewFileModal() { if (!curBot) { toast('Select an instance first', 'error'); return; } document.getElementById('mNewFile').classList.add('open'); }
async function createNewFile() {
  const name = document.getElementById('nfName').value.trim(); if (!name) { toast('Filename required', 'error'); return; }
  const r = await apiFetch(`/api/bot/${curBot}/file/${encodeURIComponent(name)}`, { method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify({content:document.getElementById('nfContent').value}) });
  if (!r || !r.ok) { toast('Create failed', 'error'); return; }
  closeModal('mNewFile'); document.getElementById('nfName').value = ''; document.getElementById('nfContent').value = '';
  loadFiles(); toast('File created', 'success');
}
async function delFile(name) {
  if (!confirm(`Delete "${name}"? This cannot be undone.`)) return;
  const r = await apiFetch(`/api/bot/${curBot}/file/${encodeURIComponent(name)}`, { method:'DELETE' });
  if (!r || !r.ok) { toast('Delete failed', 'error'); return; }
  loadFiles(); toast('File deleted', 'success');
}
function dlFile(name) { window.location.href = `/api/bot/${curBot}/file/${encodeURIComponent(name)}/download`; }

/* ══════════════════════════════════════════════════════
   DRAG & DROP / UPLOAD
══════════════════════════════════════════════════════ */
async function handleUpload(files, isFolder) {
  const fileArr = files ? Array.from(files) : [];
  try { document.getElementById('fileUploadInput').value = ''; } catch (e) {}
  try { document.getElementById('folderUploadInput').value = ''; } catch (e) {}
  if (!curBot) { toast('Select an instance first', 'error'); return; }
  if (!fileArr.length) { toast('No files selected', 'error'); return; }
  const prog = document.getElementById('uploadProgress');
  let ok = 0, fail = 0;
  for (const file of fileArr) {
    const relPath = (isFolder && file.webkitRelativePath) ? file.webkitRelativePath : file.name;
    const fd = new FormData();
    fd.append('file', file); fd.append('relative_path', relPath); fd.append('is_folder_upload', isFolder ? '1' : '0');
    const sid = 'up_' + Math.random().toString(36).slice(2);
    const wrap = document.createElement('div'); wrap.className = 'upload-row';
    const shortName = relPath.length > 50 ? '…' + relPath.slice(-47) : relPath;
    wrap.innerHTML = `<span style="color:var(--cyan);flex-shrink:0">⇪</span><span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:11px" title="${escH(relPath)}">${escH(shortName)}</span><div class="upload-bar-wrap"><div class="upload-bar-fill" id="${sid}" style="width:0%"></div></div><span id="${sid}st" style="font-size:10px;color:var(--text-3);flex-shrink:0;min-width:26px;text-align:right">0%</span>`;
    prog.appendChild(wrap);
    try {
      await new Promise((resolve, reject) => {
        const xhr = new XMLHttpRequest();
        xhr.open('POST', `/api/bot/${curBot}/upload`);
        xhr.upload.addEventListener('progress', e => {
          if (e.lengthComputable) {
            const pct = Math.round(e.loaded / e.total * 95);
            const b = document.getElementById(sid), s = document.getElementById(sid + 'st');
            if (b) b.style.width = pct + '%'; if (s) s.textContent = pct + '%';
          }
        });
        xhr.addEventListener('load', () => {
          if (xhr.status === 401) { document.getElementById('loginOverlay').style.display = 'flex'; reject(new Error('Unauthorized')); return; }
          let resp = {}; try { resp = JSON.parse(xhr.responseText); } catch (e) {}
          if (resp.error) { reject(new Error(resp.error)); return; }
          if (xhr.status >= 200 && xhr.status < 300) resolve(); else reject(new Error(`HTTP ${xhr.status}`));
        });
        xhr.addEventListener('error', () => reject(new Error('Network error')));
        xhr.addEventListener('abort', () => reject(new Error('Aborted')));
        xhr.send(fd);
      });
      const b = document.getElementById(sid), s = document.getElementById(sid + 'st');
      if (b) { b.style.width = '100%'; b.style.background = 'var(--green)'; }
      if (s) { s.textContent = '✓'; s.style.color = 'var(--green)'; }
      ok++; setTimeout(() => wrap.remove(), 2200);
    } catch (err) {
      const b = document.getElementById(sid), s = document.getElementById(sid + 'st');
      if (b) { b.style.width = '100%'; b.style.background = 'var(--red)'; }
      if (s) { s.textContent = '✕'; s.style.color = 'var(--red)'; }
      wrap.style.borderColor = 'rgba(255,32,85,0.3)'; fail++;
      toast(`Upload failed: ${err.message}`, 'error'); setTimeout(() => wrap.remove(), 4000);
    }
  }
  loadFiles();
  if (ok > 0 && fail === 0) toast(`${ok} file${ok > 1 ? 's' : ''} uploaded`, 'success');
  else if (ok > 0 && fail > 0) toast(`${ok} uploaded, ${fail} failed`, 'info');
}

let _dzDepth = 0;
document.addEventListener('dragenter', e => { e.preventDefault(); _dzDepth++; document.getElementById('dropZone')?.classList.add('dragging'); });
document.addEventListener('dragleave', e => { if (--_dzDepth <= 0) { _dzDepth = 0; document.getElementById('dropZone')?.classList.remove('dragging'); } });
document.addEventListener('dragover', e => e.preventDefault());
document.addEventListener('drop', e => {
  e.preventDefault(); _dzDepth = 0; document.getElementById('dropZone')?.classList.remove('dragging');
  if (e.dataTransfer.files.length) handleUpload(e.dataTransfer.files, false);
});

/* ── env ── */
async function loadEnv() {
  if (!curBot) return;
  const r = await apiFetch(`/api/bot/${curBot}/env`); if (!r) return;
  const env = await r.json(); const c = document.getElementById('envRows'); c.innerHTML = '';
  const entries = Object.entries(env);
  if (entries.length) entries.forEach(([k, v]) => addEnvRow(k, v)); else addEnvRow('', '');
}
function addEnvRow(k = '', v = '') {
  const d = document.createElement('div'); d.className = 'env-row';
  const kInput = document.createElement('input'); kInput.className = 'env-field env-key'; kInput.placeholder = 'KEY'; kInput.value = k;
  const vInput = document.createElement('input'); vInput.className = 'env-field'; vInput.placeholder = 'value'; vInput.value = v;
  const delBtn = document.createElement('button'); delBtn.className = 'icon-btn ib-red'; delBtn.textContent = '✕';
  delBtn.style.cssText = 'width:28px;height:28px'; delBtn.onclick = () => d.remove();
  d.append(kInput, vInput, delBtn); document.getElementById('envRows').appendChild(d);
}
async function saveEnv() {
  if (!curBot) return;
  const env = {};
  document.querySelectorAll('.env-row').forEach(r => {
    const inputs = r.querySelectorAll('.env-field');
    const k = inputs[0]?.value.trim(), v = inputs[1]?.value;
    if (k) env[k] = v || '';
  });
  const r = await apiFetch(`/api/bot/${curBot}/env`, { method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify(env) });
  if (!r || !r.ok) { toast('Save failed', 'error'); return; }
  toast('Environment saved', 'success');
}

/* ── settings ── */
async function loadSettings() {
  if (!curBot) return;
  const b = botRegistry[curBot] || {};
  document.getElementById('stName').value = b.name || '';
  document.getElementById('stStartup').value = b.startup_file || RUNTIMES[currentRuntime]?.defaultFile || 'main.py';
  document.getElementById('stAR').value = b.auto_restart ? 'true' : 'false';
  if (!b.is_shared) {
    const r = await apiFetch(`/api/bot/${curBot}/subusers`);
    if (r) {
      const users = await r.json(); const c = document.getElementById('subuserList'); c.innerHTML = '';
      users.forEach(u => {
        const div = document.createElement('div'); div.className = 'subuser-row';
        const nameSpan = document.createElement('span'); nameSpan.style.color = 'var(--text-2)'; nameSpan.textContent = u;
        const delBtn = document.createElement('button'); delBtn.className = 'icon-btn ib-red';
        delBtn.style.cssText = 'width:28px;height:28px;font-size:11px'; delBtn.textContent = '✕';
        delBtn.onclick = () => removeSubuser(u);
        div.append(nameSpan, delBtn); c.appendChild(div);
      });
    }
  }
}
async function saveSettings() {
  if (!curBot) return;
  const data = { name: document.getElementById('stName').value.trim(), startup_file: document.getElementById('stStartup').value.trim() || RUNTIMES[currentRuntime]?.defaultFile || 'main.py', auto_restart: document.getElementById('stAR').value === 'true' };
  const r = await apiFetch(`/api/bot/${curBot}/settings`, { method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify(data) });
  if (!r) return;
  if (!r.ok) { const e = await r.json(); toast(e.error || 'Save failed', 'error'); return; }
  const upd = await r.json();
  botRegistry[curBot] = { ...botRegistry[curBot], ...upd };
  document.getElementById('tbBot').textContent = data.name || curBot;
  document.getElementById('sfInput').value = data.startup_file;
  renderBotList(); toast('Settings saved', 'success');
}
async function addSubuser() {
  if (!curBot) return;
  const u = document.getElementById('newSubuser').value.trim(); if (!u) return;
  const r = await apiFetch(`/api/bot/${curBot}/subusers`, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({username:u}) });
  if (r && r.ok) { document.getElementById('newSubuser').value = ''; loadSettings(); toast(`Access granted to ${u}`, 'success'); }
  else toast('User not found', 'error');
}
async function removeSubuser(u) {
  if (!curBot) return;
  const r = await apiFetch(`/api/bot/${curBot}/subusers/${encodeURIComponent(u)}`, { method:'DELETE' });
  if (r && r.ok) { loadSettings(); toast(`Access revoked for ${u}`, 'success'); }
}

/* ── uptime ── */
function startUptime() {
  clearInterval(uptimeIv);
  uptimeIv = setInterval(() => {
    if (curBot && startTimes[curBot]) {
      const s = Math.floor((Date.now() - startTimes[curBot]) / 1000);
      const h = Math.floor(s / 3600), m = Math.floor(s % 3600 / 60), sec = s % 60;
      document.getElementById('sUptime').textContent = `${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}:${String(sec).padStart(2,'0')}`;
    }
  }, 1000);
}

/* ── resources ── */
function startRes() { stopRes(); fetchRes(); resIv = setInterval(fetchRes, 4000); }
function stopRes() { clearInterval(resIv); }
async function fetchRes() {
  try {
    const r = await fetch('/api/resources'); if (!r.ok) return;
    const d = await r.json();
    document.getElementById('rCpu').textContent = d.cpu + '%'; document.getElementById('pCpu').style.width = d.cpu + '%';
    document.getElementById('rMem').textContent = d.mem_used; document.getElementById('pMem').style.width = d.mem_pct + '%';
    document.getElementById('rDsk').textContent = d.disk_pct + '%'; document.getElementById('pDsk').style.width = d.disk_pct + '%';
    document.getElementById('sCpu').textContent = d.cpu + '%'; document.getElementById('sMem').textContent = d.mem_used;
  } catch (e) {}
}

/* ── utils ── */
function escH(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;'); }
function toast(msg, type = 'success') {
  const tray = document.getElementById('toastTray');
  const icons = { success:'✓', error:'✕', info:'i' };
  const t = document.createElement('div'); t.className = `toast ${type}`;
  t.innerHTML = `<div class="toast-icon">${icons[type] || '·'}</div><span>${escH(msg)}</span><span class="toast-close" onclick="this.parentElement.remove()">✕</span>`;
  tray.appendChild(t);
  setTimeout(() => { t.style.transition = 'all .35s'; t.style.opacity = '0'; t.style.transform = 'translateX(18px)'; setTimeout(() => t.remove(), 350); }, 3500);
}

/* ── add host modal ── */
function openAddHostModal() {
  document.getElementById('ahUrl').value = ''; document.getElementById('ahLabel').value = '';
  document.getElementById('mAddHost').classList.add('open'); setTimeout(() => document.getElementById('ahUrl').focus(), 80);
}
function submitAddHost() {
  let url = document.getElementById('ahUrl').value.trim(), lbl = document.getElementById('ahLabel').value.trim();
  if (!url) { toast('Host URL required', 'error'); return; }
  if (!/^https?:\/\//.test(url)) url = 'http://' + url;
  addHost(url, lbl || url.replace(/^https?:\/\//, '')); closeModal('mAddHost'); toast('Host added', 'success');
}

/* ── boot ── */
checkAuth().then(ok => {
  if (ok) { loadHostsFromStorage(); renderHostSwitcher(); loadBots(); fetchRes(); setInterval(fetchRes, 5000); }
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

@app.route('/')
def index():
    return render_template_string(HTML)

# ── auth ──────────────────────────────────────────────────────────────────────

@app.route('/api/login', methods=['POST'])
def login():
    data = request.json or {}
    username = data.get('username', '').strip()
    password = data.get('password', '')
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
    return jsonify({'ok': True})

@app.route('/api/register', methods=['POST'])
def register():
    data = request.json or {}
    username = data.get('username', '').strip()
    password = data.get('password', '')
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
        if p.poll() is None and p.stdin:
            try:
                p.stdin.write(inp); p.stdin.flush()
            except Exception:
                pass
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
            sz  = os.path.getsize(fp)
            s   = f"{sz}B" if sz < 1024 else f"{sz // 1024}KB" if sz < 1024 ** 2 else f"{sz // 1024 // 1024}MB"
            out.append({'name': rel, 'size': s,
                'modified': time.strftime('%Y-%m-%d %H:%M', time.localtime(os.path.getmtime(fp)))})
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

    # Normalize separators
    normalized = raw_relative.replace('\\', '/')
    parts = [p for p in normalized.split('/') if p and p not in ('.', '..')]

    # For folder uploads, browser sends "FolderName/sub/file.ext" — strip top-level folder
    if is_folder_up and len(parts) > 1:
        parts = parts[1:]

    # Sanitize each path component individually to preserve structure
    safe_parts = []
    for p in parts:
        # Use secure_filename but fall back to manual clean for path components
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

    # Final traversal check
    if dest != abs_bd and not dest.startswith(abs_bd + os.sep):
        return jsonify({'error': 'path traversal blocked'}), 403

    fname = safe_parts[-1]

    # ── Handle ZIP: extract preserving full internal folder structure ──────────
    if fname.lower().endswith('.zip'):
        # Save zip temporarily
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
                # Check for password protection
                for info in zf.infolist():
                    if info.flag_bits & 0x1:
                        emit_log(bid, '[Error] ZIP is password-protected', 'error')
                        os.remove(tmp_zip)
                        return jsonify({'error': 'password-protected zip'}), 400

                # Detect if ALL entries share a single top-level folder (e.g. "repo-main/")
                # GitHub/common ZIPs always do this. Strip it so files land in bot root.
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
                    # Normalize the member name
                    member_path = member.filename.replace('\\', '/')

                    # Skip pure directory entries
                    if member_path.endswith('/'):
                        continue

                    # Split into path components
                    m_parts = [p for p in member_path.split('/') if p and p not in ('.', '..')]
                    if not m_parts:
                        continue

                    # Strip shared top-level folder so files go straight into bot dir
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

                    # Extract directly into bot directory (sub-folders preserved)
                    dest_file = os.path.abspath(os.path.join(abs_bd, *safe_m_parts))

                    # Zip-slip check: must remain inside bot directory
                    if not dest_file.startswith(abs_bd + os.sep) and dest_file != abs_bd:
                        log.warning(f'Blocked zip-slip: {member.filename}')
                        blocked += 1
                        continue

                    # Create parent directories (preserving folder structure)
                    dest_file_dir = os.path.dirname(dest_file)
                    if dest_file_dir:
                        os.makedirs(dest_file_dir, exist_ok=True)

                    # Extract the file
                    with zf.open(member) as src_f, open(dest_file, 'wb') as dst_f:
                        dst_f.write(src_f.read())

                    extracted += 1

            os.remove(tmp_zip)
            msg = f'[System] Extracted {extracted} file(s) from {fname}'
            if blocked:
                msg += f' ({blocked} path(s) blocked)'
            emit_log(bid, msg, 'success' if extracted else 'warn')

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
        # Regular file upload — create parent directories as needed
        try:
            parent = os.path.dirname(dest)
            if parent and parent != abs_bd:
                os.makedirs(parent, exist_ok=True)
            file.save(dest)
            emit_log(bid, f'[System] Uploaded {rel_path}', 'system')
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
        'cpu': round(cpu, 1),
        'mem_used': fmt(mem.used), 'mem_total': fmt(mem.total), 'mem_pct': round(mem.percent, 1),
        'disk_used': fmt(disk.used), 'disk_total': fmt(disk.total), 'disk_pct': round(disk.percent, 1),
    })


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    print('\n' + '━' * 56)
    print(f'  VORTEX HOSTING v11.5  ·  mode={_ASYNC_MODE}  ·  port={port}')
    print('━' * 56 + '\n')
    if _ASYNC_MODE == 'eventlet':
        socketio.run(app, host='0.0.0.0', port=port, debug=False)
    else:
        socketio.run(app, host='0.0.0.0', port=port, debug=False, allow_unsafe_werkzeug=True)
