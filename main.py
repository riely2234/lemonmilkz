"""
Vortex Hosting v11.2 — Fixed File/Folder Uploading
Install: pip install flask flask-socketio psutil werkzeug eventlet
Run:     python main.py
"""
try:
    import eventlet
    eventlet.monkey_patch()
    _ASYNC_MODE = 'eventlet'
except ImportError:
    _ASYNC_MODE = 'threading'

import contextlib, json, logging, os, shutil, subprocess, sys, threading, time, zipfile, psutil
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
app.secret_key = os.environ.get('VORTEX_SECRET_KEY', 'vortex-luxury-stable-key-v11')

socketio = SocketIO(app, cors_allowed_origins='*', async_mode=_ASYNC_MODE,
    logger=False, engineio_logger=False, max_http_buffer_size=200*1024*1024)

BOTS_DIR = os.path.join(os.getcwd(), 'vortex_bots')
CONFIG_FILE = os.path.join(os.getcwd(), 'vortex_config.json')
USERS_FILE = os.path.join(os.getcwd(), 'vortex_users.json')
os.makedirs(BOTS_DIR, exist_ok=True)

_config_lock = threading.RLock()
_users_lock = threading.RLock()
bots = {}

def load_users():
    with _users_lock:
        if os.path.exists(USERS_FILE):
            try:
                with open(USERS_FILE) as f: return json.load(f)
            except Exception: pass
        return {}

def save_users(u):
    with _users_lock:
        tmp = USERS_FILE + '.tmp'
        with open(tmp, 'w') as f: json.dump(u, f, indent=2)
        os.replace(tmp, USERS_FILE)

def load_config():
    with _config_lock:
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE) as f: return json.load(f)
            except Exception: pass
        return {}

def save_config(cfg):
    with _config_lock:
        tmp = CONFIG_FILE + '.tmp'
        with open(tmp, 'w') as f: json.dump(cfg, f, indent=2)
        os.replace(tmp, CONFIG_FILE)

def get_bot_dir(bot_id):
    p = os.path.join(BOTS_DIR, bot_id)
    os.makedirs(p, exist_ok=True)
    return p

def safe_path(bot_id, fn):
    bd = os.path.abspath(get_bot_dir(bot_id))
    clean_fn = os.path.normpath('/' + fn).lstrip('/')
    fp = os.path.abspath(os.path.join(bd, clean_fn))
    if fp == bd or fp.startswith(bd + os.sep): return fp
    return None

def check_owner(bot_id):
    user = session.get('username')
    return user and load_config().get(bot_id, {}).get('owner') == user

def check_access(bot_id):
    user = session.get('username')
    if not user: return False
    cfg = load_config().get(bot_id, {})
    return cfg.get('owner') == user or user in cfg.get('shared_with', [])

def emit_log(bot_id, msg, level='default'):
    cfg = load_config().get(bot_id, {})
    listeners = [cfg.get('owner')] + cfg.get('shared_with', [])
    for u in set(listeners):
        if u:
            with contextlib.suppress(Exception):
                socketio.emit('console_log', {'bot_id': bot_id, 'msg': msg, 'level': level}, room=u)
    entry = {'msg': msg, 'level': level, 'time': time.strftime('%H:%M:%S')}
    bots.setdefault(bot_id, {}).setdefault('logs', []).append(entry)
    if len(bots[bot_id]['logs']) > 500:
        bots[bot_id]['logs'] = bots[bot_id]['logs'][-500:]
    try:
        with open(os.path.join(get_bot_dir(bot_id), 'system.log'), 'a', encoding='utf-8') as f:
            f.write(json.dumps(entry) + '\n')
    except Exception: pass

def is_running(bot_id):
    return (bot_id in bots and bots[bot_id].get('process') is not None
            and bots[bot_id]['process'].poll() is None)

def broadcast_status(bot_id, status, start_t=None):
    cfg = load_config().get(bot_id, {})
    listeners = [cfg.get('owner')] + cfg.get('shared_with', [])
    payload = {'bot_id': bot_id, 'status': status}
    if start_t: payload['start_time'] = start_t
    for u in set(listeners):
        if u:
            with contextlib.suppress(Exception):
                socketio.emit('status_update', payload, room=u)

def start_bot(bot_id, startup_file=None):
    cfg = load_config()
    bot_cfg = cfg.get(bot_id, {})
    bot_dir = get_bot_dir(bot_id)
    startup_file = startup_file or bot_cfg.get('startup_file', 'main.py')
    full_path = os.path.join(bot_dir, startup_file)
    if is_running(bot_id): emit_log(bot_id, '[System] Already running.', 'system'); return
    if not os.path.exists(full_path): emit_log(bot_id, f'[Error] Not found: {startup_file}', 'error'); return
    req = os.path.join(bot_dir, 'requirements.txt')
    if os.path.exists(req):
        emit_log(bot_id, '[System] Installing requirements...', 'system')
        subprocess.run([sys.executable, '-m', 'pip', 'install', '-r', req], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        emit_log(bot_id, '[System] Requirements installed.', 'success')
    ext = startup_file.rsplit('.', 1)[-1].lower()
    if ext == 'py': cmd = [sys.executable, '-u', full_path]
    elif ext == 'js': cmd = ['node', full_path]
    elif ext == 'sh': cmd = ['bash', full_path]
    else: emit_log(bot_id, '[Error] Only .py / .js / .sh supported.', 'error'); return
    env = os.environ.copy(); env.update(bot_cfg.get('env', {}))
    emit_log(bot_id, f'[System] Starting {startup_file}...', 'system')
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            stdin=subprocess.PIPE, text=True, cwd=bot_dir, env=env)
        start_t = time.time()
        bots.setdefault(bot_id, {}).update({'process': proc, 'startup_file': startup_file,
            'start_time': start_t, 'auto_restart': bot_cfg.get('auto_restart', False)})
        bot_cfg['startup_file'] = startup_file; cfg[bot_id] = bot_cfg; save_config(cfg)
        broadcast_status(bot_id, 'online', start_t)
        def _read():
            for line in iter(proc.stdout.readline, ''): emit_log(bot_id, line.rstrip(), 'default')
            proc.wait(); broadcast_status(bot_id, 'offline')
            emit_log(bot_id, f'[System] Exited code {proc.returncode}.', 'system')
            if bots.get(bot_id, {}).get('auto_restart') and proc.returncode != 0:
                emit_log(bot_id, '[System] Auto-restart in 3s...', 'system'); time.sleep(3)
                if bots.get(bot_id, {}).get('auto_restart') and not is_running(bot_id): start_bot(bot_id, startup_file)
        threading.Thread(target=_read, daemon=True).start()
    except Exception as e: emit_log(bot_id, f'[Error] {e}', 'error')

def stop_bot(bot_id):
    if bot_id in bots and bots[bot_id].get('process'):
        proc = bots[bot_id]['process']
        if proc.poll() is None:
            proc.terminate()
            try: proc.wait(timeout=5)
            except subprocess.TimeoutExpired: proc.kill()
            emit_log(bot_id, '[System] Stopped.', 'system')
            broadcast_status(bot_id, 'offline')


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
  --void:#03040A;
  --deep:#07090F;
  --base:#0A0D16;
  --panel:#0E1220;
  --elevated:#141828;
  --glass:rgba(10,13,22,0.82);
  --border:rgba(0,200,255,0.07);
  --border-mid:rgba(0,200,255,0.14);
  --border-hi:rgba(0,200,255,0.28);
  --border-glow:rgba(0,200,255,0.5);
  --cyan:#00E5FF;
  --cyan-dim:rgba(0,229,255,0.08);
  --cyan-glow:rgba(0,229,255,0.3);
  --cyan-bright:rgba(0,229,255,0.7);
  --purple:#A020F0;
  --purple-dim:rgba(160,32,240,0.1);
  --purple-glow:rgba(160,32,240,0.4);
  --blue:#0066FF;
  --blue-dim:rgba(0,102,255,0.1);
  --green:#00FF7F;
  --green-dim:rgba(0,255,127,0.1);
  --green-glow:rgba(0,255,127,0.4);
  --amber:#FFB800;
  --amber-dim:rgba(255,184,0,0.1);
  --red:#FF2055;
  --red-dim:rgba(255,32,85,0.1);
  --red-glow:rgba(255,32,85,0.4);
  --text:#DCF0FF;
  --text-2:#5A7A9A;
  --text-3:#2A3E5A;
  --font-disp:'Orbitron',sans-serif;
  --font-sans:'Rajdhani',sans-serif;
  --font-mono:'Fira Code',monospace;
  --ease-spring:cubic-bezier(0.34,1.56,0.64,1);
  --ease-out:cubic-bezier(0.16,1,0.3,1);
}

html,body{width:100%;height:100%;overflow:hidden;background:var(--void);color:var(--text);font-family:var(--font-sans);-webkit-font-smoothing:antialiased}

/* === NOISE GRAIN TEXTURE === */
body::before{
  content:'';position:fixed;inset:0;z-index:0;pointer-events:none;
  background-image:url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='noise'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23noise)' opacity='0.03'/%3E%3C/svg%3E");
  opacity:0.4;
}

/* === DOT GRID BG === */
body::after{
  content:'';position:fixed;inset:0;z-index:0;pointer-events:none;
  background-image:radial-gradient(circle,rgba(0,180,255,0.06) 1px,transparent 1px);
  background-size:32px 32px;
}

/* === AMBIENT GLOW === */
#ambientGlow{
  position:fixed;inset:0;z-index:0;pointer-events:none;
  background:
    radial-gradient(ellipse 60% 40% at 0% 0%,rgba(0,100,255,0.06) 0%,transparent 70%),
    radial-gradient(ellipse 50% 50% at 100% 100%,rgba(160,32,240,0.07) 0%,transparent 70%),
    radial-gradient(ellipse 40% 60% at 50% 50%,rgba(0,229,255,0.03) 0%,transparent 80%);
}

#app{display:flex;width:100%;height:100%;position:relative;z-index:1}

::-webkit-scrollbar{width:4px;height:4px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:rgba(0,200,255,0.2);border-radius:10px}
::-webkit-scrollbar-thumb:hover{background:rgba(0,200,255,0.4)}

/* ===================== SIDEBAR ===================== */
.sidebar{
  width:272px;min-width:272px;height:100%;display:flex;flex-direction:column;
  position:relative;z-index:9500;
  background:linear-gradient(180deg,rgba(10,13,22,0.98) 0%,rgba(7,9,15,0.98) 100%);
  backdrop-filter:blur(40px);-webkit-backdrop-filter:blur(40px);
  border-right:1px solid var(--border-mid);
  box-shadow:1px 0 40px rgba(0,0,0,0.8),inset -1px 0 0 rgba(0,200,255,0.04);
  transition:transform .4s var(--ease-out);
}
/* Cyan top accent line */
.sidebar::before{
  content:'';position:absolute;top:0;left:0;right:0;height:1px;
  background:linear-gradient(90deg,transparent,var(--cyan),var(--purple),transparent);
  box-shadow:0 0 20px var(--cyan-glow);z-index:1;
}
/* Subtle vertical glow stripe */
.sidebar::after{
  content:'';position:absolute;top:0;right:0;width:1px;height:100%;
  background:linear-gradient(180deg,var(--cyan-glow),transparent 40%,var(--purple-glow) 80%,transparent);
  opacity:0.5;pointer-events:none;
}

.sidebar-close-btn{
  display:none;position:absolute;right:14px;top:22px;background:var(--elevated);
  border:1px solid var(--border-mid);color:var(--text-2);font-size:13px;cursor:pointer;
  transition:all .2s;z-index:10;width:28px;height:28px;border-radius:4px;
  align-items:center;justify-content:center;
}
.sidebar-close-btn:hover{color:var(--cyan);border-color:var(--border-hi)}

/* LOGO */
.logo{padding:28px 24px 22px;border-bottom:1px solid var(--border);position:relative;overflow:hidden}
.logo::after{
  content:'';position:absolute;bottom:0;left:20%;right:20%;height:1px;
  background:linear-gradient(90deg,transparent,var(--cyan-glow),transparent);
}
.logo-wordmark{
  font-family:var(--font-disp);font-size:30px;font-weight:900;letter-spacing:6px;
  line-height:1;
  background:linear-gradient(135deg,#ffffff 0%,var(--cyan) 60%,var(--purple) 100%);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;
  filter:drop-shadow(0 0 20px var(--cyan-glow));
}
.logo-sub{
  font-family:var(--font-mono);font-size:9px;letter-spacing:5px;color:var(--text-3);
  text-transform:uppercase;margin-top:5px;
}
.logo-line{
  position:absolute;bottom:0;left:0;right:0;height:2px;
  background:linear-gradient(90deg,var(--cyan),var(--purple));
  box-shadow:0 0 12px var(--cyan-glow);
}

/* NAV */
.nav{padding:20px 14px 8px;flex-shrink:0}
.nav-section{
  font-family:var(--font-mono);font-size:9px;font-weight:700;letter-spacing:4px;
  color:var(--text-3);text-transform:uppercase;padding:8px 10px 6px;
  display:flex;align-items:center;gap:10px;
}
.nav-section::after{content:'';flex:1;height:1px;background:var(--border)}

.nav-item{
  display:flex;align-items:center;gap:12px;padding:11px 14px;border-radius:6px;
  font-size:14px;font-weight:600;color:var(--text-2);cursor:pointer;
  transition:all .22s var(--ease-out);margin-bottom:3px;
  border:1px solid transparent;position:relative;overflow:hidden;
  font-family:var(--font-sans);letter-spacing:.5px;
}
.nav-item::before{
  content:'';position:absolute;left:0;top:0;bottom:0;width:2px;
  background:var(--cyan);box-shadow:0 0 10px var(--cyan-glow);
  transform:scaleY(0);transition:transform .22s var(--ease-spring);border-radius:0 2px 2px 0;
}
.nav-item:hover{
  background:rgba(0,229,255,0.05);color:var(--text);
  border-color:var(--border);transform:translateX(3px);
}
.nav-item.active{
  background:linear-gradient(90deg,rgba(0,229,255,0.1) 0%,rgba(0,229,255,0.03) 100%);
  color:var(--cyan);border-color:var(--border-mid);
}
.nav-item.active::before{transform:scaleY(1)}
.nav-item.active .nav-icon{color:var(--cyan);text-shadow:0 0 10px var(--cyan-glow)}
.nav-icon{width:20px;text-align:center;font-size:15px;flex-shrink:0;opacity:.6;transition:all .2s}
.nav-item:hover .nav-icon{opacity:1}
.nav-item.active .nav-icon{opacity:1}

/* BOT LIST */
.instances-header{
  display:flex;align-items:center;justify-content:space-between;
  padding:14px 24px 10px;flex-shrink:0;
  border-top:1px solid var(--border);
}
.instances-label{font-family:var(--font-mono);font-size:9px;letter-spacing:4px;text-transform:uppercase;color:var(--text-3)}
.instances-count{
  font-family:var(--font-mono);font-size:11px;font-weight:700;color:var(--cyan);
  background:var(--cyan-dim);border:1px solid var(--border-mid);
  border-radius:4px;padding:2px 8px;
}

.new-bot-btn{
  margin:0 14px 12px;display:flex;align-items:center;justify-content:center;gap:9px;
  padding:12px;border:1px dashed rgba(0,229,255,0.25);border-radius:6px;
  font-family:var(--font-disp);font-size:11px;font-weight:600;letter-spacing:2px;
  color:var(--text-3);background:transparent;cursor:pointer;
  transition:all .25s var(--ease-out);text-transform:uppercase;
}
.new-bot-btn:hover{
  border-color:var(--purple);color:var(--purple);
  background:var(--purple-dim);transform:translateY(-1px);
  box-shadow:0 4px 20px rgba(160,32,240,0.15);
}
.new-bot-btn-plus{
  width:22px;height:22px;border-radius:5px;
  background:var(--elevated);display:flex;align-items:center;justify-content:center;
  font-size:16px;transition:all .2s;font-family:var(--font-sans);
}
.new-bot-btn:hover .new-bot-btn-plus{background:var(--purple-dim);color:var(--purple)}

.bot-list{flex:1;overflow-y:auto;padding:0 14px 14px}
.bot-item{
  display:flex;align-items:center;gap:11px;padding:12px 14px;border-radius:6px;
  cursor:pointer;transition:all .2s var(--ease-out);margin-bottom:5px;
  border:1px solid var(--border);background:rgba(0,0,0,0.25);position:relative;overflow:hidden;
}
.bot-item::before{
  content:'';position:absolute;inset:0;opacity:0;transition:opacity .2s;
  background:linear-gradient(135deg,rgba(0,229,255,0.04) 0%,transparent 100%);
}
.bot-item:hover{border-color:var(--border-mid);background:rgba(0,229,255,0.04)}
.bot-item:hover::before{opacity:1}
.bot-item.active{
  background:rgba(0,229,255,0.07);border-color:rgba(0,229,255,0.25);
  box-shadow:0 0 20px rgba(0,229,255,0.06),inset 0 0 20px rgba(0,229,255,0.03);
}
.bot-item.active::before{opacity:1}

.bot-dot{width:9px;height:9px;border-radius:50%;flex-shrink:0;position:relative}
.bot-dot.online{
  background:var(--green);
  box-shadow:0 0 10px var(--green-glow),0 0 4px var(--green);
  animation:dotPulse 2.2s ease-in-out infinite;
}
@keyframes dotPulse{
  0%,100%{box-shadow:0 0 10px var(--green-glow),0 0 4px var(--green)}
  50%{box-shadow:0 0 18px var(--green-glow),0 0 8px var(--green)}
}
.bot-dot.offline{background:var(--text-3);box-shadow:none}

.bot-name{
  font-family:var(--font-sans);font-size:14px;font-weight:600;
  color:var(--text-2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;
  transition:color .2s;flex:1;
}
.bot-item.active .bot-name,.bot-item:hover .bot-name{color:var(--text)}
.bot-status{
  font-family:var(--font-mono);font-size:9px;letter-spacing:2px;
  text-transform:uppercase;color:var(--text-3);margin-top:2px;transition:color .2s;
}
.bot-item.active .bot-status.online{color:var(--green)}
.bot-shared-tag{
  font-family:var(--font-mono);font-size:9px;color:var(--purple);
  background:var(--purple-dim);border:1px solid rgba(160,32,240,0.25);
  border-radius:3px;padding:1px 5px;flex-shrink:0;
}

.sidebar-footer{
  padding:16px 24px;border-top:1px solid var(--border);
  display:flex;justify-content:space-between;align-items:center;
  background:rgba(0,0,0,0.5);flex-shrink:0;
}
.logout-btn{
  font-family:var(--font-mono);font-size:10px;letter-spacing:2px;
  color:var(--text-3);cursor:pointer;transition:all .2s;
  text-transform:uppercase;background:none;border:none;
}
.logout-btn:hover{color:var(--red)}
.sidebar-clock{font-family:var(--font-mono);font-size:13px;color:var(--cyan);font-weight:500}

/* ===================== MOBILE ===================== */
.sidebar-overlay{
  display:none;position:fixed;inset:0;background:rgba(0,0,0,0.75);
  z-index:9000;opacity:0;transition:opacity .3s;cursor:pointer;
  backdrop-filter:blur(4px);
}
.sidebar-overlay.open{opacity:1}

.mobile-bottom-nav{
  display:none;position:fixed;bottom:14px;left:12px;right:12px;height:66px;
  background:rgba(10,13,22,0.9);backdrop-filter:blur(30px);-webkit-backdrop-filter:blur(30px);
  border:1px solid var(--border-mid);border-radius:18px;z-index:9000;
  justify-content:space-around;align-items:center;padding:0 8px;
  box-shadow:0 8px 40px rgba(0,0,0,0.8),0 0 0 1px rgba(0,229,255,0.05),0 0 30px rgba(0,229,255,0.04);
}
.m-nav-item{
  display:flex;flex-direction:column;align-items:center;gap:3px;
  color:var(--text-3);cursor:pointer;transition:all .25s var(--ease-spring);
  padding:8px 10px;border-radius:12px;flex:1;
}
.m-nav-icon{font-size:18px;transition:transform .25s var(--ease-spring)}
.m-nav-label{
  font-family:var(--font-mono);font-size:9px;font-weight:500;
  letter-spacing:1px;text-transform:uppercase;
}
.m-nav-item.active{color:var(--cyan)}
.m-nav-item.active .m-nav-icon{
  transform:translateY(-3px) scale(1.1);
  filter:drop-shadow(0 0 6px var(--cyan-glow));
}
.m-nav-item:hover:not(.active){color:var(--text-2);background:rgba(255,255,255,0.04)}

/* ===================== TOPBAR ===================== */
.main{flex:1;min-width:0;height:100%;display:flex;flex-direction:column;position:relative;z-index:10}

.topbar{
  height:68px;min-height:68px;
  background:linear-gradient(90deg,rgba(10,13,22,0.95) 0%,rgba(8,10,18,0.95) 100%);
  backdrop-filter:blur(20px);-webkit-backdrop-filter:blur(20px);
  border-bottom:1px solid var(--border-mid);
  display:flex;align-items:center;justify-content:space-between;
  padding:0 28px;gap:16px;flex-shrink:0;
  box-shadow:0 1px 0 var(--border),0 4px 30px rgba(0,0,0,0.6);
  position:relative;
}
/* subtle bottom glow */
.topbar::after{
  content:'';position:absolute;bottom:-1px;left:0;right:0;height:1px;
  background:linear-gradient(90deg,transparent,rgba(0,229,255,0.15),transparent);
}

.topbar-left{display:flex;align-items:center;gap:14px;min-width:0}

.mobile-menu-btn{
  display:none;background:var(--elevated);border:1px solid var(--border-mid);
  color:var(--text-2);padding:8px 12px;border-radius:6px;font-size:16px;
  cursor:pointer;transition:all .2s;flex-shrink:0;
}
.mobile-menu-btn:hover{border-color:var(--border-hi);color:var(--cyan)}

.breadcrumb{display:flex;align-items:center;gap:10px;min-width:0}
.bc-brand{font-family:var(--font-mono);font-size:11px;letter-spacing:3px;color:var(--text-3);text-transform:uppercase}
.bc-sep{color:var(--border-hi);font-size:14px;font-weight:300;opacity:.6}
.bc-page{
  font-family:var(--font-disp);font-size:18px;font-weight:700;
  letter-spacing:3px;color:var(--text);text-transform:uppercase;
}
.bc-bot{
  font-family:var(--font-mono);font-size:11px;color:var(--purple);
  background:var(--purple-dim);padding:4px 10px;border-radius:4px;
  border:1px solid rgba(160,32,240,0.25);
  overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:160px;
}

.topbar-right{display:flex;align-items:center;gap:9px;flex-shrink:0}

.status-badge{
  display:flex;align-items:center;gap:7px;padding:7px 14px;border-radius:20px;
  font-family:var(--font-mono);font-size:11px;font-weight:600;letter-spacing:1px;
  transition:all .3s;border:1px solid var(--border);background:rgba(0,0,0,0.4);
  text-transform:uppercase;
}
.status-badge.online{
  color:var(--green);border-color:rgba(0,255,127,0.3);
  background:var(--green-dim);box-shadow:0 0 14px rgba(0,255,127,0.1);
}
.status-badge.offline{color:var(--text-3);border-color:var(--border)}
.status-led{width:7px;height:7px;border-radius:50%;flex-shrink:0}
.status-badge.online .status-led{
  background:var(--green);box-shadow:0 0 8px var(--green-glow);
  animation:ledBlink 1.8s ease-in-out infinite;
}
.status-badge.offline .status-led{background:var(--text-3)}
@keyframes ledBlink{0%,100%{opacity:1}50%{opacity:.3}}

/* ===================== BUTTONS ===================== */
.btn{
  display:inline-flex;align-items:center;justify-content:center;gap:7px;
  padding:9px 18px;border-radius:6px;font-size:12px;font-weight:700;
  letter-spacing:1.5px;text-transform:uppercase;border:none;cursor:pointer;
  transition:all .18s var(--ease-out);font-family:var(--font-disp);
  position:relative;overflow:hidden;white-space:nowrap;
}
.btn::after{
  content:'';position:absolute;inset:0;opacity:0;
  background:linear-gradient(135deg,rgba(255,255,255,0.1) 0%,transparent 100%);
  transition:opacity .18s;
}
.btn:hover::after{opacity:1}
.btn:active{transform:scale(0.95)!important}

.btn-cyan{
  background:linear-gradient(135deg,rgba(0,229,255,0.2) 0%,rgba(0,150,200,0.15) 100%);
  color:var(--cyan);border:1px solid rgba(0,229,255,0.35);
  box-shadow:0 0 15px rgba(0,229,255,0.1),inset 0 1px 0 rgba(0,229,255,0.1);
}
.btn-cyan:hover{
  background:linear-gradient(135deg,rgba(0,229,255,0.3) 0%,rgba(0,150,200,0.2) 100%);
  border-color:rgba(0,229,255,0.6);transform:translateY(-1px);
  box-shadow:0 4px 20px rgba(0,229,255,0.2),inset 0 1px 0 rgba(0,229,255,0.2);
}
.btn-green{
  background:var(--green-dim);color:var(--green);
  border:1px solid rgba(0,255,127,0.3);
}
.btn-green:hover{background:rgba(0,255,127,0.18);border-color:rgba(0,255,127,0.55);transform:translateY(-1px);box-shadow:0 4px 15px rgba(0,255,127,0.15)}
.btn-red{
  background:var(--red-dim);color:var(--red);
  border:1px solid rgba(255,32,85,0.3);
}
.btn-red:hover{background:rgba(255,32,85,0.2);border-color:rgba(255,32,85,0.55);transform:translateY(-1px);box-shadow:0 4px 15px rgba(255,32,85,0.15)}
.btn-amber{
  background:var(--amber-dim);color:var(--amber);
  border:1px solid rgba(255,184,0,0.3);
}
.btn-amber:hover{background:rgba(255,184,0,0.2);border-color:rgba(255,184,0,0.55);transform:translateY(-1px);box-shadow:0 4px 15px rgba(255,184,0,0.15)}
.btn-purple{
  background:var(--purple-dim);color:var(--purple);
  border:1px solid rgba(160,32,240,0.3);
}
.btn-purple:hover{background:rgba(160,32,240,0.2);border-color:rgba(160,32,240,0.55);transform:translateY(-1px);box-shadow:0 4px 15px rgba(160,32,240,0.15)}
.btn-ghost{
  background:rgba(255,255,255,0.04);color:var(--text);
  border:1px solid var(--border-mid);
}
.btn-ghost:hover{background:rgba(255,255,255,0.08);border-color:var(--border-hi);transform:translateY(-1px)}
.btn-sm{padding:7px 13px;font-size:11px;letter-spacing:1px}
.btn-row{display:flex;gap:9px;flex-wrap:wrap;align-items:center}

/* ===================== PAGES ===================== */
.page{flex:1;min-height:0;overflow-y:auto;padding:28px;display:none}
.page.active{display:block;animation:pageIn .35s var(--ease-out)}
@keyframes pageIn{from{opacity:0;transform:translateY(12px)}to{opacity:1;transform:translateY(0)}}

/* ===================== STAT CARDS ===================== */
.stats-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:24px}

.stat-card{
  background:var(--panel);border:1px solid var(--border);border-radius:8px;
  padding:22px;position:relative;overflow:hidden;transition:all .25s var(--ease-out);
}
.stat-card::before{
  content:'';position:absolute;top:0;left:0;right:0;height:2px;
  background:var(--card-accent,linear-gradient(90deg,var(--cyan),var(--purple)));
  opacity:.8;
}
.stat-card::after{
  content:'';position:absolute;top:0;left:0;right:0;bottom:0;
  background:radial-gradient(ellipse at top left,var(--card-glow,rgba(0,229,255,0.04)) 0%,transparent 70%);
  pointer-events:none;
}
.stat-card:hover{
  border-color:var(--border-mid);transform:translateY(-3px);
  box-shadow:0 12px 40px rgba(0,0,0,0.6);
}
.stat-card-cyan{--card-accent:var(--cyan);--card-glow:rgba(0,229,255,0.05)}
.stat-card-purple{--card-accent:var(--purple);--card-glow:rgba(160,32,240,0.04)}
.stat-card-green{--card-accent:var(--green);--card-glow:rgba(0,255,127,0.04)}
.stat-card-amber{--card-accent:var(--amber);--card-glow:rgba(255,184,0,0.04)}

.stat-label{
  font-family:var(--font-mono);font-size:10px;font-weight:700;
  letter-spacing:3px;color:var(--text-2);margin-bottom:12px;
  text-transform:uppercase;
}
.stat-value{
  font-family:var(--font-disp);font-size:40px;font-weight:700;line-height:1;margin-bottom:8px;
}
.sv-cyan{color:var(--cyan);text-shadow:0 0 25px rgba(0,229,255,0.4)}
.sv-green{color:var(--green);text-shadow:0 0 25px rgba(0,255,127,0.4)}
.sv-amber{color:var(--amber);text-shadow:0 0 20px rgba(255,184,0,0.3)}
.sv-red{color:var(--red);text-shadow:0 0 20px rgba(255,32,85,0.3)}
.sv-purple{color:var(--purple);text-shadow:0 0 25px rgba(160,32,240,0.4)}
.stat-sub{font-family:var(--font-mono);font-size:10.5px;color:var(--text-3)}

/* ===================== PANELS ===================== */
.panel{
  background:var(--panel);border:1px solid var(--border);border-radius:8px;
  margin-bottom:20px;overflow:hidden;
  box-shadow:0 8px 30px rgba(0,0,0,0.5),inset 0 1px 0 rgba(0,200,255,0.04);
}
.panel-head{
  display:flex;align-items:center;justify-content:space-between;
  padding:17px 22px;border-bottom:1px solid var(--border);
  background:linear-gradient(90deg,rgba(0,0,0,0.4) 0%,rgba(0,0,0,0.2) 100%);
  flex-wrap:wrap;gap:12px;
}
.panel-title{
  display:flex;align-items:center;gap:12px;
  font-family:var(--font-disp);font-size:14px;font-weight:700;
  letter-spacing:3px;color:var(--text);text-transform:uppercase;
}
.panel-icon{
  width:28px;height:28px;border-radius:6px;
  background:var(--elevated);border:1px solid var(--border-mid);
  display:flex;align-items:center;justify-content:center;font-size:13px;
}
.panel-tag{
  font-family:var(--font-mono);font-size:10px;font-weight:700;letter-spacing:2px;
  text-transform:uppercase;color:var(--purple);padding:4px 10px;
  border:1px solid rgba(160,32,240,0.3);border-radius:4px;background:var(--purple-dim);
}
.panel-body{padding:22px}

/* ===================== TERMINAL ===================== */
.term-chrome{
  background:var(--deep);border-bottom:1px solid var(--border);
  padding:12px 18px;display:flex;align-items:center;gap:10px;
}
.term-dots{display:flex;gap:7px}
.term-dot{width:12px;height:12px;border-radius:50%}
.term-title{
  flex:1;text-align:center;font-family:var(--font-mono);font-size:10px;
  font-weight:700;letter-spacing:4px;color:var(--text-3);text-transform:uppercase;
}

.terminal{
  background:rgba(0,0,0,0.6);padding:18px;overflow-y:auto;
  font-family:var(--font-mono);font-size:13.5px;line-height:1.7;
}
.log-row{display:flex;align-items:baseline;gap:14px;padding:3px 4px;border-radius:4px;transition:background .15s}
.log-row:hover{background:rgba(0,200,255,0.04)}
.log-ts{font-size:11px;color:var(--text-3);flex-shrink:0;min-width:60px}
.log-tag{
  font-size:9.5px;padding:2px 7px;border-radius:3px;flex-shrink:0;
  text-transform:uppercase;font-weight:700;letter-spacing:2px;font-family:var(--font-mono);
}
.log-tag.sys{background:var(--blue-dim);color:var(--blue);border:1px solid rgba(0,102,255,0.3)}
.log-tag.err{background:var(--red-dim);color:var(--red);border:1px solid rgba(255,32,85,0.3)}
.log-tag.ok{background:var(--green-dim);color:var(--green);border:1px solid rgba(0,255,127,0.3)}
.log-tag.warn{background:var(--amber-dim);color:var(--amber);border:1px solid rgba(255,184,0,0.3)}
.log-tag.out{background:rgba(255,255,255,0.05);color:var(--text-2);border:1px solid var(--border)}
.log-msg{flex:1;word-break:break-all}
.log-msg.sys{color:var(--blue)}.log-msg.err{color:var(--red)}.log-msg.ok{color:var(--green)}
.log-msg.warn{color:var(--amber)}.log-msg.out{color:var(--text-2)}

.term-input-wrap{
  display:flex;align-items:center;gap:14px;
  background:rgba(0,0,0,0.5);border-top:1px solid var(--border);
  padding:14px 20px;transition:all .2s;
}
.term-input-wrap:focus-within{
  background:rgba(0,229,255,0.03);border-top-color:rgba(0,229,255,0.25);
  box-shadow:inset 0 1px 0 rgba(0,229,255,0.05);
}
.term-prompt{
  font-family:var(--font-mono);font-size:16px;color:var(--cyan);flex-shrink:0;
  text-shadow:0 0 8px var(--cyan-glow);
}
.term-input{
  flex:1;background:none;border:none;outline:none;
  font-family:var(--font-mono);font-size:14px;color:var(--cyan);
  caret-color:var(--cyan);
}
.term-input::placeholder{color:var(--text-3)}

/* ===================== FORMS ===================== */
.form-group{margin-bottom:20px}
.form-label{
  display:flex;align-items:center;gap:10px;
  font-family:var(--font-mono);font-size:10px;font-weight:700;
  letter-spacing:3px;color:var(--text-2);text-transform:uppercase;margin-bottom:10px;
}
.form-label::after{content:'';flex:1;height:1px;background:var(--border)}

.form-input,.form-select,.form-textarea{
  width:100%;background:rgba(0,0,0,0.5);border:1px solid var(--border);
  border-left:2px solid var(--border-mid);border-radius:6px;
  padding:13px 16px;font-size:14px;color:var(--text);outline:none;
  font-family:var(--font-mono);transition:all .2s;
}
.form-input:focus,.form-select:focus,.form-textarea:focus{
  border-color:var(--border-hi);border-left-color:var(--cyan);
  background:rgba(0,229,255,0.03);
  box-shadow:0 0 0 3px rgba(0,229,255,0.06);
}
.form-input::placeholder,.form-textarea::placeholder{color:var(--text-3)}
.form-select option{background:var(--base)}
.form-textarea{resize:vertical;min-height:120px;line-height:1.7}
.form-row-2{display:grid;grid-template-columns:1fr 1fr;gap:20px}

.section-divider{
  font-family:var(--font-disp);font-size:13px;font-weight:700;letter-spacing:3px;
  color:var(--cyan);border-bottom:1px solid var(--border);padding-bottom:10px;
  margin:28px 0 18px;text-transform:uppercase;
  text-shadow:0 0 15px var(--cyan-glow);
}

/* ===================== ENV ===================== */
.env-row{display:grid;grid-template-columns:1fr 1.5fr auto;gap:8px;margin-bottom:8px;align-items:center}
.env-field{
  background:rgba(0,0,0,0.5);border:1px solid var(--border);border-radius:5px;
  padding:10px 13px;font-family:var(--font-mono);font-size:12.5px;
  color:var(--text);outline:none;width:100%;transition:border-color .2s;
}
.env-field:focus{border-color:var(--border-hi)}
.env-key{color:var(--amber)}

/* ===================== FILE TABLE ===================== */
.file-table{width:100%;border-collapse:separate;border-spacing:0;min-width:520px;table-layout:fixed}
.file-table th{
  font-family:var(--font-mono);font-size:10px;text-transform:uppercase;
  letter-spacing:3px;color:var(--text-3);padding:13px 16px;
  border-bottom:1px solid var(--border-mid);text-align:left;font-weight:700;
  background:rgba(0,0,0,0.3);
}
.file-table th:nth-child(1){width:40%}
.file-table th:nth-child(2){width:80px}
.file-table th:nth-child(3){width:70px}
.file-table th:nth-child(4){width:150px}
.file-table th:nth-child(5){width:160px}
.file-table td{
  padding:11px 16px;font-size:13.5px;border-bottom:1px solid var(--border);
  vertical-align:middle;font-family:var(--font-mono);overflow:hidden;
}
.file-table tr:last-child td{border-bottom:none}
.file-table tr:hover td{background:rgba(0,229,255,0.03)}
.file-name-cell{display:flex;align-items:center;gap:8px;width:100%;min-width:0}
.file-name-link{
  display:flex;align-items:center;gap:9px;color:var(--cyan);cursor:pointer;
  transition:all .18s;min-width:0;flex:1;
}
.file-name-link:hover{color:#fff;transform:translateX(3px);text-shadow:0 0 10px var(--cyan-glow)}
.file-name-text{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex:1}
.file-rename-wrap{flex:1;display:none;align-items:center;gap:6px;min-width:0}
.file-rename-wrap.active{display:flex}
.file-rename-input{
  flex:1;background:rgba(0,0,0,0.6);border:1px solid var(--border-hi);
  border-bottom:2px solid var(--amber);border-radius:5px;
  padding:5px 10px;font-family:var(--font-mono);font-size:13px;
  color:var(--amber);outline:none;min-width:0;
}
.file-rename-confirm{
  background:var(--amber-dim);border:1px solid rgba(255,184,0,0.4);border-radius:4px;
  color:var(--amber);font-size:11px;padding:4px 9px;cursor:pointer;
  font-family:var(--font-mono);white-space:nowrap;transition:all .15s;flex-shrink:0;
}
.file-rename-confirm:hover{background:rgba(255,184,0,0.25)}
.file-rename-cancel{
  background:transparent;border:1px solid var(--border);border-radius:4px;
  color:var(--text-3);font-size:11px;padding:4px 8px;cursor:pointer;
  font-family:var(--font-mono);transition:all .15s;flex-shrink:0;
}
.file-rename-cancel:hover{color:var(--text);border-color:var(--border-hi)}
.file-ext-badge{
  font-size:9.5px;letter-spacing:1.5px;text-transform:uppercase;
  padding:2px 7px;border-radius:4px;border:1px solid var(--border-mid);
  color:var(--text-2);background:rgba(0,0,0,0.4);flex-shrink:0;
}

/* ===================== UPLOAD ===================== */
.drop-zone{
  border:2px dashed rgba(0,229,255,0.2);padding:40px 24px;text-align:center;
  transition:all .3s var(--ease-out);background:rgba(0,0,0,0.25);border-radius:8px;
}
.drop-zone.dragging{
  border-color:var(--cyan);background:rgba(0,229,255,0.04);
  box-shadow:0 0 40px rgba(0,229,255,0.08),inset 0 0 40px rgba(0,229,255,0.04);
}
.drop-icon{font-size:44px;margin-bottom:14px;display:block;color:var(--text-3);transition:all .3s;pointer-events:none}
.drop-zone.dragging .drop-icon{color:var(--cyan);transform:translateY(-5px);filter:drop-shadow(0 0 10px var(--cyan-glow))}
.drop-title{
  font-family:var(--font-disp);font-size:20px;letter-spacing:4px;
  color:var(--text);margin-bottom:8px;pointer-events:none;
}
.drop-sub{
  font-family:var(--font-mono);font-size:10px;letter-spacing:2px;
  color:var(--text-3);margin-bottom:24px;pointer-events:none;
}

.upload-row{
  display:flex;align-items:center;gap:10px;padding:10px 14px;
  background:rgba(0,0,0,0.5);border:1px solid var(--border);border-radius:6px;
  margin-top:6px;font-family:var(--font-mono);font-size:11.5px;color:var(--text-2);
  transition:border-color .3s;
}
.upload-bar-wrap{flex:1;height:3px;background:rgba(0,200,255,0.1);border-radius:2px;overflow:hidden}
.upload-bar-fill{height:100%;background:linear-gradient(90deg,var(--cyan),var(--purple));border-radius:2px;transition:width .12s linear}

/* ===================== RESOURCES ===================== */
.res-item{margin-bottom:26px}
.res-header{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:10px}
.res-label{font-family:var(--font-mono);font-size:10px;letter-spacing:3px;text-transform:uppercase;color:var(--text-2)}
.res-value{font-family:var(--font-disp);font-size:24px;font-weight:700}
.res-track{height:6px;background:rgba(0,0,0,0.6);border:1px solid var(--border);border-radius:3px;overflow:hidden}
.res-fill{height:100%;border-radius:3px;transition:width 1s ease}

/* ===================== MODAL ===================== */
.modal-veil{
  display:none;position:fixed;inset:0;background:rgba(0,0,0,0.7);
  z-index:10000;align-items:center;justify-content:center;
  backdrop-filter:blur(8px);-webkit-backdrop-filter:blur(8px);
}
.modal-veil.open{display:flex;animation:veilIn .25s ease}
@keyframes veilIn{from{opacity:0}to{opacity:1}}

.modal-box{
  background:linear-gradient(160deg,var(--elevated) 0%,var(--base) 100%);
  border:1px solid var(--border-mid);border-top:2px solid var(--cyan);
  border-radius:10px;padding:36px;width:95%;max-width:580px;
  max-height:90vh;overflow-y:auto;
  box-shadow:0 24px 80px rgba(0,0,0,0.9),0 0 0 1px rgba(0,229,255,0.05),0 0 60px rgba(0,229,255,0.06);
  animation:modalIn .35s var(--ease-spring);
}
.modal-box.wide{max-width:1000px}
@keyframes modalIn{from{transform:scale(0.93) translateY(25px);opacity:0}to{transform:scale(1) translateY(0);opacity:1}}

.modal-title{
  font-family:var(--font-disp);font-size:28px;font-weight:700;color:var(--text);
  margin-bottom:28px;letter-spacing:4px;display:flex;align-items:center;gap:12px;
}
.modal-title-accent{color:var(--cyan);text-shadow:0 0 20px var(--cyan-glow)}
.modal-footer{
  display:flex;justify-content:flex-end;gap:12px;margin-top:28px;
  padding-top:22px;border-top:1px solid var(--border);
}

/* ===================== LOGIN ===================== */
#loginOverlay{
  position:fixed;inset:0;background:var(--void);z-index:99999;
  display:flex;align-items:center;justify-content:center;
}
#loginOverlay::before{
  content:'';position:absolute;inset:0;pointer-events:none;
  background:
    radial-gradient(ellipse 60% 50% at 30% 30%,rgba(0,229,255,0.08) 0%,transparent 60%),
    radial-gradient(ellipse 50% 60% at 80% 70%,rgba(160,32,240,0.08) 0%,transparent 60%);
}

.login-card{
  background:linear-gradient(160deg,rgba(14,18,32,0.9) 0%,rgba(10,13,22,0.95) 100%);
  backdrop-filter:blur(40px);-webkit-backdrop-filter:blur(40px);
  border:1px solid var(--border-mid);border-top:2px solid var(--purple);
  padding:52px 46px;width:90%;max-width:440px;border-radius:12px;
  box-shadow:0 24px 80px rgba(0,0,0,0.9),0 0 60px rgba(160,32,240,0.08),0 0 0 1px rgba(160,32,240,0.04);
  animation:loginUp .7s var(--ease-spring);position:relative;z-index:1;
}
@keyframes loginUp{from{transform:translateY(40px) scale(0.96);opacity:0}to{transform:translateY(0) scale(1);opacity:1}}

.login-logo{
  font-family:var(--font-disp);font-size:52px;font-weight:900;letter-spacing:8px;
  text-align:center;margin-bottom:4px;
  background:linear-gradient(135deg,#fff 0%,var(--cyan) 50%,var(--purple) 100%);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;
  filter:drop-shadow(0 0 30px rgba(0,229,255,0.3));
}
.login-tagline{
  font-family:var(--font-mono);color:var(--text-3);font-size:10px;letter-spacing:6px;
  text-transform:uppercase;text-align:center;margin-bottom:28px;
}

.auth-tabs{display:flex;background:rgba(0,0,0,0.4);border-radius:6px;padding:3px;margin-bottom:22px;border:1px solid var(--border)}
.auth-tab{
  flex:1;text-align:center;padding:9px 12px;font-family:var(--font-mono);
  font-size:12px;font-weight:700;letter-spacing:2px;color:var(--text-3);
  cursor:pointer;text-transform:uppercase;transition:all .25s;border-radius:4px;
}
.auth-tab.active{
  color:var(--purple);background:var(--purple-dim);
  border:1px solid rgba(160,32,240,0.3);text-shadow:0 0 10px rgba(160,32,240,0.4);
}

/* ===================== CODE EDITOR ===================== */
.code-editor{
  width:100%;min-height:530px;background:rgba(0,0,0,0.7);
  border:1px solid var(--border);border-left:3px solid var(--blue);
  border-radius:6px;padding:20px;font-family:var(--font-mono);
  font-size:13.5px;color:var(--text);outline:none;resize:vertical;
  line-height:1.7;caret-color:var(--cyan);transition:border-color .2s;
}
.code-editor:focus{border-left-color:var(--cyan);border-color:var(--border-hi)}

/* ===================== DANGER ZONE ===================== */
.danger-zone{
  border:1px solid rgba(255,32,85,0.25);border-left:3px solid var(--red);
  background:linear-gradient(90deg,rgba(255,32,85,0.05) 0%,transparent 100%);
  padding:22px;margin-top:20px;border-radius:8px;
}

/* ===================== TOASTS ===================== */
.toast-tray{position:fixed;bottom:28px;right:28px;z-index:20000;display:flex;flex-direction:column;gap:10px;pointer-events:none}
.toast{
  background:var(--elevated);border:1px solid var(--border-mid);
  border-radius:8px;padding:14px 18px;font-size:13px;font-weight:600;
  color:var(--text);font-family:var(--font-sans);letter-spacing:.5px;
  box-shadow:0 8px 30px rgba(0,0,0,0.7);
  display:flex;align-items:center;gap:12px;pointer-events:all;min-width:260px;
  animation:toastIn .35s var(--ease-spring);position:relative;overflow:hidden;
}
.toast::before{
  content:'';position:absolute;left:0;top:0;bottom:0;width:3px;border-radius:3px 0 0 3px;
}
.toast.success::before{background:var(--green);box-shadow:0 0 10px var(--green-glow)}
.toast.error::before{background:var(--red);box-shadow:0 0 10px var(--red-glow)}
.toast.info::before{background:var(--blue)}
.toast-icon{width:22px;height:22px;border-radius:5px;display:flex;align-items:center;justify-content:center;font-size:12px;flex-shrink:0}
.toast.success .toast-icon{background:var(--green-dim);color:var(--green)}
.toast.error .toast-icon{background:var(--red-dim);color:var(--red)}
.toast.info .toast-icon{background:var(--blue-dim);color:var(--blue)}
.toast-close{margin-left:auto;cursor:pointer;color:var(--text-3);font-size:14px;transition:color .2s}
.toast-close:hover{color:var(--text)}
@keyframes toastIn{from{transform:translateX(20px);opacity:0}to{transform:translateX(0);opacity:1}}

/* ===================== SUBUSERS ===================== */
.subuser-row{
  display:flex;justify-content:space-between;align-items:center;
  padding:11px 14px;background:rgba(0,0,0,0.35);border:1px solid var(--border);
  border-radius:6px;margin-bottom:6px;font-family:var(--font-mono);font-size:13px;
}

/* ===================== RESPONSIVE ===================== */
@media(max-width:860px){
  .sidebar{position:fixed;left:0;transform:translateX(-110%);width:280px;z-index:9600}
  .sidebar.open{transform:translateX(0)}
  .sidebar-overlay{display:block}
  .sidebar .nav{display:none}
  .sidebar-close-btn{display:flex}
  .mobile-bottom-nav{display:flex}
  .mobile-menu-btn{display:flex}
  .main{padding-bottom:90px}
  .topbar{height:auto;min-height:60px;padding:12px 16px;flex-direction:column;align-items:stretch;gap:10px}
  .topbar-left{width:100%;justify-content:space-between}
  .breadcrumb{flex:1;margin-left:10px}
  .bc-page{font-size:16px}
  .bc-brand,.bc-sep:first-of-type{display:none}
  .bc-bot{max-width:120px;font-size:10px}
  .topbar-right{
    display:flex;flex-wrap:nowrap;overflow-x:auto;padding-bottom:4px;
    width:100%;gap:7px;border-top:1px solid var(--border);padding-top:10px;
    -webkit-overflow-scrolling:touch;
  }
  .topbar-right::-webkit-scrollbar{display:none}
  .topbar-right .btn{white-space:nowrap;flex-shrink:0;padding:8px 12px;font-size:10px}
  .topbar-right .status-badge span:last-child{display:none}
  .page{padding:12px}
  .stats-grid{grid-template-columns:1fr 1fr;gap:10px;margin-bottom:14px}
  .stat-value{font-size:30px}
  .form-row-2{grid-template-columns:1fr;gap:14px}
  .panel-head{padding:12px 14px;flex-direction:column;align-items:flex-start;gap:10px}
  .panel-body{padding:14px}
  .panel-title{font-size:12px;letter-spacing:2px}
  .file-table-wrap{overflow-x:auto;-webkit-overflow-scrolling:touch;width:100%}
  .file-table th:nth-child(4),.file-table td:nth-child(4){display:none}
  .file-table th:nth-child(2),.file-table td:nth-child(2){display:none}
  .drop-zone{padding:28px 14px}
  .drop-title{font-size:16px;letter-spacing:2px}
  .drop-sub{font-size:9px}
  .toast-tray{bottom:90px;right:10px;left:10px}
  .toast{min-width:0;width:100%;justify-content:flex-start}
}
@media(max-width:480px){
  .stats-grid{grid-template-columns:1fr}
  .stat-value{font-size:26px}
  .login-card{padding:32px 18px;width:96%}
  .login-logo{font-size:40px}
  .modal-box{padding:20px 14px}
  .modal-title{font-size:22px;margin-bottom:18px}
  .page{padding:8px}
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
    <div class="form-group" style="margin-bottom:12px">
      <input class="form-input" id="authUsername" placeholder="Username" style="text-align:center;letter-spacing:2px" onkeydown="if(event.key==='Enter')submitAuth()">
    </div>
    <div class="form-group" style="margin-bottom:20px">
      <input type="password" class="form-input" id="authPassword" placeholder="Password" style="text-align:center;letter-spacing:2px" onkeydown="if(event.key==='Enter')submitAuth()">
    </div>
    <button class="btn btn-cyan" id="authBtn" style="width:100%;padding:15px;font-size:13px;letter-spacing:4px" onclick="submitAuth()">AUTHENTICATE</button>
  </div>
</div>

<div class="sidebar-overlay" onclick="toggleSidebar()"></div>

<div id="app">
  <!-- SIDEBAR -->
  <aside class="sidebar">
    <button class="sidebar-close-btn" onclick="toggleSidebar()">✕</button>
    <div class="logo">
      <div class="logo-wordmark">VORTEX</div>
      <div class="logo-sub">Hosting Platform // Admin</div>
      <div class="logo-line"></div>
    </div>

    <nav class="nav">
      <div class="nav-section">System</div>
      <div class="nav-item active" data-page="dashboard" onclick="navTo('dashboard',this)">
        <span class="nav-icon">◈</span> Dashboard
      </div>
      <div class="nav-item" data-page="console" onclick="navTo('console',this)">
        <span class="nav-icon">_</span> Console
      </div>
      <div class="nav-item" data-page="files" onclick="navTo('files',this)">
        <span class="nav-icon">≡</span> File Manager
      </div>
      <div class="nav-section" style="margin-top:12px">Configure</div>
      <div class="nav-item" data-page="env" onclick="navTo('env',this)">
        <span class="nav-icon">⊛</span> Environment
      </div>
      <div class="nav-item" data-page="settings" onclick="navTo('settings',this)">
        <span class="nav-icon">⚙</span> Settings
      </div>
      <div class="nav-item" data-page="resources" onclick="navTo('resources',this)">
        <span class="nav-icon">▣</span> Resources
      </div>
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
          <div class="panel-title">
            <div class="panel-icon">▶</div>
            Launch Control
          </div>
          <span class="panel-tag">Operations</span>
        </div>
        <div class="panel-body">
          <div style="max-width:380px;margin-bottom:20px">
            <div class="form-group" style="margin:0">
              <label class="form-label">Startup File</label>
              <input class="form-input" id="sfInput" value="main.py" placeholder="main.py">
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
        <div class="terminal" id="miniTerm" style="height:230px"></div>
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
        <div class="terminal" id="mainTerm" style="height:470px"></div>
        <div class="term-input-wrap">
          <span class="term-prompt">❯</span>
          <input class="term-input" id="termIn" placeholder="Send to stdin..." onkeydown="if(event.key==='Enter')sendInput()">
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
            <thead>
              <tr>
                <th>Filename</th><th>Type</th><th>Size</th><th>Modified</th><th>Actions</th>
              </tr>
            </thead>
            <tbody id="fileList"></tbody>
          </table>
        </div>
        <div style="padding:16px;border-top:1px solid var(--border)">
          <div class="drop-zone" id="dropZone">
            <span class="drop-icon">⇪</span>
            <div class="drop-title">DROP FILES HERE</div>
            <div class="drop-sub">ZIP auto-extracted · folder structure preserved</div>
            <div style="display:flex;gap:10px;justify-content:center;flex-wrap:wrap;position:relative;z-index:5">
              <button class="btn btn-cyan" onclick="event.stopPropagation();document.getElementById('fileUploadInput').click()">⇪ Upload Files</button>
              <button class="btn btn-purple" onclick="event.stopPropagation();document.getElementById('folderUploadInput').click()">📁 Upload Folder</button>
            </div>
          </div>
          <div id="uploadProgress" style="margin-top:10px"></div>
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
          <p style="font-family:var(--font-mono);font-size:11px;color:var(--text-2);margin-bottom:20px">Injected into the process environment at startup.</p>
          <div class="env-row" style="margin-bottom:10px">
            <span style="font-family:var(--font-mono);font-size:9px;letter-spacing:3px;color:var(--text-3);text-transform:uppercase">KEY</span>
            <span style="font-family:var(--font-mono);font-size:9px;letter-spacing:3px;color:var(--text-3);text-transform:uppercase">VALUE</span>
            <span></span>
          </div>
          <div id="envRows"></div>
          <button class="btn btn-ghost btn-sm" onclick="addEnvRow('','')" style="margin-top:14px">+ Add Variable</button>
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
            <div class="form-group"><label class="form-label">Startup File</label><input class="form-input" id="stStartup" placeholder="main.py"></div>
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
              <div style="display:flex;gap:10px">
                <input class="form-input" id="newSubuser" placeholder="Enter username...">
                <button class="btn btn-purple" onclick="addSubuser()">Grant</button>
              </div>
            </div>
            <div id="subuserList"></div>
          </div>
        </div>
      </div>
      <div class="danger-zone" id="dangerZoneSection">
        <div style="font-family:var(--font-mono);font-size:10px;font-weight:700;letter-spacing:2px;color:var(--red);margin-bottom:8px;text-transform:uppercase">⚠ Danger Zone</div>
        <div style="font-size:13px;color:var(--text-2);margin-bottom:14px">Permanently destroys this instance and all associated files. This cannot be undone.</div>
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
            <div class="res-header">
              <span class="res-label">CPU Utilization</span>
              <span class="res-value sv-cyan" id="rCpu">—</span>
            </div>
            <div class="res-track"><div class="res-fill" id="pCpu" style="width:0%;background:linear-gradient(90deg,var(--cyan),var(--blue))"></div></div>
          </div>
          <div class="res-item">
            <div class="res-header">
              <span class="res-label">Memory Usage</span>
              <span class="res-value sv-purple" id="rMem">—</span>
            </div>
            <div class="res-track"><div class="res-fill" id="pMem" style="width:0%;background:linear-gradient(90deg,var(--purple),var(--cyan))"></div></div>
          </div>
          <div class="res-item">
            <div class="res-header">
              <span class="res-label">Disk Usage</span>
              <span class="res-value sv-cyan" id="rDsk">—</span>
            </div>
            <div class="res-track"><div class="res-fill" id="pDsk" style="width:0%;background:linear-gradient(90deg,var(--cyan),var(--purple))"></div></div>
          </div>
        </div>
      </div>
    </div>
  </main>

  <!-- MOBILE BOTTOM NAV -->
  <nav class="mobile-bottom-nav">
    <div class="m-nav-item" onclick="toggleSidebar()">
      <span class="m-nav-icon">▤</span>
      <span class="m-nav-label">Bots</span>
    </div>
    <div class="m-nav-item active" data-page="dashboard" onclick="navTo('dashboard',this)">
      <span class="m-nav-icon">◈</span>
      <span class="m-nav-label">Dash</span>
    </div>
    <div class="m-nav-item" data-page="console" onclick="navTo('console',this)">
      <span class="m-nav-icon">_</span>
      <span class="m-nav-label">Terminal</span>
    </div>
    <div class="m-nav-item" data-page="files" onclick="navTo('files',this)">
      <span class="m-nav-icon">≡</span>
      <span class="m-nav-label">Files</span>
    </div>
    <div class="m-nav-item" data-page="settings" onclick="navTo('settings',this)">
      <span class="m-nav-icon">⚙</span>
      <span class="m-nav-label">Config</span>
    </div>
  </nav>
</div>

<div class="toast-tray" id="toastTray"></div>

<!-- CREATE MODAL -->
<div class="modal-veil" id="mCreate">
  <div class="modal-box">
    <div class="modal-title">DEPLOY <span class="modal-title-accent">INSTANCE</span></div>
    <div class="form-group"><label class="form-label">Instance Name</label><input class="form-input" id="mName" placeholder="Project Alpha"></div>
    <div class="form-group"><label class="form-label">Startup File</label><input class="form-input" id="mFile" value="main.py" placeholder="main.py"></div>
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
    <div class="form-group"><label class="form-label">Initial Content</label><textarea class="form-textarea" id="nfContent" placeholder="# Start writing..." style="height:150px"></textarea></div>
    <div class="modal-footer">
      <button class="btn btn-ghost" onclick="closeModal('mNewFile')">Cancel</button>
      <button class="btn btn-cyan" onclick="createNewFile()">Create</button>
    </div>
  </div>
</div>

<!-- RENAME MODAL -->
<div class="modal-veil" id="mRename">
  <div class="modal-box">
    <div class="modal-title">RENAME <span class="modal-title-accent">FILE</span></div>
    <div class="form-group">
      <label class="form-label">Current Name</label>
      <div id="rnOldName" style="font-family:var(--font-mono);font-size:13px;color:var(--text-2);padding:10px 14px;background:rgba(0,0,0,0.4);border:1px solid var(--border);border-radius:6px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"></div>
    </div>
    <div class="form-group">
      <label class="form-label">New Name</label>
      <input class="form-input" id="rnNewName" placeholder="new-filename.py" onkeydown="if(event.key==='Enter')renameFile()">
    </div>
    <div class="modal-footer">
      <button class="btn btn-ghost" onclick="closeModal('mRename')">Cancel</button>
      <button class="btn btn-amber" onclick="renameFile()">Rename</button>
    </div>
  </div>
</div>

<script>
const sock=io({transports:['websocket','polling']});
let curBot=null,botRegistry={},startTimes={},uptimeIv=null,resIv=null,currentUser="",authMode='login';
setInterval(()=>document.getElementById('clock').textContent=new Date().toTimeString().slice(0,8),1000);
sock.on('connect',()=>console.log('[WS] Connected'));
sock.on('console_log',({bot_id,msg,level})=>{if(bot_id===curBot)appendLog(msg,level)});
sock.on('status_update',({bot_id,status,start_time})=>{
  if(botRegistry[bot_id])botRegistry[bot_id].status=status;
  renderBotList();if(bot_id===curBot)applyStatus(status);
  if(status==='online'&&start_time){startTimes[bot_id]=start_time*1000;startUptime()}else delete startTimes[bot_id];
});
function toggleSidebar(){
  const sb=document.querySelector('.sidebar');sb.classList.toggle('open');
  const o=document.querySelector('.sidebar-overlay');
  if(sb.classList.contains('open')){o.style.display='block';void o.offsetWidth;o.classList.add('open')}
  else{o.classList.remove('open');setTimeout(()=>o.style.display='none',300)}
}
const PAGE_NAMES={dashboard:'DASHBOARD',console:'CONSOLE',files:'FILE MANAGER',env:'ENVIRONMENT',settings:'SETTINGS',resources:'RESOURCES'};
function navTo(name,el){
  document.querySelectorAll('.sidebar .nav-item').forEach(n=>n.classList.remove('active'));
  const d=document.querySelector(`.sidebar .nav-item[data-page="${name}"]`);if(d)d.classList.add('active');
  document.querySelectorAll('.mobile-bottom-nav .m-nav-item').forEach(n=>n.classList.remove('active'));
  const m=document.querySelector(`.mobile-bottom-nav .m-nav-item[data-page="${name}"]`);if(m)m.classList.add('active');
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
  const p=document.getElementById('page-'+name);if(p)p.classList.add('active');
  document.getElementById('tbPage').textContent=PAGE_NAMES[name]||name.toUpperCase();
  if(name==='files')loadFiles();if(name==='env')loadEnv();if(name==='settings')loadSettings();
  if(name==='resources')startRes();else stopRes();
}
async function apiFetch(url,opts={}){
  try{const r=await fetch(url,opts);if(r.status===401){document.getElementById('loginOverlay').style.display='flex';return null}return r}
  catch(e){toast('Network error','error');return null}
}
async function checkAuth(){
  const r=await fetch('/api/me');if(r.status===401){document.getElementById('loginOverlay').style.display='flex';return false}
  const data=await r.json();currentUser=data.username;document.getElementById('loginOverlay').style.display='none';return true;
}
function switchAuthMode(mode){
  authMode=mode;
  document.getElementById('tabLogin').classList.toggle('active',mode==='login');
  document.getElementById('tabRegister').classList.toggle('active',mode==='register');
  document.getElementById('authBtn').textContent=mode==='login'?'AUTHENTICATE':'CREATE ACCOUNT';
}
async function submitAuth(){
  const u=document.getElementById('authUsername').value.trim(),p=document.getElementById('authPassword').value;
  if(!u||!p){toast('Credentials required','error');return}
  const ep=authMode==='login'?'/api/login':'/api/register';
  const r=await fetch(ep,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:u,password:p})});
  const res=await r.json();if(r.ok)location.reload();else toast(res.error||'Authentication failed','error');
}
async function logout(){await fetch('/api/logout',{method:'POST'});location.reload()}
async function loadBots(){
  const r=await apiFetch('/api/bots');if(!r)return;botRegistry=await r.json();
  Object.entries(botRegistry).forEach(([id,b])=>{if(b.status==='online'&&b.start_time)startTimes[id]=b.start_time*1000});
  renderBotList();document.getElementById('botCount').textContent=Object.keys(botRegistry).length;
  if(Object.keys(botRegistry).length>0&&!curBot)selectBot(Object.keys(botRegistry)[0]);
}
function renderBotList(){
  const el=document.getElementById('botList');el.innerHTML='';
  const entries=Object.entries(botRegistry);
  if(!entries.length){el.innerHTML='<div style="padding:20px;text-align:center;color:var(--text-3);font-family:var(--font-mono);font-size:10px;letter-spacing:2px">NO INSTANCES</div>';return}
  entries.forEach(([id,b])=>{
    const d=document.createElement('div');d.className='bot-item'+(id===curBot?' active':'');
    const s=b.status||'offline',sh=b.is_shared?`<span class="bot-shared-tag">shared</span>`:'';
    d.innerHTML=`<div class="bot-dot ${s}"></div><div style="flex:1;min-width:0"><div class="bot-name">${escH(b.name||id)}</div><div class="bot-status ${s}">${s}</div></div>${sh}`;
    d.onclick=()=>{selectBot(id);if(window.innerWidth<=860&&document.querySelector('.sidebar').classList.contains('open'))toggleSidebar()};
    el.appendChild(d);
  });
}
function selectBot(id){
  curBot=id;const b=botRegistry[id];
  document.getElementById('tbBot').textContent=b?.name||id;
  document.getElementById('sfInput').value=b?.startup_file||'main.py';
  document.getElementById('termTitle').textContent=(b?.name||id).toUpperCase()+' // STDOUT';
  ['mainTerm','miniTerm'].forEach(i=>document.getElementById(i).innerHTML='');
  applyStatus(b?.status||'offline');renderBotList();loadBotLogs();startUptime();
  if(b&&b.is_shared){document.getElementById('accessMgmtTitle').style.display='none';document.getElementById('accessMgmtSection').style.display='none';document.getElementById('dangerZoneSection').style.display='none'}
  else{document.getElementById('accessMgmtTitle').style.display='';document.getElementById('accessMgmtSection').style.display='';document.getElementById('dangerZoneSection').style.display='';if(document.getElementById('page-settings').classList.contains('active'))loadSettings()}
}
async function loadBotLogs(){
  if(!curBot)return;const r=await apiFetch(`/api/bot/${curBot}/logs`);if(!r)return;
  const logs=await r.json();['mainTerm','miniTerm'].forEach(id=>document.getElementById(id).innerHTML='');
  logs.forEach(({msg,level,time:ts})=>appendLog(msg,level,ts));
}
function applyStatus(s){
  const on=s==='online';
  document.getElementById('statusTag').className='status-badge '+(on?'online':'offline');
  document.getElementById('statusText').textContent=on?'ONLINE':'OFFLINE';
  document.getElementById('sStat').textContent=on?'ONLINE':'OFFLINE';
  document.getElementById('sStat').className='stat-value '+(on?'sv-green':'sv-red');
  document.getElementById('sStatSub').textContent=on?'Process running':'Process halted';
  if(!on)document.getElementById('sUptime').textContent='—';
}
function openCreateModal(){document.getElementById('mCreate').classList.add('open');setTimeout(()=>document.getElementById('mName').focus(),80)}
function closeModal(id){document.getElementById(id).classList.remove('open')}
document.querySelectorAll('.modal-veil').forEach(m=>m.addEventListener('click',e=>{if(e.target===m)m.classList.remove('open')}));
async function createBot(){
  const n=document.getElementById('mName').value.trim(),f=document.getElementById('mFile').value.trim()||'main.py';
  if(!n){toast('Instance name required','error');return}
  const r=await apiFetch('/api/bots',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:n,startup_file:f})});if(!r)return;
  const b=await r.json();botRegistry[b.id]=b;closeModal('mCreate');document.getElementById('mName').value='';
  renderBotList();document.getElementById('botCount').textContent=Object.keys(botRegistry).length;selectBot(b.id);toast('Instance deployed','success');
}
async function startBot(){if(!curBot)return;const sf=document.getElementById('sfInput').value.trim()||'main.py';await apiFetch(`/api/bot/${curBot}/start`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({startup_file:sf})});toast('Booting…','info')}
async function stopBot(){if(!curBot)return;await apiFetch(`/api/bot/${curBot}/stop`,{method:'POST'});toast('Stopped','success')}
async function restartBot(){if(!curBot)return;await apiFetch(`/api/bot/${curBot}/stop`,{method:'POST'});toast('Rebooting…','info');setTimeout(startBot,800)}
async function killBot(){if(!curBot)return;await apiFetch(`/api/bot/${curBot}/kill`,{method:'POST'});toast('Force killed','error')}
async function deleteBot(){
  if(!curBot||!confirm('Permanently destroy this instance?'))return;
  await apiFetch(`/api/bot/${curBot}`,{method:'DELETE'});delete botRegistry[curBot];curBot=null;
  document.getElementById('tbBot').textContent='— SELECT INSTANCE —';
  ['mainTerm','miniTerm'].forEach(i=>document.getElementById(i).innerHTML='');
  applyStatus('offline');renderBotList();document.getElementById('botCount').textContent=Object.keys(botRegistry).length;toast('Instance destroyed','error');
}
function appendLog(msg,level,ts){
  const tagMap={system:'sys',error:'err',success:'ok',warn:'warn',default:'out'};
  const tag=tagMap[level]||'out',t=ts||new Date().toTimeString().slice(0,8);
  const row=`<div class="log-row"><span class="log-ts">${escH(t)}</span><span class="log-tag ${tag}">${tag}</span><span class="log-msg ${tag}">${escH(msg)}</span></div>`;
  ['mainTerm','miniTerm'].forEach(id=>{const el=document.getElementById(id);if(el){el.innerHTML+=row;el.scrollTop=el.scrollHeight}});
}
function clearConsole(){['mainTerm','miniTerm'].forEach(id=>document.getElementById(id).innerHTML='');toast('Console cleared','info')}
function exportLogs(){
  const lines=Array.from(document.getElementById('mainTerm').querySelectorAll('.log-row')).map(r=>r.textContent.trim()).join('\n');
  const a=document.createElement('a');a.href='data:text/plain;charset=utf-8,'+encodeURIComponent(lines);a.download=`${curBot||'vortex'}-${Date.now()}.log`;a.click();toast('Logs exported','success');
}
async function sendInput(){
  if(!curBot)return;let v=document.getElementById('termIn').value;document.getElementById('termIn').value='';
  v=v.replace(/\x1b\[[0-9;]*[a-zA-Z]/g,'').replace(/[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f]/g,'');
  await apiFetch(`/api/bot/${curBot}/input`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({input:v+'\n'})});
}
const EXT_COLORS={py:'#00FF7F',js:'#FFB800',json:'#00E5FF',md:'#A020F0',txt:'#5A7A9A',sh:'#00E5FF',zip:'#FF2055',env:'#FFB800',ts:'#00E5FF',html:'#FF7043',css:'#00BCD4'};
const EXT_ICONS={py:'🐍',js:'⚡',json:'{}',txt:'≡',md:'#',zip:'⊞',env:'⊛',sh:'$',ts:'⟨⟩',html:'<>',css:'◐'};
async function loadFiles(){
  const tb=document.getElementById('fileList');
  if(!curBot){tb.innerHTML=`<tr><td colspan="5"><div style="padding:40px;text-align:center;color:var(--text-3);font-family:var(--font-mono);font-size:11px">NO INSTANCE TARGETED</div></td></tr>`;return}
  const r=await apiFetch(`/api/bot/${curBot}/files`);if(!r)return;const files=await r.json();
  if(!files.length){tb.innerHTML=`<tr><td colspan="5"><div style="padding:40px;text-align:center;color:var(--text-3);font-family:var(--font-mono);font-size:11px">DIRECTORY EMPTY</div></td></tr>`;return}
  tb.innerHTML=files.map(f=>{
    const ext=f.name.split('.').pop().toLowerCase(),c=EXT_COLORS[ext]||'#5A7A9A',ic=EXT_ICONS[ext]||'□',jn=JSON.stringify(f.name);
    const rid='rn_'+Math.random().toString(36).slice(2);
    return `<tr id="row_${rid}">
      <td><div class="file-name-cell">
        <div class="file-name-link" id="lnk_${rid}" onclick='editFile(${jn})'><span style="font-size:14px;opacity:.8;flex-shrink:0">${ic}</span><span class="file-name-text" title="${escH(f.name)}">${escH(f.name)}</span></div>
        <div class="file-rename-wrap" id="rnw_${rid}">
          <input class="file-rename-input" id="rni_${rid}" value="${escH(f.name.split('/').pop())}" onkeydown="if(event.key==='Enter')commitInlineRename('${rid}',${jn});if(event.key==='Escape')cancelInlineRename('${rid}')">
          <button class="file-rename-confirm" onclick="commitInlineRename('${rid}',${jn})">✓ Rename</button>
          <button class="file-rename-cancel" onclick="cancelInlineRename('${rid}')">✕</button>
        </div>
      </div></td>
      <td><span class="file-ext-badge" style="color:${c};border-color:${c}30">.${ext}</span></td>
      <td style="color:var(--text-2)">${escH(f.size)}</td>
      <td style="color:var(--text-3);font-size:11px">${escH(f.modified)}</td>
      <td><div class="btn-row" style="flex-wrap:nowrap;gap:5px">
        <button class="btn btn-ghost btn-sm" onclick='editFile(${jn})' title="Edit" style="padding:5px 9px">✏</button>
        <button class="btn btn-amber btn-sm" id="rnbtn_${rid}" onclick="toggleInlineRename('${rid}')" title="Rename" style="padding:5px 9px">⟳</button>
        <button class="btn btn-ghost btn-sm" onclick='dlFile(${jn})' title="Download" style="padding:5px 9px">↓</button>
        <button class="btn btn-red btn-sm" onclick='delFile(${jn})' title="Delete" style="padding:5px 9px">✕</button>
      </div></td>
    </tr>`;
  }).join('');
}
async function editFile(name){
  const r=await apiFetch(`/api/bot/${curBot}/file/${encodeURIComponent(name)}`);if(!r)return;const d=await r.json();
  document.getElementById('edName').textContent=name;document.getElementById('edContent').value=d.content;document.getElementById('edContent').dataset.fn=name;
  document.getElementById('mEditor').classList.add('open');
}
async function saveFile(){
  const name=document.getElementById('edContent').dataset.fn;if(!name)return;
  await apiFetch(`/api/bot/${curBot}/file/${encodeURIComponent(name)}`,{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({content:document.getElementById('edContent').value})});
  closeModal('mEditor');loadFiles();toast(`${name} saved`,'success');
}
function openNewFileModal(){if(!curBot)return;document.getElementById('mNewFile').classList.add('open')}
async function createNewFile(){
  const name=document.getElementById('nfName').value.trim();if(!name){toast('Filename required','error');return}
  await apiFetch(`/api/bot/${curBot}/file/${encodeURIComponent(name)}`,{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({content:document.getElementById('nfContent').value})});
  closeModal('mNewFile');document.getElementById('nfName').value='';document.getElementById('nfContent').value='';loadFiles();toast('File created','success');
}
async function delFile(name){if(!confirm(`Delete ${name}?`))return;await apiFetch(`/api/bot/${curBot}/file/${encodeURIComponent(name)}`,{method:'DELETE'});loadFiles();toast('File deleted','success')}
function dlFile(name){window.location.href=`/api/bot/${curBot}/file/${encodeURIComponent(name)}/download`}
function openRenameModal(name){
  if(!curBot)return;
  document.getElementById('rnOldName').textContent=name;
  const parts=name.split('/');const base=parts[parts.length-1];
  document.getElementById('rnNewName').value=base;
  document.getElementById('mRename').classList.add('open');
  setTimeout(()=>{const inp=document.getElementById('rnNewName');inp.focus();const dot=base.lastIndexOf('.');inp.setSelectionRange(0,dot>0?dot:base.length)},80);
}
async function renameFile(){
  const oldName=document.getElementById('rnOldName').textContent.trim();
  const newBase=document.getElementById('rnNewName').value.trim();
  if(!oldName||!newBase){toast('New name required','error');return}
  const parts=oldName.split('/');parts[parts.length-1]=newBase;
  const newName=parts.join('/');
  if(newName===oldName){closeModal('mRename');return}
  const r=await apiFetch(`/api/bot/${curBot}/file/${encodeURIComponent(oldName)}/rename`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({new_name:newName})});
  if(!r)return;
  const res=await r.json();
  if(res.error){toast(res.error,'error');return}
  closeModal('mRename');document.getElementById('rnNewName').value='';loadFiles();toast(`Renamed to ${newBase}`,'success');
}
function toggleInlineRename(rid){
  const lnk=document.getElementById('lnk_'+rid);
  const rnw=document.getElementById('rnw_'+rid);
  const btn=document.getElementById('rnbtn_'+rid);
  const active=rnw.classList.contains('active');
  if(active){cancelInlineRename(rid)}
  else{
    lnk.style.display='none';rnw.classList.add('active');
    btn.textContent='✕';btn.title='Cancel rename';
    const inp=document.getElementById('rni_'+rid);
    inp.focus();const v=inp.value;const dot=v.lastIndexOf('.');
    inp.setSelectionRange(0,dot>0?dot:v.length);
  }
}
function cancelInlineRename(rid){
  const lnk=document.getElementById('lnk_'+rid);
  const rnw=document.getElementById('rnw_'+rid);
  const btn=document.getElementById('rnbtn_'+rid);
  lnk.style.display='';rnw.classList.remove('active');
  btn.textContent='⟳';btn.title='Rename';
}
async function commitInlineRename(rid,oldName){
  const newBase=document.getElementById('rni_'+rid).value.trim();
  if(!newBase){toast('Name cannot be empty','error');return}
  const parts=oldName.split('/');parts[parts.length-1]=newBase;
  const newName=parts.join('/');
  if(newName===oldName){cancelInlineRename(rid);return}
  const r=await apiFetch(`/api/bot/${curBot}/file/${encodeURIComponent(oldName)}/rename`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({new_name:newName})});
  if(!r)return;const res=await r.json();
  if(res.error){toast(res.error,'error');return}
  loadFiles();toast(`Renamed to ${newBase}`,'success');
}

async function handleUpload(files,isFolder){
  const fileArr=files?Array.from(files):[];
  try{document.getElementById('fileUploadInput').value=''}catch(e){}
  try{document.getElementById('folderUploadInput').value=''}catch(e){}
  if(!curBot){toast('Select an instance first','error');return}
  if(!fileArr.length){toast('No files selected','error');return}
  const prog=document.getElementById('uploadProgress');if(!prog)return;
  let ok=0,fail=0;
  for(const file of fileArr){
    const relPath=(isFolder&&file.webkitRelativePath)?file.webkitRelativePath:file.name;
    const fd=new FormData();fd.append('file',file);fd.append('relative_path',relPath);
    const sid='up_'+Math.random().toString(36).slice(2);
    const wrap=document.createElement('div');wrap.className='upload-row';
    wrap.innerHTML=`<span style="color:var(--cyan);flex-shrink:0">⇪</span><span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:11px" title="${escH(relPath)}">${escH(relPath)}</span><div class="upload-bar-wrap"><div class="upload-bar-fill" id="${sid}" style="width:0%"></div></div><span id="${sid}st" style="font-size:10px;color:var(--text-3);flex-shrink:0;min-width:28px;text-align:right">0%</span>`;
    prog.appendChild(wrap);
    try{
      await new Promise((resolve,reject)=>{
        const xhr=new XMLHttpRequest();xhr.open('POST',`/api/bot/${curBot}/upload`);
        xhr.upload.addEventListener('progress',e=>{if(e.lengthComputable){const pct=Math.round(e.loaded/e.total*95);const b=document.getElementById(sid),s=document.getElementById(sid+'st');if(b)b.style.width=pct+'%';if(s)s.textContent=pct+'%'}});
        xhr.addEventListener('load',()=>{if(xhr.status===401){document.getElementById('loginOverlay').style.display='flex';reject(new Error('Unauthorized'));return}let resp={};try{resp=JSON.parse(xhr.responseText)}catch(e){}if(resp.error){reject(new Error(resp.error));return}if(xhr.status>=200&&xhr.status<300)resolve();else reject(new Error(`HTTP ${xhr.status}`))});
        xhr.addEventListener('error',()=>reject(new Error('Network error')));
        xhr.addEventListener('abort',()=>reject(new Error('Aborted')));
        xhr.send(fd);
      });
      const b=document.getElementById(sid),s=document.getElementById(sid+'st');
      if(b){b.style.width='100%';b.style.background='var(--green)'}if(s){s.textContent='✓';s.style.color='var(--green)'}
      ok++;setTimeout(()=>wrap.remove(),2500);
    }catch(err){
      const b=document.getElementById(sid),s=document.getElementById(sid+'st');
      if(b){b.style.width='100%';b.style.background='var(--red)'}if(s){s.textContent='✕';s.style.color='var(--red)'}
      wrap.style.borderColor='rgba(255,32,85,0.3)';fail++;
      toast(`Upload failed: ${err.message}`,'error');setTimeout(()=>wrap.remove(),5000);
    }
  }
  loadFiles();
  if(ok>0&&fail===0)toast(`${ok} file${ok>1?'s':''} uploaded`,'success');
  else if(ok>0&&fail>0)toast(`${ok} uploaded, ${fail} failed`,'info');
}

let _dzDepth=0;
document.addEventListener('dragenter',e=>{e.preventDefault();_dzDepth++;const dz=document.getElementById('dropZone');if(dz)dz.classList.add('dragging')});
document.addEventListener('dragleave',e=>{if(--_dzDepth<=0){_dzDepth=0;const dz=document.getElementById('dropZone');if(dz)dz.classList.remove('dragging')}});
document.addEventListener('dragover',e=>e.preventDefault());
document.addEventListener('drop',e=>{e.preventDefault();_dzDepth=0;const dz=document.getElementById('dropZone');if(dz)dz.classList.remove('dragging');if(e.dataTransfer.files.length)handleUpload(e.dataTransfer.files,false)});

async function loadEnv(){
  if(!curBot)return;const r=await apiFetch(`/api/bot/${curBot}/env`);if(!r)return;
  const env=await r.json();const c=document.getElementById('envRows');c.innerHTML='';
  const entries=Object.entries(env);if(entries.length)entries.forEach(([k,v])=>addEnvRow(k,v));else addEnvRow('','');
}
function addEnvRow(k='',v=''){
  const d=document.createElement('div');d.className='env-row';
  d.innerHTML=`<input class="env-field env-key" placeholder="KEY" value="${escH(k)}"><input class="env-field" placeholder="value" value="${escH(v)}"><button class="btn btn-red btn-sm" onclick="this.parentElement.remove()" style="padding:6px 10px">✕</button>`;
  document.getElementById('envRows').appendChild(d);
}
async function saveEnv(){
  if(!curBot)return;const env={};
  document.querySelectorAll('.env-row').forEach(r=>{const k=r.querySelector('.env-key')?.value.trim(),v=r.querySelectorAll('.env-field')[1]?.value;if(k)env[k]=v||''});
  await apiFetch(`/api/bot/${curBot}/env`,{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(env)});toast('Environment saved','success');
}
async function loadSettings(){
  if(!curBot)return;const b=botRegistry[curBot]||{};
  document.getElementById('stName').value=b.name||'';document.getElementById('stStartup').value=b.startup_file||'main.py';document.getElementById('stAR').value=b.auto_restart?'true':'false';
  if(!b.is_shared){const r=await apiFetch(`/api/bot/${curBot}/subusers`);if(r){const users=await r.json();const c=document.getElementById('subuserList');c.innerHTML='';users.forEach(u=>{const div=document.createElement('div');div.className='subuser-row';div.innerHTML=`<span style="color:var(--text-2)">${escH(u)}</span><button class="btn btn-red btn-sm" onclick="removeSubuser('${escH(u)}')">Revoke</button>`;c.appendChild(div)})}}
}
async function saveSettings(){
  if(!curBot)return;const data={name:document.getElementById('stName').value.trim(),startup_file:document.getElementById('stStartup').value.trim()||'main.py',auto_restart:document.getElementById('stAR').value==='true'};
  const r=await apiFetch(`/api/bot/${curBot}/settings`,{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});if(!r)return;
  const upd=await r.json();botRegistry[curBot]={...botRegistry[curBot],...upd};document.getElementById('tbBot').textContent=data.name||curBot;renderBotList();toast('Settings saved','success');
}
async function addSubuser(){
  if(!curBot)return;const u=document.getElementById('newSubuser').value.trim();if(!u)return;
  const r=await apiFetch(`/api/bot/${curBot}/subusers`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:u})});
  if(r&&r.ok){document.getElementById('newSubuser').value='';loadSettings();toast(`Access granted to ${u}`,'success')}else toast('User not found','error');
}
async function removeSubuser(u){
  if(!curBot)return;const r=await apiFetch(`/api/bot/${curBot}/subusers/${encodeURIComponent(u)}`,{method:'DELETE'});
  if(r&&r.ok){loadSettings();toast(`Access revoked for ${u}`,'success')}
}
function startUptime(){
  clearInterval(uptimeIv);
  uptimeIv=setInterval(()=>{if(curBot&&startTimes[curBot]){const s=Math.floor((Date.now()-startTimes[curBot])/1000),h=Math.floor(s/3600),m=Math.floor(s%3600/60),sec=s%60;document.getElementById('sUptime').textContent=`${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}:${String(sec).padStart(2,'0')}`}},1000);
}
function startRes(){stopRes();fetchRes();resIv=setInterval(fetchRes,3000)}
function stopRes(){clearInterval(resIv)}
async function fetchRes(){
  try{const r=await fetch('/api/resources');if(!r.ok)return;const d=await r.json();
    document.getElementById('rCpu').textContent=d.cpu+'%';document.getElementById('pCpu').style.width=d.cpu+'%';
    document.getElementById('rMem').textContent=d.mem_used;document.getElementById('pMem').style.width=d.mem_pct+'%';
    document.getElementById('rDsk').textContent=d.disk_pct+'%';document.getElementById('pDsk').style.width=d.disk_pct+'%';
    document.getElementById('sCpu').textContent=d.cpu+'%';document.getElementById('sMem').textContent=d.mem_used;
  }catch(e){}
}
function escH(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;')}
function toast(msg,type='success'){
  const tray=document.getElementById('toastTray');
  const icons={success:'✓',error:'✕',info:'i'};
  const t=document.createElement('div');t.className=`toast ${type}`;
  t.innerHTML=`<div class="toast-icon">${icons[type]||'·'}</div><span>${escH(msg)}</span><span class="toast-close" onclick="this.parentElement.remove()">✕</span>`;
  tray.appendChild(t);
  setTimeout(()=>{t.style.transition='all .4s';t.style.opacity='0';t.style.transform='translateX(20px)';setTimeout(()=>t.remove(),400)},3200);
}
checkAuth().then(ok=>{if(ok){loadBots();fetchRes();setInterval(fetchRes,5000)}});
</script>
</body>
</html>"""


@socketio.on('connect')
def handle_connect():
    user = session.get('username')
    if user:
        join_room(user)
        log.info(f'WS connected: {user}')

@app.route('/')
def index():
    return render_template_string(HTML)

@app.route('/api/login', methods=['POST'])
def login():
    data = request.json or {}
    username = data.get('username', '').strip()
    password = data.get('password', '')
    if not username or not password: return jsonify({'error': 'Credentials required'}), 400
    users = load_users()
    if username not in users: return jsonify({'error': 'User not found'}), 401
    if users[username]['pwd'] != password: return jsonify({'error': 'Invalid password'}), 401
    session['username'] = username
    return jsonify({'ok': True})

@app.route('/api/register', methods=['POST'])
def register():
    data = request.json or {}
    username = data.get('username', '').strip()
    password = data.get('password', '')
    if not username or not password: return jsonify({'error': 'Credentials required'}), 400
    users = load_users()
    if username in users: return jsonify({'error': 'Username already exists'}), 400
    users[username] = {'pwd': password}
    save_users(users)
    session['username'] = username
    return jsonify({'ok': True})

@app.route('/api/logout', methods=['POST'])
def do_logout():
    session.clear()
    return jsonify({'ok': True})

@app.route('/api/me')
def me():
    if 'username' in session: return jsonify({'username': session['username']})
    return jsonify({'error': 'unauthorized'}), 401

@app.route('/api/bots')
def get_bots():
    user = session.get('username')
    if not user: return jsonify({'error': 'unauth'}), 401
    cfg = load_config(); out = {}
    for bid, bc in cfg.items():
        owner = bc.get('owner'); shared = bc.get('shared_with', [])
        if owner == user or user in shared:
            running = is_running(bid)
            out[bid] = {'id': bid, 'name': bc.get('name', bid), 'startup_file': bc.get('startup_file', 'main.py'),
                'status': 'online' if running else 'offline', 'auto_restart': bc.get('auto_restart', False),
                'start_time': bots.get(bid, {}).get('start_time') if running else None, 'is_shared': owner != user}
    return jsonify(out)

@app.route('/api/bots', methods=['POST'])
def create_bot_route():
    user = session.get('username')
    if not user: return jsonify({'error': 'unauth'}), 401
    data = request.json or {}; bid = f"bot_{int(time.time() * 1000)}"
    cfg = load_config()
    cfg[bid] = {'name': data.get('name', 'New Instance'), 'startup_file': data.get('startup_file', 'main.py'),
        'auto_restart': False, 'env': {}, 'owner': user, 'shared_with': []}
    save_config(cfg); get_bot_dir(bid)
    return jsonify({'id': bid, **cfg[bid], 'status': 'offline', 'is_shared': False})

@app.route('/api/bot/<bid>', methods=['DELETE'])
def del_bot(bid):
    if not check_owner(bid): return jsonify({'error': 'unauth'}), 401
    stop_bot(bid); cfg = load_config(); cfg.pop(bid, None); save_config(cfg)
    bd = os.path.join(BOTS_DIR, bid)
    if os.path.exists(bd): shutil.rmtree(bd)
    bots.pop(bid, None)
    return jsonify({'ok': True})

@app.route('/api/bot/<bid>/subusers', methods=['GET'])
def get_subusers(bid):
    if not check_owner(bid): return jsonify({'error': 'unauth'}), 401
    return jsonify(load_config().get(bid, {}).get('shared_with', []))

@app.route('/api/bot/<bid>/subusers', methods=['POST'])
def add_subuser(bid):
    if not check_owner(bid): return jsonify({'error': 'unauth'}), 401
    target = (request.json or {}).get('username', '').strip()
    if target not in load_users(): return jsonify({'error': 'User does not exist'}), 404
    cfg = load_config(); shared = cfg.get(bid, {}).get('shared_with', [])
    if target not in shared and target != cfg[bid]['owner']:
        shared.append(target); cfg[bid]['shared_with'] = shared; save_config(cfg)
    return jsonify({'ok': True})

@app.route('/api/bot/<bid>/subusers/<user>', methods=['DELETE'])
def remove_subuser(bid, user):
    if not check_owner(bid): return jsonify({'error': 'unauth'}), 401
    cfg = load_config(); shared = cfg.get(bid, {}).get('shared_with', [])
    if user in shared: shared.remove(user); cfg[bid]['shared_with'] = shared; save_config(cfg)
    return jsonify({'ok': True})

@app.route('/api/bot/<bid>/start', methods=['POST'])
def start_route(bid):
    if not check_access(bid): return jsonify({'error': 'unauth'}), 401
    sf = (request.json or {}).get('startup_file')
    threading.Thread(target=start_bot, args=(bid, sf), daemon=True).start()
    return jsonify({'ok': True})

@app.route('/api/bot/<bid>/stop', methods=['POST'])
def stop_route(bid):
    if not check_access(bid): return jsonify({'error': 'unauth'}), 401
    stop_bot(bid); return jsonify({'ok': True})

@app.route('/api/bot/<bid>/kill', methods=['POST'])
def kill_route(bid):
    if not check_access(bid): return jsonify({'error': 'unauth'}), 401
    if bid in bots and bots[bid].get('process'):
        try: bots[bid]['process'].kill(); emit_log(bid, '[System] Force killed.', 'error')
        except Exception: pass
    return jsonify({'ok': True})

@app.route('/api/bot/<bid>/input', methods=['POST'])
def input_route(bid):
    if not check_access(bid): return jsonify({'error': 'unauth'}), 401
    inp = (request.json or {}).get('input', '')
    if len(inp) > 4096: return jsonify({'error': 'input too long'}), 400
    if bid in bots and bots[bid].get('process'):
        p = bots[bid]['process']
        if p.poll() is None and p.stdin:
            try: p.stdin.write(inp); p.stdin.flush()
            except Exception: pass
    return jsonify({'ok': True})

@app.route('/api/bot/<bid>/logs')
def logs_route(bid):
    if not check_access(bid): return jsonify({'error': 'unauth'}), 401
    mem = bots.get(bid, {}).get('logs', [])
    if mem: return jsonify(mem)
    lf = os.path.join(get_bot_dir(bid), 'system.log'); disk = []
    if os.path.exists(lf):
        try:
            with open(lf, 'r', encoding='utf-8') as f:
                for line in f.readlines()[-500:]:
                    try: disk.append(json.loads(line.strip()))
                    except Exception: pass
        except Exception: pass
    return jsonify(disk)

@app.route('/api/bot/<bid>/files')
def files_route(bid):
    if not check_access(bid): return jsonify({'error': 'unauth'}), 401
    bd = get_bot_dir(bid); out = []
    for root, dirs, files in os.walk(bd):
        for f in files:
            if f == 'system.log': continue
            fp = os.path.join(root, f); rel = os.path.relpath(fp, bd).replace('\\', '/')
            sz = os.path.getsize(fp)
            s = f"{sz}B" if sz < 1024 else f"{sz//1024}KB" if sz < 1024**2 else f"{sz//1024//1024}MB"
            out.append({'name': rel, 'size': s, 'modified': time.strftime('%Y-%m-%d %H:%M', time.localtime(os.path.getmtime(fp)))})
    return jsonify(out)

@app.route('/api/bot/<bid>/file/<path:fn>', methods=['GET'])
def get_file(bid, fn):
    if not check_access(bid): return jsonify({'error': 'unauth'}), 401
    fp = safe_path(bid, fn)
    if not fp: return jsonify({'error': 'invalid path'}), 403
    if not os.path.exists(fp): return jsonify({'content': ''})
    try: return jsonify({'content': open(fp, encoding='utf-8', errors='replace').read()})
    except Exception: return jsonify({'content': '[Binary — cannot display]'})

@app.route('/api/bot/<bid>/file/<path:fn>', methods=['PUT'])
def put_file(bid, fn):
    if not check_access(bid): return jsonify({'error': 'unauth'}), 401
    fp = safe_path(bid, fn)
    if not fp: return jsonify({'error': 'invalid path'}), 403
    os.makedirs(os.path.dirname(fp), exist_ok=True)
    with open(fp, 'w', encoding='utf-8') as f: f.write((request.json or {}).get('content', ''))
    return jsonify({'ok': True})

@app.route('/api/bot/<bid>/file/<path:fn>', methods=['DELETE'])
def del_file(bid, fn):
    if not check_access(bid): return jsonify({'error': 'unauth'}), 401
    fp = safe_path(bid, fn)
    if not fp: return jsonify({'error': 'invalid path'}), 403
    if os.path.exists(fp): os.remove(fp)
    return jsonify({'ok': True})

@app.route('/api/bot/<bid>/file/<path:fn>/rename', methods=['POST'])
def rename_file(bid, fn):
    if not check_access(bid): return jsonify({'error': 'unauth'}), 401
    new_name = (request.json or {}).get('new_name', '').strip()
    if not new_name: return jsonify({'error': 'new_name required'}), 400
    src = safe_path(bid, fn)
    dst = safe_path(bid, new_name)
    if not src: return jsonify({'error': 'invalid source path'}), 403
    if not dst: return jsonify({'error': 'invalid destination path'}), 403
    if not os.path.exists(src): return jsonify({'error': 'source not found'}), 404
    if os.path.exists(dst): return jsonify({'error': 'a file with that name already exists'}), 409
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    os.rename(src, dst)
    return jsonify({'ok': True, 'new_name': new_name})

@app.route('/api/bot/<bid>/file/<path:fn>/download')
def dl_file(bid, fn):
    if not check_access(bid): return jsonify({'error': 'unauth'}), 401
    fp = safe_path(bid, fn)
    if not fp or not os.path.exists(fp): return jsonify({'error': 'not found'}), 404
    return send_file(fp, as_attachment=True)

@app.route('/api/bot/<bid>/upload', methods=['POST'])
def upload_route(bid):
    if not check_access(bid): return jsonify({'error': 'unauth'}), 401
    if 'file' not in request.files: return jsonify({'error': 'no file field in request'}), 400
    file = request.files['file']
    if not file or not file.filename: return jsonify({'error': 'no file selected'}), 400

    bd = get_bot_dir(bid)
    abs_bd = os.path.abspath(bd)

    raw_relative = (request.form.get('relative_path') or file.filename or '').strip()
    if not raw_relative:
        return jsonify({'error': 'could not determine filename'}), 400

    parts = [p for p in raw_relative.replace('\\', '/').split('/') if p and p != '.']
    safe_parts = []
    for p in parts:
        s = secure_filename(p)
        if s:
            safe_parts.append(s)
        else:
            cleaned = p.replace('..', '').replace('/', '').replace('\\', '').strip()
            if cleaned:
                safe_parts.append(cleaned)

    if not safe_parts:
        return jsonify({'error': 'invalid filename after sanitisation'}), 400

    if len(safe_parts) > 1:
        safe_parts = safe_parts[1:]

    rel_path = os.path.join(*safe_parts)
    dest = os.path.abspath(os.path.join(abs_bd, rel_path))

    if dest != abs_bd and not dest.startswith(abs_bd + os.sep):
        return jsonify({'error': 'path traversal blocked'}), 403

    try:
        parent = os.path.dirname(dest)
        if parent and parent != abs_bd:
            os.makedirs(parent, exist_ok=True)
        file.save(dest)
    except Exception as e:
        log.exception('Upload save failed')
        return jsonify({'error': f'Save failed: {e}'}), 500

    fname = safe_parts[-1]

    if fname.lower().endswith('.zip'):
        extracted, blocked = 0, 0
        try:
            with zipfile.ZipFile(dest, 'r') as zf:
                for info in zf.infolist():
                    if info.flag_bits & 0x1:
                        emit_log(bid, '[Error] ZIP is password-protected', 'error')
                        os.remove(dest); return jsonify({'error': 'password-protected zip'}), 400
                for m in zf.namelist():
                    mp = os.path.abspath(os.path.join(abs_bd, m))
                    if mp.startswith(abs_bd + os.sep):
                        os.makedirs(os.path.dirname(mp), exist_ok=True)
                        if not m.endswith('/'):
                            with zf.open(m) as src, open(mp, 'wb') as dst: dst.write(src.read())
                            extracted += 1
                    else:
                        log.warning(f'Blocked zip-slip: {m}'); blocked += 1
            os.remove(dest)
            msg = f'[System] Extracted {extracted} file(s) from {fname}'
            if blocked: msg += f' ({blocked} blocked)'
            emit_log(bid, msg, 'success' if extracted else 'warn')
        except zipfile.BadZipFile:
            emit_log(bid, f'[Error] {fname} is not a valid ZIP', 'error')
            if os.path.exists(dest): os.remove(dest)
            return jsonify({'error': 'bad zip'}), 400
        except Exception as e:
            emit_log(bid, f'[Error] ZIP extract failed: {e}', 'error')
            if os.path.exists(dest): os.remove(dest)
            return jsonify({'error': str(e)}), 500
    else:
        emit_log(bid, f'[System] Uploaded {rel_path}', 'system')

    return jsonify({'ok': True, 'filename': rel_path})

@app.route('/api/bot/<bid>/env')
def get_env(bid):
    if not check_access(bid): return jsonify({'error': 'unauth'}), 401
    return jsonify(load_config().get(bid, {}).get('env', {}))

@app.route('/api/bot/<bid>/env', methods=['PUT'])
def put_env(bid):
    if not check_access(bid): return jsonify({'error': 'unauth'}), 401
    cfg = load_config(); cfg.setdefault(bid, {})['env'] = request.json or {}; save_config(cfg)
    return jsonify({'ok': True})

@app.route('/api/bot/<bid>/settings', methods=['PUT'])
def put_settings(bid):
    if not check_access(bid): return jsonify({'error': 'unauth'}), 401
    data = request.json or {}; cfg = load_config(); bc = cfg.setdefault(bid, {})
    bc['name'] = data.get('name', bc.get('name', bid))
    bc['startup_file'] = data.get('startup_file', 'main.py')
    bc['auto_restart'] = bool(data.get('auto_restart', False))
    save_config(cfg)
    if bid in bots: bots[bid]['auto_restart'] = bc['auto_restart']
    return jsonify(bc)

@app.route('/api/resources')
def resources():
    cpu = psutil.cpu_percent(interval=0.3); mem = psutil.virtual_memory(); disk = psutil.disk_usage('/')
    def fmt(b): return f"{b//1024//1024}MB" if b < 1024**3 else f"{b/1024**3:.1f}GB"
    return jsonify({'cpu': round(cpu,1), 'mem_used': fmt(mem.used), 'mem_total': fmt(mem.total),
        'mem_pct': round(mem.percent,1), 'disk_used': fmt(disk.used), 'disk_total': fmt(disk.total), 'disk_pct': round(disk.percent,1)})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    print('\n' + '━' * 52)
    print(f'  VORTEX HOSTING v11.2  ·  mode={_ASYNC_MODE}  ·  port={port}')
    print('━' * 52 + '\n')
    if _ASYNC_MODE == 'eventlet':
        socketio.run(app, host='0.0.0.0', port=port, debug=False)
    else:
        socketio.run(app, host='0.0.0.0', port=port, debug=False, allow_unsafe_werkzeug=True)
