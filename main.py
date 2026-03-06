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
<link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@400;500;700;900&family=Rajdhani:wght@400;500;600;700&family=Fira+Code:wght@300;400;500;700&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
:root{--bg-void:#040508;--bg-panel:#0A0D14;--bg-glass:rgba(10,13,20,.85);--line-dim:rgba(0,240,255,.05);--line:rgba(0,240,255,.15);--line-bright:rgba(0,240,255,.3);--neon-cyan:#00F0FF;--neon-purple:#B000FF;--neon-blue:#0077FF;--amber:#FFB800;--green:#00FF66;--red:#FF0055;--text-main:#E0F2FE;--text-muted:#6A85B6;--text-dark:#3A4E7A;--font-mono:'Fira Code',monospace;--font-sans:'Rajdhani',sans-serif;--font-disp:'Orbitron',sans-serif;--anim-spring:cubic-bezier(.175,.885,.32,1.1);--anim-smooth:cubic-bezier(.25,.8,.25,1)}
html,body{width:100%;height:100%;overflow:hidden;background:var(--bg-void);color:var(--text-main);font-family:var(--font-sans)}
#app{display:flex;width:100%;height:100%;position:relative;z-index:1}
button,input,select,textarea,.clickable{cursor:pointer}
body::before{content:'';position:fixed;inset:0;z-index:-2;pointer-events:none;background-image:linear-gradient(var(--line-dim) 1px,transparent 1px),linear-gradient(90deg,var(--line-dim) 1px,transparent 1px);background-size:50px 50px;opacity:.6}
body::after{content:'';position:fixed;inset:0;z-index:-1;pointer-events:none;background:radial-gradient(circle at center,rgba(0,119,255,.08) 0%,transparent 60%)}
::-webkit-scrollbar{width:6px;height:6px}::-webkit-scrollbar-track{background:transparent}::-webkit-scrollbar-thumb{background:rgba(0,119,255,.5);border-radius:10px}::-webkit-scrollbar-thumb:hover{background:var(--neon-cyan)}
.sidebar{width:280px;min-width:280px;height:100%;display:flex;flex-direction:column;position:relative;z-index:9500;background:var(--bg-glass);backdrop-filter:blur(25px);-webkit-backdrop-filter:blur(25px);border-right:1px solid var(--line);box-shadow:5px 0 30px rgba(0,0,0,.8);transition:transform .4s var(--anim-spring)}
.sidebar::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,transparent,var(--neon-cyan),transparent);box-shadow:0 2px 15px rgba(0,240,255,.5)}
.sidebar-close-btn{display:none;position:absolute;right:20px;top:35px;background:none;border:none;color:var(--text-muted);font-size:22px;cursor:pointer;transition:color .2s;z-index:1000}
.sidebar-close-btn:hover{color:var(--neon-cyan)}
.logo{padding:30px 24px 20px;border-bottom:1px solid var(--line);position:relative}
.logo::after{content:'';position:absolute;bottom:0;left:0;width:100%;height:2px;background:linear-gradient(90deg,var(--neon-cyan),var(--neon-purple));box-shadow:0 0 10px var(--neon-cyan)}
.logo-wordmark{font-family:var(--font-disp);font-size:32px;font-weight:900;letter-spacing:2px;line-height:1;background:linear-gradient(90deg,#fff,var(--neon-cyan));-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.nav{padding:24px 16px;flex-shrink:0;overflow-y:auto}
.nav-label{font-family:var(--font-mono);font-size:10px;font-weight:700;letter-spacing:4px;color:var(--text-dark);text-transform:uppercase;padding:10px 14px;display:flex;align-items:center;gap:10px}
.nav-label::after{content:'';flex:1;height:1px;background:var(--line)}
.nav-item{display:flex;align-items:center;gap:12px;padding:12px 16px;border-radius:6px;font-size:15px;font-weight:600;color:var(--text-muted);cursor:pointer;transition:all .3s;margin-bottom:6px;border:1px solid transparent}
.nav-item:hover{background:rgba(0,240,255,.05);color:var(--text-main);transform:translateX(4px);border-color:var(--line-dim)}
.nav-item.active{background:linear-gradient(90deg,rgba(0,240,255,.15),transparent);color:var(--neon-cyan);border-left:3px solid var(--neon-cyan)}
.nav-glyph{width:20px;font-family:var(--font-mono);font-size:16px;text-align:center;opacity:.5;transition:all .3s}
.nav-item.active .nav-glyph{opacity:1;text-shadow:0 0 12px var(--neon-cyan)}
.bot-section-header{display:flex;align-items:center;justify-content:space-between;padding:10px 24px 12px;flex-shrink:0}
.bot-section-label{font-family:var(--font-mono);font-size:11px;font-weight:700;letter-spacing:3px;color:var(--text-dark);text-transform:uppercase}
.bot-count-badge{background:rgba(0,240,255,.1);border:1px solid rgba(0,240,255,.3);border-radius:4px;padding:2px 8px;font-family:var(--font-mono);font-size:11px;color:var(--neon-cyan);font-weight:700}
.new-bot-btn{margin:0 16px 16px;display:flex;align-items:center;justify-content:center;gap:8px;padding:14px;border:1px dashed var(--line-bright);border-radius:6px;font-size:14px;font-weight:700;color:var(--text-muted);background:rgba(0,0,0,.4);transition:all .3s var(--anim-spring);font-family:var(--font-disp);letter-spacing:2px}
.new-bot-btn:hover{border-color:var(--neon-purple);color:var(--neon-purple);background:rgba(176,0,255,.1);transform:translateY(-2px)}
.bot-list{flex:1;overflow-y:auto;padding:0 16px 16px}
.bot-item{display:flex;align-items:center;gap:12px;padding:12px 14px;border-radius:6px;cursor:pointer;transition:all .3s;margin-bottom:8px;border:1px solid var(--line-dim);background:rgba(0,0,0,.4)}
.bot-item:hover{background:rgba(0,240,255,.05);border-color:var(--line);transform:translateY(-1px)}
.bot-item.active{background:rgba(0,240,255,.1);border-color:var(--neon-cyan)}
.bot-indicator{width:10px;height:10px;border-radius:50%;flex-shrink:0}
.bot-indicator.online{background:var(--green);box-shadow:0 0 12px var(--green);animation:pulse 2s ease-in-out infinite}
.bot-indicator.offline{background:var(--text-dark)}
@keyframes pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.6;transform:scale(.85)}}
.bot-name{font-size:15px;font-weight:600;color:var(--text-muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;transition:color .3s}
.bot-item.active .bot-name,.bot-item:hover .bot-name{color:#fff}
.bot-status-text{font-family:var(--font-mono);font-size:10px;font-weight:500;color:var(--text-dark);margin-top:3px;letter-spacing:1px;text-transform:uppercase}
.bot-item.active .bot-status-text{color:var(--neon-cyan)}
.bot-shared-icon{font-size:10px;margin-left:auto;color:var(--neon-purple);opacity:.7}
.sidebar-footer{padding:20px 24px;border-top:1px solid var(--line);display:flex;justify-content:space-between;align-items:center;background:rgba(0,0,0,.6)}
.footer-brand{font-family:var(--font-disp);font-size:14px;font-weight:700;letter-spacing:4px;color:var(--text-dark);cursor:pointer;transition:all .3s}
.footer-brand:hover{color:var(--red)}
.footer-clock{font-family:var(--font-mono);font-size:13px;font-weight:700;color:var(--neon-cyan)}
.mobile-bottom-nav{display:none;position:fixed;bottom:16px;left:16px;right:16px;height:70px;background:rgba(15,20,30,.85);backdrop-filter:blur(25px);-webkit-backdrop-filter:blur(25px);border:1px solid var(--line-bright);border-radius:20px;z-index:9000;box-shadow:0 10px 40px rgba(0,0,0,.8);justify-content:space-around;align-items:center;padding:0 10px}
.m-nav-item{display:flex;flex-direction:column;align-items:center;justify-content:center;gap:4px;color:var(--text-muted);cursor:pointer;transition:all .3s var(--anim-spring);padding:8px;flex:1}
.m-nav-glyph{font-family:var(--font-mono);font-size:22px;transition:transform .3s}
.m-nav-text{font-family:var(--font-sans);font-size:11px;font-weight:700;letter-spacing:1px;text-transform:uppercase}
.m-nav-item.active{color:var(--neon-cyan)}
.m-nav-item.active .m-nav-glyph{transform:translateY(-4px) scale(1.1)}
.sidebar-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:0;transition:opacity .3s;opacity:0;cursor:pointer}
.sidebar-overlay.open{display:block;opacity:1}
.main{flex:1;min-width:0;height:100%;display:flex;flex-direction:column;position:relative;z-index:10}
.topbar{height:75px;min-height:75px;background:var(--bg-glass);backdrop-filter:blur(20px);border-bottom:1px solid var(--line);display:flex;align-items:center;justify-content:space-between;padding:0 32px;gap:20px;z-index:20;box-shadow:0 10px 30px rgba(0,0,0,.8)}
.topbar-main{display:flex;align-items:center;gap:16px;min-width:0}
.mobile-nav-toggle{display:none;background:none;border:1px solid var(--line);color:var(--text-muted);padding:8px 12px;border-radius:6px;font-size:18px}
.tb-breadcrumb{display:flex;align-items:center;gap:12px;min-width:0}
.tb-section{font-family:var(--font-mono);font-size:12px;font-weight:700;letter-spacing:3px;color:var(--text-dark);text-transform:uppercase}
.tb-slash{color:var(--line-bright);font-weight:300}
.tb-page{font-family:var(--font-disp);font-size:24px;font-weight:700;letter-spacing:3px;line-height:1;margin-top:2px;color:#fff}
.tb-bot{font-family:var(--font-mono);font-size:12px;font-weight:600;color:var(--neon-purple);background:rgba(176,0,255,.1);padding:5px 12px;border-radius:4px;border:1px solid rgba(176,0,255,.3);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:180px}
.tb-controls{display:flex;align-items:center;gap:12px;flex-shrink:0}
.status-tag{display:flex;align-items:center;gap:8px;padding:8px 16px;border-radius:4px;font-family:var(--font-mono);font-size:12px;font-weight:700;letter-spacing:2px;transition:all .3s;background:rgba(0,0,0,.5);border:1px solid var(--line)}
.status-tag.online{color:var(--green);border-color:var(--green);box-shadow:0 0 10px rgba(0,255,102,.2)}
.status-tag.offline{color:var(--red);border-color:rgba(255,0,85,.3)}
.status-led{width:8px;height:8px;border-radius:50%}
.status-tag.online .status-led{background:var(--green);box-shadow:0 0 12px var(--green);animation:ledBlink 1.5s ease-in-out infinite}
.status-tag.offline .status-led{background:var(--red)}
@keyframes ledBlink{0%,100%{opacity:1}50%{opacity:.4}}
.btn{display:inline-flex;align-items:center;justify-content:center;gap:8px;padding:12px 24px;border-radius:6px;font-size:13px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;border:none;transition:all .2s;font-family:var(--font-disp);position:relative;overflow:hidden;box-shadow:0 4px 15px rgba(0,0,0,.4)}
.btn:active{transform:scale(.95)!important}
.btn-cyan{background:linear-gradient(to bottom,var(--neon-cyan),#00a0b0);color:#000;border:1px solid #70ffff}
.btn-cyan:hover{transform:translateY(-2px);filter:brightness(1.1)}
.btn-green{background:rgba(0,255,102,.1);color:var(--green);border:1px solid rgba(0,255,102,.4)}
.btn-green:hover{transform:translateY(-2px);background:rgba(0,255,102,.2)}
.btn-red{background:rgba(255,0,85,.1);color:var(--red);border:1px solid rgba(255,0,85,.4)}
.btn-red:hover{transform:translateY(-2px);background:rgba(255,0,85,.2)}
.btn-amber{background:rgba(255,184,0,.1);color:var(--amber);border:1px solid rgba(255,184,0,.4)}
.btn-amber:hover{transform:translateY(-2px);background:rgba(255,184,0,.2)}
.btn-purple{background:rgba(176,0,255,.1);color:var(--neon-purple);border:1px solid rgba(176,0,255,.4)}
.btn-purple:hover{transform:translateY(-2px);background:rgba(176,0,255,.2)}
.btn-ghost{background:rgba(255,255,255,.05);color:var(--text-main);border:1px solid var(--line-bright)}
.btn-ghost:hover{background:rgba(255,255,255,.1);border-color:#fff;transform:translateY(-2px)}
.btn-sm{padding:10px 16px;font-size:12px}
.btn-row{display:flex;gap:12px;flex-wrap:wrap;align-items:center}
.page{flex:1;min-height:0;overflow-y:auto;padding:32px;display:none}
.page.active{display:block;animation:pageEnter .4s var(--anim-smooth) forwards}
@keyframes pageEnter{from{opacity:0;transform:translateY(15px)}to{opacity:1;transform:translateY(0)}}
.stats-row{display:grid;grid-template-columns:repeat(4,1fr);gap:20px;margin-bottom:30px}
.stat-block{background:var(--bg-panel);border:1px solid var(--line);border-radius:8px;padding:24px;position:relative;transition:all .3s;box-shadow:0 15px 35px rgba(0,0,0,.6);overflow:hidden}
.stat-block:hover{transform:translateY(-4px);border-color:var(--line-bright)}
.stat-block::before{content:'';position:absolute;top:0;left:0;right:0;height:3px;opacity:.8}
.stat-block.s-cyan::before{background:var(--neon-cyan)}
.stat-block.s-purple::before{background:var(--neon-purple)}
.stat-label{font-family:var(--font-mono);font-size:11px;font-weight:700;letter-spacing:4px;color:var(--text-muted);margin-bottom:14px;text-transform:uppercase}
.stat-value{font-family:var(--font-disp);font-size:46px;font-weight:700;line-height:1;margin-bottom:8px}
.sv-cyan{color:var(--neon-cyan)}.sv-green{color:var(--green)}.sv-amber{color:var(--amber)}.sv-red{color:var(--red)}.sv-purple{color:var(--neon-purple)}
.stat-sub{font-family:var(--font-mono);font-size:12px;color:var(--text-dark)}
.panel{background:var(--bg-panel);backdrop-filter:blur(15px);border:1px solid var(--line);border-radius:8px;margin-bottom:24px;box-shadow:0 15px 40px rgba(0,0,0,.7);overflow:hidden}
.panel-head{display:flex;align-items:center;justify-content:space-between;padding:20px 28px;border-bottom:1px solid var(--line);background:rgba(0,0,0,.3);flex-wrap:wrap;gap:16px}
.panel-title{display:flex;align-items:center;gap:12px;font-family:var(--font-disp);font-size:22px;font-weight:700;letter-spacing:3px;color:#fff}
.panel-tag{font-family:var(--font-mono);font-size:11px;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:var(--neon-purple);padding:6px 12px;border:1px solid rgba(176,0,255,.4);border-radius:4px;background:rgba(176,0,255,.1)}
.panel-body{padding:28px}
.term-chrome{background:#030305;border-bottom:1px solid var(--line);padding:14px 20px;display:flex;align-items:center;gap:12px}
.term-dot{width:12px;height:12px;border-radius:50%}
.term-title{flex:1;text-align:center;font-family:var(--font-mono);font-size:12px;font-weight:700;letter-spacing:4px;color:var(--text-dark);text-transform:uppercase}
.terminal{background:#020203;position:relative;padding:20px;overflow-y:auto;font-family:var(--font-mono);font-size:14px;line-height:1.6}
.log-row{display:flex;align-items:baseline;gap:16px;padding:4px 0}
.log-row:hover{background:rgba(0,240,255,.05)}
.log-ts{font-size:12px;color:var(--text-dark);flex-shrink:0}
.log-tag{font-size:10px;padding:4px 8px;border-radius:2px;flex-shrink:0;text-transform:uppercase;font-weight:700;letter-spacing:2px}
.log-tag.sys{background:rgba(0,119,255,.2);color:var(--neon-blue);border:1px solid rgba(0,119,255,.4)}
.log-tag.err{background:rgba(255,0,85,.2);color:var(--red);border:1px solid rgba(255,0,85,.4)}
.log-tag.ok{background:rgba(0,255,102,.2);color:var(--green);border:1px solid rgba(0,255,102,.4)}
.log-tag.warn{background:rgba(255,184,0,.2);color:var(--amber);border:1px solid rgba(255,184,0,.4)}
.log-tag.out{background:rgba(255,255,255,.1);color:var(--text-main)}
.log-msg{flex:1;word-break:break-all}
.log-msg.sys{color:var(--neon-blue)}.log-msg.err{color:var(--red)}.log-msg.ok{color:var(--green)}.log-msg.warn{color:var(--amber)}.log-msg.out{color:var(--text-muted)}
.term-input-wrap{display:flex;align-items:center;gap:16px;background:#050508;border-top:1px solid var(--line);padding:16px 24px;transition:all .3s}
.term-input-wrap:focus-within{background:#0A0F1A;border-color:var(--neon-cyan)}
.term-input{flex:1;background:none;border:none;outline:none;font-family:var(--font-mono);font-size:15px;color:var(--neon-cyan);caret-color:var(--neon-cyan)}
.form-group{margin-bottom:24px}
.form-label{display:flex;align-items:center;gap:12px;font-family:var(--font-mono);font-size:11px;font-weight:700;letter-spacing:3px;color:var(--text-muted);text-transform:uppercase;margin-bottom:12px}
.form-label::after{content:'';flex:1;height:1px;background:var(--line)}
.form-input,.form-select,.form-textarea{width:100%;background:rgba(0,0,0,.5);border:1px solid var(--line);border-left:3px solid var(--line-bright);padding:16px 20px;font-size:16px;color:#fff;outline:none;font-family:var(--font-mono);transition:all .3s;border-radius:6px}
.form-input:focus,.form-select:focus,.form-textarea:focus{border-color:var(--neon-cyan);border-left-color:var(--neon-cyan);background:rgba(0,240,255,.05)}
.form-select option{background:var(--bg-void)}
.form-textarea{resize:vertical;min-height:120px;line-height:1.7}
.form-row-2{display:grid;grid-template-columns:1fr 1fr;gap:24px}
.divider{font-family:var(--font-disp);font-size:18px;letter-spacing:2px;color:var(--neon-purple);border-bottom:1px solid var(--line);padding-bottom:8px;margin:30px 0 20px}
.env-row{display:grid;grid-template-columns:1fr 1.5fr auto;gap:8px;margin-bottom:10px;align-items:center}
.env-field{background:rgba(0,0,0,.5);border:1px solid var(--line);border-left:3px solid var(--line-bright);padding:12px 16px;font-family:var(--font-mono);font-size:13px;color:#fff;outline:none;width:100%;border-radius:4px}
.env-field:focus{border-left-color:var(--neon-cyan)}
.env-field.key-field{color:var(--amber)}
.file-table{width:100%;border-collapse:separate;border-spacing:0;min-width:550px}
.file-table th{font-family:var(--font-mono);font-size:11px;text-transform:uppercase;letter-spacing:4px;color:var(--text-dark);padding:16px 20px;border-bottom:1px solid var(--line-bright);text-align:left;font-weight:700}
.file-table td{padding:16px 20px;font-size:15px;border-bottom:1px solid var(--line);vertical-align:middle;font-family:var(--font-mono)}
.file-table tr:hover td{background:rgba(0,240,255,.05)}
.file-name-cell{color:var(--neon-cyan);cursor:pointer;display:flex;align-items:center;gap:12px;font-weight:500;transition:all .2s}
.file-name-cell:hover{color:#fff;transform:translateX(6px)}
.file-ext-badge{font-size:10px;letter-spacing:2px;text-transform:uppercase;padding:4px 8px;border:1px solid var(--line-bright);border-radius:4px;color:var(--text-muted);background:rgba(0,0,0,.5)}
.drop-zone{border:2px dashed rgba(0,240,255,.4);padding:40px 24px;text-align:center;transition:all .3s;position:relative;background:rgba(0,0,0,.4);border-radius:6px}
.drop-zone.dragging{border-color:var(--neon-cyan);background:rgba(0,240,255,.05);box-shadow:inset 0 0 40px rgba(0,240,255,.08)}
.drop-icon{font-size:48px;margin-bottom:16px;display:block;color:var(--text-dark);transition:all .3s;pointer-events:none}
.drop-zone.dragging .drop-icon{color:var(--neon-cyan);transform:translateY(-5px)}
.drop-headline{font-family:var(--font-disp);font-size:28px;letter-spacing:4px;color:#fff;margin-bottom:8px;pointer-events:none}
.drop-sub{font-family:var(--font-mono);font-size:11px;letter-spacing:2px;color:var(--text-dark)}
.upload-item{display:flex;align-items:center;gap:12px;padding:12px 16px;background:rgba(0,0,0,.6);border:1px solid var(--line);border-radius:6px;margin-top:8px;font-family:var(--font-mono);font-size:12px;color:var(--text-muted);transition:border-color .3s}
.upload-bar-wrap{flex:1;height:4px;background:var(--line);border-radius:2px;overflow:hidden}
.upload-bar-fill{height:100%;background:var(--neon-cyan);border-radius:2px;transition:width .15s linear}
.modal-veil{display:none;position:fixed;inset:0;background:rgba(0,0,0,.5);z-index:10000;align-items:center;justify-content:center;backdrop-filter:blur(6px)}
.modal-veil.open{display:flex;animation:fadeIn .3s ease}
.modal-box{background:var(--bg-panel);border:1px solid var(--line-bright);border-top:4px solid var(--neon-cyan);padding:40px;width:95%;max-width:600px;max-height:90vh;overflow-y:auto;border-radius:8px;box-shadow:0 20px 60px rgba(0,0,0,.9);animation:modalPop .4s var(--anim-spring)}
.modal-box.wide{max-width:1000px}
@keyframes modalPop{from{transform:scale(.95) translateY(30px);opacity:0}to{transform:scale(1) translateY(0);opacity:1}}
.modal-heading{font-family:var(--font-disp);font-size:36px;color:#fff;margin-bottom:32px;letter-spacing:4px;display:flex;align-items:center;gap:14px}
.modal-heading-accent{color:var(--neon-cyan)}
.modal-footer{display:flex;justify-content:flex-end;gap:16px;margin-top:32px;padding-top:24px;border-top:1px solid var(--line)}
#loginOverlay{position:fixed;inset:0;background:var(--bg-void);z-index:99999;display:flex;align-items:center;justify-content:center;flex-direction:column}
#loginOverlay::before{content:'';position:absolute;inset:0;pointer-events:none;background:radial-gradient(circle at center,rgba(0,240,255,.15) 0%,transparent 60%)}
.login-card{background:rgba(10,15,26,.8);backdrop-filter:blur(30px);border:1px solid var(--line-bright);border-top:4px solid var(--neon-purple);padding:60px 50px;display:flex;flex-direction:column;width:90%;max-width:450px;border-radius:12px;animation:floatUp .8s var(--anim-spring);position:relative;z-index:1}
.login-tabs{display:flex;margin-bottom:24px;border-bottom:1px solid var(--line)}
.login-tab{flex:1;text-align:center;padding:12px;font-family:var(--font-mono);font-size:14px;font-weight:700;letter-spacing:2px;color:var(--text-dark);cursor:pointer;text-transform:uppercase;transition:all .3s}
.login-tab.active{color:var(--neon-purple);border-bottom:2px solid var(--neon-purple);background:rgba(176,0,255,.05)}
@keyframes fadeIn{from{opacity:0}to{opacity:1}}
@keyframes floatUp{from{transform:translateY(50px) scale(.95);opacity:0}to{transform:translateY(0) scale(1);opacity:1}}
.code-editor{width:100%;min-height:550px;background:#020203;border:1px solid var(--line);border-radius:6px;border-left:4px solid var(--neon-blue);padding:20px;font-family:var(--font-mono);font-size:15px;color:#E0F2FE;outline:none;resize:vertical;line-height:1.7;caret-color:var(--neon-cyan);transition:all .3s}
.code-editor:focus{border-left-color:var(--neon-cyan)}
.danger-block{border:1px solid rgba(255,0,85,.4);border-left:4px solid var(--red);background:linear-gradient(90deg,rgba(255,0,85,.1),transparent);padding:24px;margin-top:24px;border-radius:8px}
.res-item{margin-bottom:30px}
.res-header{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:12px}
.res-track{height:8px;background:rgba(0,0,0,.8);border:1px solid var(--line);border-radius:4px;overflow:hidden}
.res-fill{height:100%;transition:width .8s ease}
.toast-tray{position:fixed;bottom:32px;right:32px;z-index:10001;display:flex;flex-direction:column;gap:14px;pointer-events:none}
.toast{background:var(--bg-panel);border:1px solid var(--line-bright);border-left:4px solid var(--neon-cyan);border-radius:6px;padding:16px 24px;font-size:14px;font-weight:600;color:#fff;font-family:var(--font-mono);animation:toastPop .4s var(--anim-spring);display:flex;align-items:center;gap:14px;box-shadow:0 10px 30px rgba(0,0,0,.8);pointer-events:all;text-transform:uppercase;letter-spacing:1px}
.toast.success{border-left-color:var(--green)}.toast.error{border-left-color:var(--red)}.toast.info{border-left-color:var(--neon-blue)}
@keyframes toastPop{from{transform:translateX(40px);opacity:0}to{transform:translateX(0);opacity:1}}
.subuser-item{display:flex;justify-content:space-between;align-items:center;padding:12px 16px;background:rgba(0,0,0,.4);border:1px solid var(--line);border-radius:6px;margin-bottom:8px;font-family:var(--font-mono);font-size:14px}
@media(max-width:850px){
.sidebar{position:fixed;left:-320px;width:300px;transition:transform .4s var(--anim-spring);border-right:1px solid var(--line-bright)}
.sidebar.open{transform:translateX(320px)}
.sidebar .nav{display:none}
.sidebar-footer{position:absolute;bottom:0;width:100%}
.sidebar-close-btn{display:block}
.mobile-bottom-nav{display:flex}
.mobile-nav-toggle{display:block}
.main{padding-bottom:90px}
.topbar{height:auto;min-height:70px;padding:16px 20px;flex-direction:column;align-items:stretch;gap:12px}
.topbar-main{display:flex;align-items:center;gap:16px;width:100%;justify-content:space-between}
.tb-breadcrumb{flex:1;flex-wrap:wrap;margin-left:10px}
.tb-page{font-size:20px}
.tb-bot{max-width:120px;font-size:10px}
.tb-controls{display:flex;flex-wrap:nowrap;overflow-x:auto;padding-bottom:8px;width:100%;justify-content:flex-start;border-top:1px solid var(--line);padding-top:12px;gap:8px;-webkit-overflow-scrolling:touch}
.tb-controls::-webkit-scrollbar{display:none}
.tb-controls .btn{white-space:nowrap;flex-shrink:0;padding:12px 16px;font-size:12px}
.page{padding:10px}
.stats-row{grid-template-columns:1fr 1fr;gap:10px;margin-bottom:16px}
.stat-value{font-size:32px}
.form-row-2{grid-template-columns:1fr;gap:16px}
.panel-head{padding:14px 16px;flex-direction:column;align-items:flex-start;gap:10px}
.panel-title{font-size:17px}
.panel-body{padding:14px}
.toast-tray{bottom:100px;right:12px;left:12px}
.toast{justify-content:center;font-size:12px;padding:12px 16px}
.file-table-wrap{overflow-x:auto;-webkit-overflow-scrolling:touch;width:100%}
.file-table{min-width:0;width:100%;table-layout:fixed}
.file-table th:nth-child(4),.file-table td:nth-child(4){display:none}
.file-table th:nth-child(2),.file-table td:nth-child(2){display:none}
.file-table th:nth-child(1){width:55%}
.file-table th:nth-child(3){width:18%}
.file-table th:nth-child(5){width:27%}
.file-table th,.file-table td{padding:10px 8px;font-size:11px}
.file-name-cell{font-size:11px;gap:5px}
.file-table .btn-row{gap:4px;flex-wrap:nowrap}
.file-table .btn.btn-sm{padding:7px 8px;font-size:10px;letter-spacing:0;min-width:0}
.drop-zone{padding:28px 14px}
.drop-icon{font-size:32px;margin-bottom:10px}
.drop-headline{font-size:19px;letter-spacing:2px}
.drop-sub{font-size:10px;letter-spacing:0;margin-bottom:20px!important}
.drop-btns{flex-direction:column!important;gap:10px!important}
.drop-btns .btn{width:100%}
.upload-item{flex-wrap:wrap;padding:10px 12px}
.upload-item .upload-bar-wrap{order:3;flex-basis:100%}
}
@media(max-width:480px){
.stats-row{grid-template-columns:1fr;gap:8px}
.stat-value{font-size:28px}
.login-card{padding:32px 16px;width:96%}
.modal-box{padding:18px 14px}
.modal-heading{font-size:20px;margin-bottom:14px}
.drop-zone{padding:20px 10px}
.drop-headline{font-size:16px;letter-spacing:1px}
.page{padding:8px}
.panel-head{padding:10px 12px}
}
</style>
</head>
<body>
<div id="loginOverlay">
  <div class="login-card">
    <div style="font-family:var(--font-disp);font-size:48px;font-weight:900;text-align:center;margin-bottom:5px;background:linear-gradient(90deg,#fff,var(--neon-cyan));-webkit-background-clip:text;-webkit-text-fill-color:transparent">VORTEX</div>
    <p style="font-family:var(--font-mono);color:var(--text-dark);font-size:12px;letter-spacing:6px;text-transform:uppercase;text-align:center;margin-bottom:20px">HOSTING PLATFORM</p>
    <div class="login-tabs">
      <div class="login-tab active" id="tabLogin" onclick="switchAuthMode('login')">LOGIN</div>
      <div class="login-tab" id="tabRegister" onclick="switchAuthMode('register')">REGISTER</div>
    </div>
    <input class="form-input" id="authUsername" placeholder="USERNAME" style="text-align:center;letter-spacing:2px;padding:16px;margin-bottom:12px" onkeydown="if(event.key==='Enter')submitAuth()">
    <input type="password" class="form-input" id="authPassword" placeholder="PASSWORD" style="text-align:center;letter-spacing:2px;padding:16px;margin-bottom:12px" onkeydown="if(event.key==='Enter')submitAuth()">
    <button class="btn btn-cyan" id="authBtn" style="width:100%;padding:18px;font-size:16px;margin-top:10px;letter-spacing:4px" onclick="submitAuth()">AUTHENTICATE</button>
  </div>
</div>
<div class="sidebar-overlay" onclick="toggleSidebar()"></div>
<div id="app">
  <aside class="sidebar">
    <button class="sidebar-close-btn" onclick="toggleSidebar()">✕</button>
    <div class="logo">
      <div class="logo-lockup"><div class="logo-wordmark">VORTEX</div></div>
      <div style="font-size:10px;color:var(--text-dark);letter-spacing:6px;font-family:var(--font-mono);text-transform:uppercase;font-weight:700">Admin Interface</div>
    </div>
    <nav class="nav">
      <div class="nav-label">System</div>
      <div class="nav-item active" data-page="dashboard" onclick="navTo('dashboard',this)"><span class="nav-glyph">◈</span> Dashboard</div>
      <div class="nav-item" data-page="console" onclick="navTo('console',this)"><span class="nav-glyph">_</span> Console</div>
      <div class="nav-item" data-page="files" onclick="navTo('files',this)"><span class="nav-glyph">≡</span> File Manager</div>
      <div class="nav-label" style="margin-top:24px">Configuration</div>
      <div class="nav-item" data-page="env" onclick="navTo('env',this)"><span class="nav-glyph">⊛</span> Environment</div>
      <div class="nav-item" data-page="settings" onclick="navTo('settings',this)"><span class="nav-glyph">⚙</span> Settings</div>
      <div class="nav-item" data-page="resources" onclick="navTo('resources',this)"><span class="nav-glyph">▣</span> Resources</div>
    </nav>
    <div class="bot-section-header"><span class="bot-section-label">Instances</span><span class="bot-count-badge" id="botCount">0</span></div>
    <div class="new-bot-btn" onclick="openCreateModal()"><span style="font-size:18px">+</span> DEPLOY NEW</div>
    <div class="bot-list" id="botList"></div>
    <div class="sidebar-footer">
      <span class="footer-brand" onclick="logout()">LOGOUT</span>
      <span class="footer-clock" id="clock">00:00:00</span>
    </div>
  </aside>
  <main class="main">
    <div class="topbar">
      <div class="topbar-main">
        <button class="mobile-nav-toggle" onclick="toggleSidebar()">☰</button>
        <div class="tb-breadcrumb">
          <span class="tb-section">VORTEX</span><span class="tb-slash">/</span>
          <span class="tb-page" id="tbPage">DASHBOARD</span><span class="tb-slash">·</span>
          <span class="tb-bot" id="tbBot">— SELECT INSTANCE —</span>
        </div>
      </div>
      <div class="tb-controls">
        <div class="status-tag offline" id="statusTag"><div class="status-led"></div><span id="statusText">OFFLINE</span></div>
        <button class="btn btn-green btn-sm" onclick="startBot()">▶ START</button>
        <button class="btn btn-red btn-sm" onclick="stopBot()">■ STOP</button>
        <button class="btn btn-amber btn-sm" onclick="restartBot()">↺ RESTART</button>
      </div>
    </div>
    <div class="page active" id="page-dashboard">
      <div class="stats-row">
        <div class="stat-block s-cyan"><div class="stat-label">Status</div><div class="stat-value sv-red" id="sStat">OFFLINE</div><div class="stat-sub" id="sStatSub">No active process</div></div>
        <div class="stat-block s-purple"><div class="stat-label">Uptime</div><div class="stat-value sv-purple" id="sUptime">—</div><div class="stat-sub">HH:MM:SS</div></div>
        <div class="stat-block s-cyan"><div class="stat-label">Sys CPU</div><div class="stat-value sv-cyan" id="sCpu">—</div><div class="stat-sub">Load Average</div></div>
        <div class="stat-block s-purple"><div class="stat-label">Sys Memory</div><div class="stat-value sv-purple" id="sMem">—</div><div class="stat-sub">RAM Usage</div></div>
      </div>
      <div class="panel">
        <div class="panel-head"><div class="panel-title">LAUNCH CONTROL</div><span class="panel-tag">Operations</span></div>
        <div class="panel-body">
          <div class="form-row-2" style="margin-bottom:24px"><div class="form-group" style="margin:0"><label class="form-label">Startup File</label><input class="form-input" id="sfInput" value="main.py" placeholder="main.py"></div></div>
          <div class="btn-row">
            <button class="btn btn-cyan" onclick="startBot()">▶ Start Process</button>
            <button class="btn btn-red" onclick="stopBot()">■ Stop</button>
            <button class="btn btn-amber" onclick="restartBot()">↺ Restart</button>
            <button class="btn btn-ghost" onclick="killBot()" style="margin-left:auto">☠ Force Kill</button>
          </div>
        </div>
      </div>
      <div class="panel">
        <div class="panel-head"><div class="panel-title">LIVE FEED</div><button class="btn btn-ghost btn-sm" onclick="navTo('console',null)">Full Console →</button></div>
        <div class="panel-body" style="padding:0">
          <div class="term-chrome" style="border:none;border-bottom:1px solid rgba(255,255,255,.05)">
            <div class="term-dot" style="background:#FF0055"></div><div class="term-dot" style="background:#FFB800"></div><div class="term-dot" style="background:#00FF66"></div>
            <div class="term-title">STDOUT</div>
          </div>
          <div class="terminal" id="miniTerm" style="height:250px;border:none"></div>
        </div>
      </div>
    </div>
    <div class="page" id="page-console">
      <div class="panel">
        <div class="panel-head"><div class="panel-title">PROCESS CONSOLE</div>
          <div class="btn-row">
            <button class="btn btn-ghost btn-sm" onclick="clearConsole()">⊘ Clear</button>
            <button class="btn btn-ghost btn-sm" onclick="exportLogs()">↓ Export</button>
          </div>
        </div>
        <div class="term-chrome">
          <div class="term-dot" style="background:#FF0055"></div><div class="term-dot" style="background:#FFB800"></div><div class="term-dot" style="background:#00FF66"></div>
          <div class="term-title" id="termTitle">NO INSTANCE SELECTED</div>
        </div>
        <div class="terminal" id="mainTerm" style="height:500px;border-top:none"></div>
        <div class="term-input-wrap">
          <span style="font-family:var(--font-mono);font-size:18px;color:var(--neon-cyan);flex-shrink:0">❯</span>
          <input class="term-input" id="termIn" placeholder="Send to stdin..." onkeydown="if(event.key==='Enter')sendInput()">
          <button class="btn btn-cyan" onclick="sendInput()">Send</button>
        </div>
      </div>
    </div>
    <div class="page" id="page-files">
      <input type="file" multiple id="fileUploadInput" style="display:none" onchange="handleUpload(this.files,false)">
      <input type="file" webkitdirectory directory multiple id="folderUploadInput" style="display:none" onchange="handleUpload(this.files,true)">
      <div class="panel">
        <div class="panel-head">
          <div class="panel-title">FILE MANAGER</div>
          <div class="btn-row">
            <button class="btn btn-ghost btn-sm" onclick="openNewFileModal()">+ New File</button>
            <button class="btn btn-cyan btn-sm" onclick="loadFiles()">↻ Refresh</button>
          </div>
        </div>
        <div class="file-table-wrap" style="padding:8px 0">
          <table class="file-table">
            <thead><tr><th>Filename</th><th>Type</th><th>Size</th><th>Modified</th><th>Actions</th></tr></thead>
            <tbody id="fileList"></tbody>
          </table>
        </div>
        <div style="border-top:1px solid var(--line);padding:20px 16px">
          <div class="drop-zone" id="dropZone">
            <span class="drop-icon">⇪</span>
            <div class="drop-headline">DROP FILES HERE</div>
            <div class="drop-sub" style="margin-bottom:24px">OR USE THE BUTTONS BELOW · ZIP AUTO-EXTRACTED · FOLDER STRUCTURE PRESERVED</div>
            <div class="drop-btns" style="display:flex;gap:12px;justify-content:center;flex-wrap:wrap;position:relative;z-index:5">
              <button class="btn btn-cyan" onclick="event.stopPropagation();document.getElementById('fileUploadInput').click()">⇪ Upload Files</button>
              <button class="btn btn-purple" onclick="event.stopPropagation();document.getElementById('folderUploadInput').click()">📁 Upload Folder</button>
            </div>
          </div>
          <div id="uploadProgress" style="margin-top:12px"></div>
        </div>
      </div>
    </div>
    <div class="page" id="page-env">
      <div class="panel">
        <div class="panel-head"><div class="panel-title">ENVIRONMENT VARIABLES</div><button class="btn btn-cyan btn-sm" onclick="saveEnv()">Save Variables</button></div>
        <div class="panel-body">
          <p style="font-family:var(--font-mono);font-size:12px;color:var(--text-muted);letter-spacing:1px;margin-bottom:24px">Secure keys injected into the process at startup.</p>
          <div class="env-row" style="margin-bottom:12px">
            <span style="font-family:var(--font-mono);font-size:11px;letter-spacing:4px;color:var(--text-dark);text-transform:uppercase">KEY</span>
            <span style="font-family:var(--font-mono);font-size:11px;letter-spacing:4px;color:var(--text-dark);text-transform:uppercase">VALUE</span><span></span>
          </div>
          <div id="envRows"></div>
          <button class="btn btn-ghost" onclick="addEnvRow('','')" style="margin-top:16px">+ Add Row</button>
        </div>
      </div>
    </div>
    <div class="page" id="page-settings">
      <div class="panel">
        <div class="panel-head"><div class="panel-title">INSTANCE CONFIGURATION</div></div>
        <div class="panel-body">
          <div class="form-group"><label class="form-label">Instance Name</label><input class="form-input" id="stName" placeholder="My Server"></div>
          <div class="form-group"><label class="form-label">Startup File</label><input class="form-input" id="stStartup" placeholder="main.py"></div>
          <div class="form-group"><label class="form-label">Crash Recovery</label><select class="form-select" id="stAR"><option value="false">Disabled</option><option value="true">Auto Restart on crash</option></select></div>
          <button class="btn btn-cyan" onclick="saveSettings()" style="margin-top:16px">Save Configuration</button>
          <div class="divider" id="accessMgmtTitle">ACCESS MANAGEMENT</div>
          <div id="accessMgmtSection">
            <div class="form-group"><label class="form-label">Grant Access to User</label>
              <div style="display:flex;gap:12px"><input class="form-input" id="newSubuser" placeholder="Enter username..."><button class="btn btn-purple" onclick="addSubuser()">Grant</button></div>
            </div>
            <div id="subuserList"></div>
          </div>
        </div>
      </div>
      <div class="danger-block" id="dangerZoneSection">
        <div style="font-family:var(--font-mono);font-size:12px;font-weight:700;letter-spacing:2px;color:var(--red);margin-bottom:10px">⚠ CRITICAL ACTION</div>
        <div style="font-size:14px;color:var(--text-muted);margin-bottom:16px">Permanently deletes this instance and all associated files.</div>
        <button class="btn btn-red" onclick="deleteBot()">☠ DESTROY INSTANCE</button>
      </div>
    </div>
    <div class="page" id="page-resources">
      <div class="panel">
        <div class="panel-head"><div class="panel-title">SYSTEM HARDWARE</div><span class="panel-tag" style="color:var(--neon-cyan);border-color:var(--neon-cyan);background:rgba(0,240,255,.1)">LIVE TELEMETRY</span></div>
        <div class="panel-body">
          <div class="res-item"><div class="res-header"><span style="font-family:var(--font-mono);font-size:12px;letter-spacing:2px;color:var(--text-muted)">CPU USAGE</span><span class="sv-cyan" style="font-family:var(--font-disp);font-size:24px" id="rCpu">—</span></div><div class="res-track"><div class="res-fill" id="pCpu" style="width:0%;background:var(--neon-cyan)"></div></div></div>
          <div class="res-item"><div class="res-header"><span style="font-family:var(--font-mono);font-size:12px;letter-spacing:2px;color:var(--text-muted)">MEMORY</span><span class="sv-purple" style="font-family:var(--font-disp);font-size:24px" id="rMem">—</span></div><div class="res-track"><div class="res-fill" id="pMem" style="width:0%;background:var(--neon-purple)"></div></div></div>
          <div class="res-item"><div class="res-header"><span style="font-family:var(--font-mono);font-size:12px;letter-spacing:2px;color:var(--text-muted)">DISK</span><span class="sv-cyan" style="font-family:var(--font-disp);font-size:24px" id="rDsk">—</span></div><div class="res-track"><div class="res-fill" id="pDsk" style="width:0%;background:var(--neon-cyan)"></div></div></div>
        </div>
      </div>
    </div>
  </main>
  <nav class="mobile-bottom-nav">
    <div class="m-nav-item" onclick="toggleSidebar()"><span class="m-nav-glyph">▤</span><span class="m-nav-text">Bots</span></div>
    <div class="m-nav-item active" data-page="dashboard" onclick="navTo('dashboard',this)"><span class="m-nav-glyph">◈</span><span class="m-nav-text">Dash</span></div>
    <div class="m-nav-item" data-page="console" onclick="navTo('console',this)"><span class="m-nav-glyph">_</span><span class="m-nav-text">Term</span></div>
    <div class="m-nav-item" data-page="files" onclick="navTo('files',this)"><span class="m-nav-glyph">≡</span><span class="m-nav-text">Files</span></div>
    <div class="m-nav-item" data-page="settings" onclick="navTo('settings',this)"><span class="m-nav-glyph">⚙</span><span class="m-nav-text">Config</span></div>
  </nav>
</div>
<div class="toast-tray" id="toastTray"></div>
<div class="modal-veil" id="mCreate">
  <div class="modal-box">
    <div class="modal-heading">DEPLOY <span class="modal-heading-accent">INSTANCE</span></div>
    <div class="form-group"><label class="form-label">Instance Name</label><input class="form-input" id="mName" placeholder="Project Alpha"></div>
    <div class="form-group"><label class="form-label">Startup File</label><input class="form-input" id="mFile" value="main.py" placeholder="main.py"></div>
    <div class="modal-footer"><button class="btn btn-ghost" onclick="closeModal('mCreate')">Cancel</button><button class="btn btn-cyan" onclick="createBot()">Initialize</button></div>
  </div>
</div>
<div class="modal-veil" id="mEditor">
  <div class="modal-box wide">
    <div class="modal-heading">EDIT <span class="modal-heading-accent" id="edName">FILE</span></div>
    <textarea class="code-editor" id="edContent"></textarea>
    <div class="modal-footer"><button class="btn btn-ghost" onclick="closeModal('mEditor')">Discard</button><button class="btn btn-cyan" onclick="saveFile()">Commit Changes</button></div>
  </div>
</div>
<div class="modal-veil" id="mNewFile">
  <div class="modal-box">
    <div class="modal-heading">CREATE <span class="modal-heading-accent">FILE</span></div>
    <div class="form-group"><label class="form-label">Filename (e.g. src/app.py)</label><input class="form-input" id="nfName" placeholder="main.py"></div>
    <div class="form-group"><label class="form-label">Initial Content</label><textarea class="form-textarea" id="nfContent" placeholder="# Code..." style="height:160px"></textarea></div>
    <div class="modal-footer"><button class="btn btn-ghost" onclick="closeModal('mNewFile')">Cancel</button><button class="btn btn-cyan" onclick="createNewFile()">Create</button></div>
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
  document.querySelector('.sidebar').classList.toggle('open');
  const o=document.querySelector('.sidebar-overlay');
  if(o.classList.contains('open')){o.classList.remove('open');setTimeout(()=>o.style.display='none',300)}
  else{o.style.display='block';void o.offsetWidth;o.classList.add('open')}
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
  const res=await r.json();if(r.ok)location.reload();else toast(res.error||'Authentication Failed','error');
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
  if(!entries.length){el.innerHTML='<div style="padding:20px;text-align:center;color:var(--text-dark);font-family:var(--font-mono);font-size:11px">NO INSTANCES</div>';return}
  entries.forEach(([id,b])=>{
    const d=document.createElement('div');d.className='bot-item'+(id===curBot?' active':'');
    const s=b.status||'offline',sh=b.is_shared?`<span class="bot-shared-icon">SHARED</span>`:'';
    d.innerHTML=`<div class="bot-indicator ${s}"></div><div style="flex:1"><div class="bot-name">${escH(b.name||id)}</div><div style="display:flex"><div class="bot-status-text">${s}</div>${sh}</div></div>`;
    d.onclick=()=>{selectBot(id);if(window.innerWidth<=850&&document.querySelector('.sidebar').classList.contains('open'))toggleSidebar()};
    el.appendChild(d);
  });
}
function selectBot(id){
  curBot=id;const b=botRegistry[id];
  document.getElementById('tbBot').textContent=b?.name||id;
  document.getElementById('sfInput').value=b?.startup_file||'main.py';
  document.getElementById('termTitle').textContent=(b?.name||id)+' STDOUT';
  ['mainTerm','miniTerm'].forEach(i=>document.getElementById(i).innerHTML='');
  applyStatus(b?.status||'offline');renderBotList();loadBotLogs();startUptime();
  if(b&&b.is_shared){document.getElementById('accessMgmtTitle').style.display='none';document.getElementById('accessMgmtSection').style.display='none';document.getElementById('dangerZoneSection').style.display='none'}
  else{document.getElementById('accessMgmtTitle').style.display='block';document.getElementById('accessMgmtSection').style.display='block';document.getElementById('dangerZoneSection').style.display='block';if(document.getElementById('page-settings').classList.contains('active'))loadSettings()}
}
async function loadBotLogs(){
  if(!curBot)return;const r=await apiFetch(`/api/bot/${curBot}/logs`);if(!r)return;
  const logs=await r.json();['mainTerm','miniTerm'].forEach(id=>document.getElementById(id).innerHTML='');
  logs.forEach(({msg,level,time:ts})=>appendLog(msg,level,ts));
}
function applyStatus(s){
  const on=s==='online';
  document.getElementById('statusTag').className='status-tag '+(on?'online':'offline');
  document.getElementById('statusText').textContent=on?'ONLINE':'OFFLINE';
  document.getElementById('sStat').textContent=on?'ONLINE':'OFFLINE';
  document.getElementById('sStat').className='stat-value '+(on?'sv-green':'sv-red');
  document.getElementById('sStatSub').textContent=on?'Process Active':'Process Halted';
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
async function startBot(){if(!curBot)return;const sf=document.getElementById('sfInput').value.trim()||'main.py';await apiFetch(`/api/bot/${curBot}/start`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({startup_file:sf})});toast('Booting...','info')}
async function stopBot(){if(!curBot)return;await apiFetch(`/api/bot/${curBot}/stop`,{method:'POST'});toast('Stopped','success')}
async function restartBot(){if(!curBot)return;await apiFetch(`/api/bot/${curBot}/stop`,{method:'POST'});toast('Rebooting...','info');setTimeout(startBot,800)}
async function killBot(){if(!curBot)return;await apiFetch(`/api/bot/${curBot}/kill`,{method:'POST'});toast('Force-killed','error')}
async function deleteBot(){
  if(!curBot||!confirm('Permanently wipe this instance?'))return;
  await apiFetch(`/api/bot/${curBot}`,{method:'DELETE'});delete botRegistry[curBot];curBot=null;
  document.getElementById('tbBot').textContent='— SELECT INSTANCE —';
  ['mainTerm','miniTerm'].forEach(i=>document.getElementById(i).innerHTML='');
  applyStatus('offline');renderBotList();document.getElementById('botCount').textContent=Object.keys(botRegistry).length;toast('Instance wiped','error');
}
function appendLog(msg,level,ts){
  const tagMap={system:'sys',error:'err',success:'ok',warn:'warn',default:'out'};
  const tag=tagMap[level]||'out',t=ts||new Date().toTimeString().slice(0,8);
  const row=`<div class="log-row"><span class="log-ts">${escH(t)}</span><span class="log-tag ${tag}">${tag.toUpperCase()}</span><span class="log-msg ${tag}">${escH(msg)}</span></div>`;
  ['mainTerm','miniTerm'].forEach(id=>{const el=document.getElementById(id);if(el){el.innerHTML+=row;el.scrollTop=el.scrollHeight}});
}
function clearConsole(){['mainTerm','miniTerm'].forEach(id=>document.getElementById(id).innerHTML='');toast('Buffer cleared','info')}
function exportLogs(){
  const lines=Array.from(document.getElementById('mainTerm').querySelectorAll('.log-row')).map(r=>r.textContent.trim()).join('\n');
  const a=document.createElement('a');a.href='data:text/plain;charset=utf-8,'+encodeURIComponent(lines);a.download=`${curBot||'vortex'}-${Date.now()}.log`;a.click();toast('Logs downloaded','success');
}
async function sendInput(){
  if(!curBot)return;let v=document.getElementById('termIn').value;document.getElementById('termIn').value='';
  v=v.replace(/\x1b\[[0-9;]*[a-zA-Z]/g,'').replace(/[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f]/g,'');
  await apiFetch(`/api/bot/${curBot}/input`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({input:v+'\n'})});
}
const EXT_COLORS={py:'#00FF66',js:'#FFB800',json:'#00F0FF',md:'#B000FF',txt:'#6A85B6',sh:'#00F0FF',zip:'#FF0055',env:'#FFB800',ts:'#00F0FF'};
function fileGlyph(ext){const g={py:'🐍',js:'⚡',json:'{}',txt:'≡',md:'#',zip:'⊞',env:'⊛',sh:'$',ts:'⟨⟩'};return `<span style="font-size:14px">${g[ext]||'□'}</span>`}
async function loadFiles(){
  const tb=document.getElementById('fileList');
  if(!curBot){tb.innerHTML=`<tr><td colspan="5"><div style="padding:40px;text-align:center;color:var(--text-dark);font-family:var(--font-mono)">NO INSTANCE TARGETED</div></td></tr>`;return}
  const r=await apiFetch(`/api/bot/${curBot}/files`);if(!r)return;const files=await r.json();
  if(!files.length){tb.innerHTML=`<tr><td colspan="5"><div style="padding:40px;text-align:center;color:var(--text-dark);font-family:var(--font-mono)">DIRECTORY EMPTY</div></td></tr>`;return}
  tb.innerHTML=files.map(f=>{
    const ext=f.name.split('.').pop().toLowerCase(),c=EXT_COLORS[ext]||'#6A85B6',jn=JSON.stringify(f.name);
    return `<tr><td><div class="file-name-cell" onclick='editFile(${jn})'>${fileGlyph(ext)} <span style='overflow:hidden;text-overflow:ellipsis;white-space:nowrap;display:block;max-width:180px'>${escH(f.name)}</span></div></td><td><span class="file-ext-badge" style="color:${c};border-color:${c}40">${ext}</span></td><td style='white-space:nowrap'>${escH(f.size)}</td><td>${escH(f.modified)}</td><td><div class="btn-row" style='flex-wrap:nowrap'><button class="btn btn-ghost btn-sm" onclick='editFile(${jn})' title='Edit'>✏</button><button class="btn btn-ghost btn-sm" onclick='dlFile(${jn})' title='Download'>↓</button><button class="btn btn-red btn-sm" onclick='delFile(${jn})' title='Delete'>⊘</button></div></td></tr>`;
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
  closeModal('mEditor');loadFiles();toast(`${name} updated`,'success');
}
function openNewFileModal(){if(!curBot)return;document.getElementById('mNewFile').classList.add('open')}
async function createNewFile(){
  const name=document.getElementById('nfName').value.trim();if(!name){toast('Filename required','error');return}
  await apiFetch(`/api/bot/${curBot}/file/${encodeURIComponent(name)}`,{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({content:document.getElementById('nfContent').value})});
  closeModal('mNewFile');document.getElementById('nfName').value='';document.getElementById('nfContent').value='';loadFiles();toast('File injected','success');
}
async function delFile(name){if(!confirm(`Delete ${name}?`))return;await apiFetch(`/api/bot/${curBot}/file/${encodeURIComponent(name)}`,{method:'DELETE'});loadFiles();toast('File erased','success')}
function dlFile(name){window.location.href=`/api/bot/${curBot}/file/${encodeURIComponent(name)}/download`}

// ── UPLOAD: preserves folder structure via webkitRelativePath ──
async function handleUpload(files, isFolder){
  // Snapshot files immediately before resetting inputs
  const fileArr = files ? Array.from(files) : [];
  // Reset inputs so the same file can be re-selected next time
  try{ document.getElementById('fileUploadInput').value=''; }catch(e){}
  try{ document.getElementById('folderUploadInput').value=''; }catch(e){}

  if(!curBot){ toast('Select an instance first','error'); return; }
  if(!fileArr.length){ toast('No files selected','error'); return; }

  const prog = document.getElementById('uploadProgress');
  if(!prog){ console.error('uploadProgress element not found'); return; }
  let ok=0, fail=0;

  for(const file of fileArr){
    const relPath = (isFolder && file.webkitRelativePath) ? file.webkitRelativePath : file.name;
    const fd = new FormData();
    fd.append('file', file);
    fd.append('relative_path', relPath);

    const sid = 'up_' + Math.random().toString(36).slice(2);
    const wrap = document.createElement('div');
    wrap.className = 'upload-item';
    wrap.innerHTML = `
      <span style="flex-shrink:0;color:var(--neon-cyan)">⇪</span>
      <span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:11px" title="${escH(relPath)}">${escH(relPath)}</span>
      <div class="upload-bar-wrap"><div class="upload-bar-fill" id="${sid}" style="width:0%"></div></div>
      <span id="${sid}st" style="font-size:11px;color:var(--text-dark);flex-shrink:0;min-width:30px;text-align:right">0%</span>`;
    prog.appendChild(wrap);

    try{
      await new Promise((resolve, reject)=>{
        const xhr = new XMLHttpRequest();
        xhr.open('POST', `/api/bot/${curBot}/upload`);
        xhr.upload.addEventListener('progress', e=>{
          if(e.lengthComputable){
            const pct = Math.round(e.loaded/e.total*95);
            const b=document.getElementById(sid), s=document.getElementById(sid+'st');
            if(b) b.style.width=pct+'%';
            if(s) s.textContent=pct+'%';
          }
        });
        xhr.addEventListener('load', ()=>{
          if(xhr.status===401){ document.getElementById('loginOverlay').style.display='flex'; reject(new Error('Unauthorized')); return; }
          let resp={};
          try{ resp=JSON.parse(xhr.responseText); }catch(e){}
          if(resp.error){ reject(new Error(resp.error)); return; }
          if(xhr.status>=200 && xhr.status<300) resolve();
          else reject(new Error(`HTTP ${xhr.status}: ${xhr.responseText}`));
        });
        xhr.addEventListener('error', ()=>reject(new Error('Network error')));
        xhr.addEventListener('abort', ()=>reject(new Error('Aborted')));
        xhr.send(fd);
      });
      const b=document.getElementById(sid), s=document.getElementById(sid+'st');
      if(b){ b.style.width='100%'; b.style.background='var(--green)'; }
      if(s){ s.textContent='✓'; s.style.color='var(--green)'; }
      ok++;
      setTimeout(()=>wrap.remove(), 2500);
    }catch(err){
      const b=document.getElementById(sid), s=document.getElementById(sid+'st');
      if(b){ b.style.width='100%'; b.style.background='var(--red)'; }
      if(s){ s.textContent='✕'; s.style.color='var(--red)'; }
      wrap.style.borderColor='rgba(255,0,85,.4)';
      fail++;
      console.error('Upload failed:', relPath, err.message);
      toast(`Upload failed: ${err.message}`, 'error');
      setTimeout(()=>wrap.remove(), 5000);
    }
  }
  loadFiles();
  if(ok>0 && fail===0)      toast(`${ok} file${ok>1?'s':''} uploaded`, 'success');
  else if(ok>0 && fail>0)   toast(`${ok} uploaded, ${fail} failed`, 'info');
}

// Drag-and-drop — attach to document so it works even when page is hidden,
// but only trigger upload when the files panel is visible.
let _dzDepth=0;
document.addEventListener('dragenter', e=>{ e.preventDefault(); _dzDepth++; const dz=document.getElementById('dropZone'); if(dz) dz.classList.add('dragging'); });
document.addEventListener('dragleave', e=>{ if(--_dzDepth<=0){ _dzDepth=0; const dz=document.getElementById('dropZone'); if(dz) dz.classList.remove('dragging'); } });
document.addEventListener('dragover', e=>e.preventDefault());
document.addEventListener('drop', e=>{
  e.preventDefault(); _dzDepth=0;
  const dz=document.getElementById('dropZone'); if(dz) dz.classList.remove('dragging');
  if(e.dataTransfer.files.length) handleUpload(e.dataTransfer.files, false);
});

async function loadEnv(){
  if(!curBot)return;const r=await apiFetch(`/api/bot/${curBot}/env`);if(!r)return;
  const env=await r.json();const c=document.getElementById('envRows');c.innerHTML='';
  const entries=Object.entries(env);if(entries.length)entries.forEach(([k,v])=>addEnvRow(k,v));else addEnvRow('','');
}
function addEnvRow(k='',v=''){
  const d=document.createElement('div');d.className='env-row';
  d.innerHTML=`<input class="env-field key-field" placeholder="KEY" value="${escH(k)}"><input class="env-field" placeholder="value" value="${escH(v)}"><button class="btn btn-red btn-sm" onclick="this.parentElement.remove()">✕</button>`;
  document.getElementById('envRows').appendChild(d);
}
async function saveEnv(){
  if(!curBot)return;const env={};
  document.querySelectorAll('.env-row').forEach(r=>{const k=r.querySelector('.key-field')?.value.trim(),v=r.querySelectorAll('.env-field')[1]?.value;if(k)env[k]=v||''});
  await apiFetch(`/api/bot/${curBot}/env`,{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(env)});toast('Environment synced','success');
}
async function loadSettings(){
  if(!curBot)return;const b=botRegistry[curBot]||{};
  document.getElementById('stName').value=b.name||'';document.getElementById('stStartup').value=b.startup_file||'main.py';document.getElementById('stAR').value=b.auto_restart?'true':'false';
  if(!b.is_shared){const r=await apiFetch(`/api/bot/${curBot}/subusers`);if(r){const users=await r.json();const c=document.getElementById('subuserList');c.innerHTML='';users.forEach(u=>{const div=document.createElement('div');div.className='subuser-item';div.innerHTML=`<span>${escH(u)}</span><button class="btn btn-red btn-sm" onclick="removeSubuser('${escH(u)}')">REVOKE</button>`;c.appendChild(div)})}}
}
async function saveSettings(){
  if(!curBot)return;const data={name:document.getElementById('stName').value.trim(),startup_file:document.getElementById('stStartup').value.trim()||'main.py',auto_restart:document.getElementById('stAR').value==='true'};
  const r=await apiFetch(`/api/bot/${curBot}/settings`,{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});if(!r)return;
  const upd=await r.json();botRegistry[curBot]={...botRegistry[curBot],...upd};document.getElementById('tbBot').textContent=data.name||curBot;renderBotList();toast('Config saved','success');
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
  const tray=document.getElementById('toastTray'),icons={success:'✓',error:'✕',info:'ℹ'};
  const t=document.createElement('div');t.className=`toast ${type}`;t.innerHTML=`<span>${icons[type]||'·'}</span> ${escH(msg)}`;tray.appendChild(t);
  setTimeout(()=>{t.style.transition='all .5s';t.style.opacity='0';t.style.transform='translateX(40px)';setTimeout(()=>t.remove(),500)},3000);
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

    # Use relative_path sent by JS (preserves folder structure).
    # Fall back to raw filename if not provided.
    raw_relative = (request.form.get('relative_path') or file.filename or '').strip()
    if not raw_relative:
        return jsonify({'error': 'could not determine filename'}), 400

    # Sanitise every path segment to block traversal.
    parts = [p for p in raw_relative.replace('\\', '/').split('/') if p and p != '.']
    safe_parts = []
    for p in parts:
        s = secure_filename(p)
        if s:
            safe_parts.append(s)
        else:
            # secure_filename returned '' (e.g. unicode-only name) — clean manually
            cleaned = p.replace('..', '').replace('/', '').replace('\\', '').strip()
            if cleaned:
                safe_parts.append(cleaned)

    if not safe_parts:
        return jsonify({'error': 'invalid filename after sanitisation'}), 400

    # For folder uploads, webkitRelativePath includes root folder as first segment.
    # Drop it so files land in bot_dir root, not a subfolder.
    if len(safe_parts) > 1:
        safe_parts = safe_parts[1:]

    rel_path = os.path.join(*safe_parts)
    dest = os.path.abspath(os.path.join(abs_bd, rel_path))

    # Final path traversal guard
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