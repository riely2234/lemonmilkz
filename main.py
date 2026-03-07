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
<title>Vortex — Hosting Platform</title>
<script src="https://cdn.socket.io/4.6.1/socket.io.min.js"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;500;600;700;800&family=JetBrains+Mono:wght@300;400;500;700&family=Instrument+Sans:wght@400;500;600&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}

:root{
  --void:#08080B;
  --base:#0E0E12;
  --surface:#141419;
  --elevated:#1B1B22;
  --overlay:#22222C;
  --border:rgba(255,255,255,0.055);
  --border-mid:rgba(255,255,255,0.1);
  --border-hi:rgba(255,255,255,0.18);
  --amber:#E8A855;
  --amber-dim:rgba(232,168,85,0.12);
  --amber-glow:rgba(232,168,85,0.35);
  --mint:#4DFFCC;
  --mint-dim:rgba(77,255,204,0.1);
  --mint-glow:rgba(77,255,204,0.3);
  --rose:#FF4D6D;
  --rose-dim:rgba(255,77,109,0.12);
  --blue:#5B8DEF;
  --blue-dim:rgba(91,141,239,0.12);
  --text:#F0EDE8;
  --text-2:#9E9EA8;
  --text-3:#52525E;
  --font-display:'Syne',sans-serif;
  --font-body:'Instrument Sans',sans-serif;
  --font-mono:'JetBrains Mono',monospace;
  --radius:8px;
  --radius-lg:12px;
  --ease-out:cubic-bezier(0.16,1,0.3,1);
  --ease-spring:cubic-bezier(0.34,1.56,0.64,1);
}

html,body{width:100%;height:100%;overflow:hidden;background:var(--void);color:var(--text);font-family:var(--font-body);font-size:14px;-webkit-font-smoothing:antialiased}

/* Scanline overlay */
body::before{
  content:'';position:fixed;inset:0;z-index:9999;pointer-events:none;
  background:repeating-linear-gradient(0deg,transparent,transparent 2px,rgba(0,0,0,0.015) 2px,rgba(0,0,0,0.015) 4px);
  animation:scanline 8s linear infinite;
}
@keyframes scanline{0%{background-position:0 0}100%{background-position:0 100px}}

/* Dot grid bg */
body::after{
  content:'';position:fixed;inset:0;z-index:-1;pointer-events:none;
  background-image:radial-gradient(circle,rgba(255,255,255,0.035) 1px,transparent 1px);
  background-size:28px 28px;
}

#app{display:flex;width:100%;height:100%;position:relative}

::-webkit-scrollbar{width:4px;height:4px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--overlay);border-radius:4px}
::-webkit-scrollbar-thumb:hover{background:var(--border-mid)}

/* ═══════════════════ SIDEBAR ═══════════════════ */
.sidebar{
  width:248px;min-width:248px;height:100%;display:flex;flex-direction:column;
  background:var(--base);border-right:1px solid var(--border);
  position:relative;z-index:9500;transition:transform .35s var(--ease-out);
}

.sidebar-close-btn{display:none;position:absolute;right:16px;top:20px;background:none;border:none;color:var(--text-3);font-size:16px;cursor:pointer;transition:color .2s;z-index:10;width:32px;height:32px;border-radius:6px;align-items:center;justify-content:center}
.sidebar-close-btn:hover{color:var(--text);background:var(--elevated)}

.logo{padding:24px 20px 20px;border-bottom:1px solid var(--border)}
.logo-mark{display:flex;align-items:center;gap:10px;margin-bottom:4px}
.logo-hex{width:28px;height:28px;background:var(--amber);border-radius:6px;display:flex;align-items:center;justify-content:center;flex-shrink:0;box-shadow:0 0 20px var(--amber-glow)}
.logo-hex svg{width:16px;height:16px;fill:#000}
.logo-name{font-family:var(--font-display);font-size:18px;font-weight:800;letter-spacing:.5px;color:var(--text)}
.logo-sub{font-family:var(--font-mono);font-size:10px;color:var(--text-3);letter-spacing:3px;text-transform:uppercase;margin-left:38px}

.nav{padding:16px 12px;flex-shrink:0}
.nav-section{font-family:var(--font-mono);font-size:9.5px;font-weight:500;letter-spacing:3px;color:var(--text-3);text-transform:uppercase;padding:8px 8px 6px;margin-top:6px}
.nav-item{
  display:flex;align-items:center;gap:10px;padding:9px 10px;border-radius:var(--radius);
  font-size:13.5px;font-weight:500;color:var(--text-2);cursor:pointer;
  transition:all .18s ease;margin-bottom:2px;position:relative;
}
.nav-item:hover{background:var(--elevated);color:var(--text)}
.nav-item.active{background:var(--amber-dim);color:var(--amber)}
.nav-item.active::before{content:'';position:absolute;left:0;top:50%;transform:translateY(-50%);width:3px;height:60%;background:var(--amber);border-radius:0 3px 3px 0;box-shadow:0 0 8px var(--amber-glow)}
.nav-icon{width:18px;height:18px;display:flex;align-items:center;justify-content:center;font-size:14px;opacity:.7;flex-shrink:0;transition:opacity .18s}
.nav-item.active .nav-icon{opacity:1}
.nav-item:hover .nav-icon{opacity:1}

.instances-header{display:flex;align-items:center;justify-content:space-between;padding:8px 20px 8px;border-top:1px solid var(--border);margin-top:4px;flex-shrink:0}
.instances-label{font-family:var(--font-mono);font-size:9.5px;letter-spacing:3px;text-transform:uppercase;color:var(--text-3)}
.instances-count{
  background:var(--elevated);border:1px solid var(--border-mid);
  border-radius:4px;padding:2px 7px;font-family:var(--font-mono);font-size:11px;
  color:var(--amber);font-weight:500;
}

.new-bot-btn{
  margin:0 12px 10px;display:flex;align-items:center;justify-content:center;gap:8px;
  padding:10px;border:1px dashed var(--border-mid);border-radius:var(--radius);
  font-size:12.5px;font-weight:600;color:var(--text-3);background:transparent;
  transition:all .2s;font-family:var(--font-display);letter-spacing:.5px;cursor:pointer;
}
.new-bot-btn:hover{border-color:var(--amber);color:var(--amber);background:var(--amber-dim);transform:none}
.new-bot-btn-icon{width:20px;height:20px;border-radius:5px;background:var(--elevated);display:flex;align-items:center;justify-content:center;font-size:14px;transition:background .2s}
.new-bot-btn:hover .new-bot-btn-icon{background:var(--amber-dim)}

.bot-list{flex:1;overflow-y:auto;padding:0 12px 12px}
.bot-item{
  display:flex;align-items:center;gap:10px;padding:10px 10px;border-radius:var(--radius);
  cursor:pointer;transition:all .18s ease;margin-bottom:4px;
  border:1px solid transparent;
}
.bot-item:hover{background:var(--elevated)}
.bot-item.active{background:var(--elevated);border-color:var(--border-mid)}
.bot-dot{width:7px;height:7px;border-radius:50%;flex-shrink:0;transition:all .3s}
.bot-dot.online{background:var(--mint);box-shadow:0 0 8px var(--mint-glow)}
.bot-dot.online::after{content:'';position:absolute;width:7px;height:7px;border-radius:50%;background:var(--mint);animation:dotPulse 2s ease-in-out infinite;opacity:.4}
@keyframes dotPulse{0%,100%{transform:scale(1);opacity:.4}50%{transform:scale(2.5);opacity:0}}
.bot-dot{position:relative}
.bot-dot.offline{background:var(--text-3)}
.bot-info{flex:1;min-width:0}
.bot-name{font-size:13px;font-weight:500;color:var(--text-2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;transition:color .2s}
.bot-item.active .bot-name,.bot-item:hover .bot-name{color:var(--text)}
.bot-state{font-family:var(--font-mono);font-size:10px;color:var(--text-3);margin-top:2px;letter-spacing:1px}
.bot-item.active .bot-state.online{color:var(--mint)}
.bot-shared-badge{font-family:var(--font-mono);font-size:9px;color:var(--blue);background:var(--blue-dim);border:1px solid rgba(91,141,239,0.25);border-radius:3px;padding:1px 5px;flex-shrink:0}

.sidebar-footer{
  padding:14px 20px;border-top:1px solid var(--border);
  display:flex;justify-content:space-between;align-items:center;
  background:var(--void);flex-shrink:0;
}
.logout-btn{font-family:var(--font-mono);font-size:11px;letter-spacing:1px;color:var(--text-3);cursor:pointer;transition:color .2s;text-transform:uppercase;background:none;border:none}
.logout-btn:hover{color:var(--rose)}
.sidebar-clock{font-family:var(--font-mono);font-size:12px;color:var(--text-3)}

/* ═══════════════════ OVERLAY (mobile) ═══════════════════ */
.sidebar-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,0.6);z-index:9000;opacity:0;transition:opacity .3s;cursor:pointer;backdrop-filter:blur(2px)}
.sidebar-overlay.open{opacity:1}

/* ═══════════════════ MOBILE BOTTOM NAV ═══════════════════ */
.mobile-bottom-nav{
  display:none;position:fixed;bottom:14px;left:14px;right:14px;
  height:64px;background:rgba(20,20,26,0.92);
  backdrop-filter:blur(20px);-webkit-backdrop-filter:blur(20px);
  border:1px solid var(--border-mid);border-radius:18px;
  z-index:9000;justify-content:space-around;align-items:center;padding:0 8px;
  box-shadow:0 8px 32px rgba(0,0,0,0.6),0 0 0 1px rgba(255,255,255,0.04);
}
.m-nav-item{
  display:flex;flex-direction:column;align-items:center;gap:3px;
  color:var(--text-3);cursor:pointer;transition:all .2s var(--ease-out);
  padding:8px 12px;border-radius:12px;flex:1;
}
.m-nav-icon{font-size:18px;transition:transform .2s var(--ease-spring)}
.m-nav-label{font-size:9px;font-weight:600;letter-spacing:.5px;text-transform:uppercase;font-family:var(--font-display)}
.m-nav-item.active{color:var(--amber)}
.m-nav-item.active .m-nav-icon{transform:translateY(-2px) scale(1.1)}
.m-nav-item:hover{background:var(--elevated);color:var(--text)}

/* ═══════════════════ TOPBAR ═══════════════════ */
.main{flex:1;min-width:0;height:100%;display:flex;flex-direction:column;position:relative;z-index:10}

.topbar{
  height:60px;min-height:60px;background:var(--base);border-bottom:1px solid var(--border);
  display:flex;align-items:center;justify-content:space-between;padding:0 24px;gap:16px;
  flex-shrink:0;
}
.topbar-left{display:flex;align-items:center;gap:14px;min-width:0}
.mobile-menu-btn{display:none;background:var(--elevated);border:1px solid var(--border);color:var(--text-2);padding:7px 10px;border-radius:var(--radius);font-size:15px;cursor:pointer;transition:all .2s}
.mobile-menu-btn:hover{border-color:var(--border-mid);color:var(--text)}

.breadcrumb{display:flex;align-items:center;gap:8px;min-width:0}
.bc-brand{font-family:var(--font-display);font-size:13px;font-weight:700;color:var(--text-3)}
.bc-sep{color:var(--text-3);font-size:12px;opacity:.5}
.bc-page{font-family:var(--font-display);font-size:15px;font-weight:700;color:var(--text)}
.bc-bot{
  font-family:var(--font-mono);font-size:11px;color:var(--blue);
  background:var(--blue-dim);padding:3px 10px;border-radius:4px;
  border:1px solid rgba(91,141,239,0.2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:160px;
}

.topbar-right{display:flex;align-items:center;gap:8px;flex-shrink:0}

.status-pill{
  display:flex;align-items:center;gap:7px;padding:6px 12px;border-radius:20px;
  font-family:var(--font-mono);font-size:11px;font-weight:500;letter-spacing:.5px;
  transition:all .3s;border:1px solid var(--border);background:var(--elevated);
}
.status-pill.online{color:var(--mint);border-color:rgba(77,255,204,0.25);background:var(--mint-dim)}
.status-pill.offline{color:var(--text-3);border-color:var(--border)}
.status-dot{width:6px;height:6px;border-radius:50%;flex-shrink:0}
.status-pill.online .status-dot{background:var(--mint);box-shadow:0 0 6px var(--mint-glow);animation:statusBlink 1.8s ease-in-out infinite}
.status-pill.offline .status-dot{background:var(--text-3)}
@keyframes statusBlink{0%,100%{opacity:1}50%{opacity:.3}}

/* ═══════════════════ BUTTONS ═══════════════════ */
.btn{
  display:inline-flex;align-items:center;justify-content:center;gap:6px;
  padding:8px 16px;border-radius:var(--radius);font-size:12px;font-weight:600;
  letter-spacing:.3px;border:none;cursor:pointer;transition:all .18s ease;
  font-family:var(--font-display);white-space:nowrap;position:relative;overflow:hidden;
}
.btn:active{transform:scale(0.96)!important}

.btn-amber{background:var(--amber);color:#000;box-shadow:0 2px 12px var(--amber-glow)}
.btn-amber:hover{filter:brightness(1.08);transform:translateY(-1px)}

.btn-ghost{background:var(--elevated);color:var(--text-2);border:1px solid var(--border)}
.btn-ghost:hover{background:var(--overlay);color:var(--text);border-color:var(--border-mid);transform:translateY(-1px)}

.btn-mint{background:var(--mint-dim);color:var(--mint);border:1px solid rgba(77,255,204,0.25)}
.btn-mint:hover{background:rgba(77,255,204,0.18);transform:translateY(-1px)}

.btn-rose{background:var(--rose-dim);color:var(--rose);border:1px solid rgba(255,77,109,0.25)}
.btn-rose:hover{background:rgba(255,77,109,0.2);transform:translateY(-1px)}

.btn-blue{background:var(--blue-dim);color:var(--blue);border:1px solid rgba(91,141,239,0.25)}
.btn-blue:hover{background:rgba(91,141,239,0.2);transform:translateY(-1px)}

.btn-sm{padding:6px 12px;font-size:11.5px}
.btn-row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}

/* ═══════════════════ PAGES ═══════════════════ */
.page{flex:1;min-height:0;overflow-y:auto;padding:28px;display:none}
.page.active{display:block;animation:pageIn .3s var(--ease-out)}
@keyframes pageIn{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:translateY(0)}}

/* ═══════════════════ STAT CARDS ═══════════════════ */
.stats-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:24px}
.stat-card{
  background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);
  padding:20px;position:relative;overflow:hidden;transition:all .2s ease;
}
.stat-card:hover{border-color:var(--border-mid);transform:translateY(-2px);background:var(--elevated)}
.stat-card::after{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:var(--stat-accent,var(--amber));opacity:.6}
.stat-card-amber{--stat-accent:var(--amber)}
.stat-card-mint{--stat-accent:var(--mint)}
.stat-card-blue{--stat-accent:var(--blue)}
.stat-card-rose{--stat-accent:var(--rose)}

.stat-label{font-family:var(--font-mono);font-size:10px;letter-spacing:2px;text-transform:uppercase;color:var(--text-3);margin-bottom:12px;display:flex;align-items:center;gap:8px}
.stat-label-dot{width:5px;height:5px;border-radius:50%;background:var(--stat-accent,var(--amber))}
.stat-number{font-family:var(--font-display);font-size:38px;font-weight:800;line-height:1;margin-bottom:6px;color:var(--text)}
.stat-number.online{color:var(--mint)}
.stat-number.offline{color:var(--rose)}
.stat-number.amber{color:var(--amber)}
.stat-sub{font-family:var(--font-mono);font-size:10.5px;color:var(--text-3)}

/* ═══════════════════ PANELS ═══════════════════ */
.panel{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);margin-bottom:20px;overflow:hidden}
.panel-head{
  display:flex;align-items:center;justify-content:space-between;
  padding:16px 20px;border-bottom:1px solid var(--border);
  flex-wrap:wrap;gap:12px;background:var(--base);
}
.panel-title{font-family:var(--font-display);font-size:14px;font-weight:700;color:var(--text);letter-spacing:.3px;display:flex;align-items:center;gap:10px}
.panel-title-icon{width:26px;height:26px;border-radius:6px;background:var(--elevated);display:flex;align-items:center;justify-content:center;font-size:12px}
.panel-badge{font-family:var(--font-mono);font-size:10px;letter-spacing:1px;text-transform:uppercase;color:var(--amber);background:var(--amber-dim);padding:3px 8px;border-radius:4px;border:1px solid rgba(232,168,85,0.25)}
.panel-body{padding:20px}

/* ═══════════════════ TERMINAL ═══════════════════ */
.term-titlebar{
  background:var(--void);border-bottom:1px solid var(--border);
  padding:11px 16px;display:flex;align-items:center;gap:10px;
}
.term-traffic{display:flex;gap:6px}
.term-traffic-dot{width:11px;height:11px;border-radius:50%}
.term-name{flex:1;text-align:center;font-family:var(--font-mono);font-size:10px;letter-spacing:3px;color:var(--text-3);text-transform:uppercase}

.terminal{
  background:var(--void);padding:16px;overflow-y:auto;
  font-family:var(--font-mono);font-size:13px;line-height:1.65;
}
.log-row{display:flex;align-items:baseline;gap:12px;padding:2px 0;border-radius:4px;transition:background .15s}
.log-row:hover{background:rgba(255,255,255,0.025)}
.log-ts{font-size:10.5px;color:var(--text-3);flex-shrink:0;min-width:56px}
.log-badge{font-size:9.5px;padding:2px 6px;border-radius:3px;flex-shrink:0;text-transform:uppercase;font-weight:600;letter-spacing:1.5px;font-family:var(--font-mono)}
.log-badge.sys{background:var(--blue-dim);color:var(--blue)}
.log-badge.err{background:var(--rose-dim);color:var(--rose)}
.log-badge.ok{background:var(--mint-dim);color:var(--mint)}
.log-badge.warn{background:rgba(232,168,85,0.12);color:var(--amber)}
.log-badge.out{background:var(--elevated);color:var(--text-3)}
.log-msg{flex:1;word-break:break-all}
.log-msg.sys{color:var(--blue)}
.log-msg.err{color:var(--rose)}
.log-msg.ok{color:var(--mint)}
.log-msg.warn{color:var(--amber)}
.log-msg.out{color:var(--text-2)}

.term-input-area{
  display:flex;align-items:center;gap:12px;
  background:var(--base);border-top:1px solid var(--border);
  padding:12px 16px;transition:all .2s;
}
.term-input-area:focus-within{background:var(--surface);border-top-color:var(--amber)}
.term-prompt{font-family:var(--font-mono);font-size:14px;color:var(--amber);flex-shrink:0;animation:cursorAnim 1.2s step-end infinite}
@keyframes cursorAnim{0%,100%{opacity:1}50%{opacity:0.3}}
.term-input{flex:1;background:none;border:none;outline:none;font-family:var(--font-mono);font-size:13.5px;color:var(--text);caret-color:var(--amber)}
.term-input::placeholder{color:var(--text-3)}

/* ═══════════════════ FORMS ═══════════════════ */
.form-group{margin-bottom:20px}
.form-label{
  display:flex;align-items:center;gap:8px;
  font-family:var(--font-mono);font-size:10px;font-weight:500;letter-spacing:2px;
  color:var(--text-3);text-transform:uppercase;margin-bottom:8px;
}
.form-label::after{content:'';flex:1;height:1px;background:var(--border)}

.form-input,.form-select,.form-textarea{
  width:100%;background:var(--elevated);border:1px solid var(--border);
  border-radius:var(--radius);padding:11px 14px;font-size:13.5px;
  color:var(--text);outline:none;font-family:var(--font-mono);transition:all .2s;
}
.form-input:focus,.form-select:focus,.form-textarea:focus{
  border-color:rgba(232,168,85,0.5);background:var(--overlay);box-shadow:0 0 0 3px rgba(232,168,85,0.08);
}
.form-input::placeholder,.form-textarea::placeholder{color:var(--text-3)}
.form-select option{background:var(--base)}
.form-textarea{resize:vertical;min-height:110px;line-height:1.6}
.form-row-2{display:grid;grid-template-columns:1fr 1fr;gap:20px}

.section-divider{
  font-family:var(--font-display);font-size:13px;font-weight:700;letter-spacing:.5px;
  color:var(--text);border-bottom:1px solid var(--border);padding-bottom:10px;margin:28px 0 18px;
  display:flex;align-items:center;gap:10px;
}
.section-divider::before{content:'';width:3px;height:14px;background:var(--amber);border-radius:2px}

/* ═══════════════════ ENV ROWS ═══════════════════ */
.env-row{display:grid;grid-template-columns:1fr 1.5fr auto;gap:8px;margin-bottom:8px;align-items:center}
.env-field{
  background:var(--elevated);border:1px solid var(--border);border-radius:var(--radius);
  padding:9px 12px;font-family:var(--font-mono);font-size:12.5px;color:var(--text);
  outline:none;width:100%;transition:border-color .2s;
}
.env-field:focus{border-color:rgba(232,168,85,0.4)}
.env-key{color:var(--amber)}

/* ═══════════════════ FILE MANAGER ═══════════════════ */
.file-table{width:100%;border-collapse:separate;border-spacing:0;min-width:520px}
.file-table th{
  font-family:var(--font-mono);font-size:10px;text-transform:uppercase;letter-spacing:2.5px;
  color:var(--text-3);padding:11px 14px;border-bottom:1px solid var(--border);text-align:left;
  font-weight:500;background:var(--base);
}
.file-table td{
  padding:11px 14px;font-size:13px;border-bottom:1px solid var(--border);
  vertical-align:middle;font-family:var(--font-mono);
}
.file-table tr:last-child td{border-bottom:none}
.file-table tr:hover td{background:rgba(255,255,255,0.02)}
.file-name-link{
  display:flex;align-items:center;gap:8px;color:var(--text-2);cursor:pointer;
  transition:all .18s;font-weight:400;overflow:hidden;
}
.file-name-link:hover{color:var(--amber);transform:translateX(3px)}
.file-icon{font-size:13px;flex-shrink:0;opacity:.7}
.file-name-text{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:200px}
.file-type-chip{
  font-size:9.5px;letter-spacing:1px;text-transform:uppercase;
  padding:2px 7px;border:1px solid var(--border-mid);border-radius:4px;
  color:var(--text-3);background:var(--elevated);
}

/* ═══════════════════ UPLOAD ═══════════════════ */
.drop-zone{
  border:2px dashed var(--border-mid);padding:36px 24px;text-align:center;
  transition:all .3s var(--ease-out);background:transparent;border-radius:var(--radius-lg);cursor:pointer;
}
.drop-zone.dragging{border-color:var(--amber);background:var(--amber-dim);box-shadow:0 0 0 4px rgba(232,168,85,0.08)}
.drop-icon{font-size:36px;margin-bottom:12px;display:block;color:var(--text-3);transition:all .3s;pointer-events:none}
.drop-zone.dragging .drop-icon{color:var(--amber);transform:translateY(-4px)}
.drop-title{font-family:var(--font-display);font-size:22px;font-weight:700;color:var(--text);margin-bottom:6px;pointer-events:none}
.drop-hint{font-family:var(--font-mono);font-size:11px;letter-spacing:1px;color:var(--text-3);margin-bottom:20px;pointer-events:none}

.upload-row{
  display:flex;align-items:center;gap:10px;padding:10px 14px;
  background:var(--elevated);border:1px solid var(--border);
  border-radius:var(--radius);margin-top:8px;
  font-family:var(--font-mono);font-size:11.5px;color:var(--text-2);
}
.upload-bar-wrap{flex:1;height:3px;background:var(--overlay);border-radius:2px;overflow:hidden}
.upload-bar-fill{height:100%;background:var(--amber);border-radius:2px;transition:width .12s linear}

/* ═══════════════════ RESOURCES ═══════════════════ */
.res-item{margin-bottom:24px}
.res-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px}
.res-label{font-family:var(--font-mono);font-size:10.5px;letter-spacing:2px;text-transform:uppercase;color:var(--text-3)}
.res-value{font-family:var(--font-display);font-size:22px;font-weight:700;color:var(--text)}
.res-track{height:6px;background:var(--overlay);border-radius:4px;overflow:hidden}
.res-fill{height:100%;border-radius:4px;transition:width 1s ease}

/* ═══════════════════ MODALS ═══════════════════ */
.modal-veil{
  display:none;position:fixed;inset:0;
  background:rgba(0,0,0,0.65);z-index:10000;
  align-items:center;justify-content:center;
  backdrop-filter:blur(8px);-webkit-backdrop-filter:blur(8px);
}
.modal-veil.open{display:flex;animation:veilIn .25s ease}
@keyframes veilIn{from{opacity:0}to{opacity:1}}

.modal{
  background:var(--surface);border:1px solid var(--border-mid);
  border-radius:16px;padding:32px;width:95%;max-width:560px;
  max-height:90vh;overflow-y:auto;
  box-shadow:0 24px 80px rgba(0,0,0,0.8),0 0 0 1px rgba(255,255,255,0.04);
  animation:modalIn .3s var(--ease-spring);
}
.modal.wide{max-width:960px}
@keyframes modalIn{from{transform:scale(0.94) translateY(20px);opacity:0}to{transform:scale(1) translateY(0);opacity:1}}

.modal-title{
  font-family:var(--font-display);font-size:24px;font-weight:800;color:var(--text);
  margin-bottom:24px;letter-spacing:.3px;display:flex;align-items:center;gap:10px;
}
.modal-title-accent{color:var(--amber)}
.modal-footer{
  display:flex;justify-content:flex-end;gap:10px;margin-top:24px;
  padding-top:20px;border-top:1px solid var(--border);
}

/* ═══════════════════ LOGIN ═══════════════════ */
#loginOverlay{
  position:fixed;inset:0;background:var(--void);z-index:99999;
  display:flex;align-items:center;justify-content:center;
}
#loginOverlay::before{
  content:'';position:absolute;inset:0;pointer-events:none;
  background:radial-gradient(ellipse at 30% 40%,rgba(232,168,85,0.06) 0%,transparent 60%),
             radial-gradient(ellipse at 80% 70%,rgba(77,255,204,0.04) 0%,transparent 50%);
}

.login-wrap{
  background:var(--surface);border:1px solid var(--border);border-radius:20px;
  padding:48px 44px;width:90%;max-width:420px;
  box-shadow:0 32px 80px rgba(0,0,0,0.7),0 0 0 1px rgba(255,255,255,0.03);
  animation:loginReveal .7s var(--ease-spring);position:relative;z-index:1;
}
@keyframes loginReveal{from{transform:translateY(30px) scale(0.96);opacity:0}to{transform:translateY(0) scale(1);opacity:1}}

.login-logo{display:flex;align-items:center;gap:12px;margin-bottom:8px;justify-content:center}
.login-hex{width:40px;height:40px;background:var(--amber);border-radius:10px;display:flex;align-items:center;justify-content:center;box-shadow:0 0 30px var(--amber-glow)}
.login-hex svg{width:22px;height:22px;fill:#000}
.login-brand{font-family:var(--font-display);font-size:26px;font-weight:800;color:var(--text)}
.login-tagline{font-family:var(--font-mono);font-size:10px;letter-spacing:4px;text-transform:uppercase;color:var(--text-3);text-align:center;margin-bottom:28px}

.auth-tabs{display:flex;background:var(--elevated);border-radius:var(--radius);padding:3px;margin-bottom:22px;border:1px solid var(--border)}
.auth-tab{
  flex:1;text-align:center;padding:8px 12px;font-family:var(--font-display);font-size:12px;
  font-weight:600;letter-spacing:.5px;color:var(--text-3);cursor:pointer;border-radius:6px;transition:all .2s;
}
.auth-tab.active{background:var(--amber);color:#000;box-shadow:0 2px 8px var(--amber-glow)}

/* ═══════════════════ CODE EDITOR ═══════════════════ */
.code-editor{
  width:100%;min-height:520px;background:var(--void);border:1px solid var(--border);
  border-radius:var(--radius);padding:18px;font-family:var(--font-mono);font-size:13.5px;
  color:var(--text);outline:none;resize:vertical;line-height:1.7;caret-color:var(--amber);
  transition:border-color .2s;
}
.code-editor:focus{border-color:rgba(232,168,85,0.4)}

/* ═══════════════════ DANGER ZONE ═══════════════════ */
.danger-zone{
  border:1px solid rgba(255,77,109,0.2);border-left:3px solid var(--rose);
  background:linear-gradient(90deg,rgba(255,77,109,0.05),transparent);
  padding:20px;border-radius:var(--radius-lg);margin-top:20px;
}

/* ═══════════════════ TOASTS ═══════════════════ */
.toast-stack{position:fixed;bottom:28px;right:28px;z-index:20000;display:flex;flex-direction:column;gap:10px;pointer-events:none}
.toast{
  background:var(--elevated);border:1px solid var(--border-mid);border-radius:10px;
  padding:13px 18px;font-size:13px;font-weight:500;color:var(--text);
  font-family:var(--font-display);
  box-shadow:0 8px 24px rgba(0,0,0,0.6),0 0 0 1px rgba(255,255,255,0.04);
  display:flex;align-items:center;gap:10px;pointer-events:all;
  animation:toastIn .35s var(--ease-spring);min-width:260px;
}
.toast-icon{width:24px;height:24px;border-radius:6px;display:flex;align-items:center;justify-content:center;font-size:12px;flex-shrink:0}
.toast.success .toast-icon{background:var(--mint-dim);color:var(--mint)}
.toast.error .toast-icon{background:var(--rose-dim);color:var(--rose)}
.toast.info .toast-icon{background:var(--blue-dim);color:var(--blue)}
.toast-close{margin-left:auto;cursor:pointer;color:var(--text-3);font-size:14px;transition:color .2s}
.toast-close:hover{color:var(--text)}
@keyframes toastIn{from{transform:translateX(20px);opacity:0}to{transform:translateX(0);opacity:1}}

/* ═══════════════════ SUBUSER LIST ═══════════════════ */
.subuser-row{
  display:flex;justify-content:space-between;align-items:center;
  padding:10px 14px;background:var(--elevated);border:1px solid var(--border);
  border-radius:var(--radius);margin-bottom:6px;font-family:var(--font-mono);font-size:13px;
}

/* ═══════════════════ RESPONSIVE ═══════════════════ */
@media(max-width:860px){
  .sidebar{position:fixed;left:0;transform:translateX(-100%);width:272px;z-index:9600}
  .sidebar.open{transform:translateX(0)}
  .sidebar-overlay{display:block}
  .sidebar .nav{display:none}
  .sidebar-close-btn{display:flex}
  .mobile-bottom-nav{display:flex}
  .mobile-menu-btn{display:flex}
  .main{padding-bottom:88px}
  .topbar{padding:0 16px;height:56px}
  .bc-brand{display:none}
  .bc-sep{display:none}
  .bc-bot{max-width:110px;font-size:10px}
  .topbar-right{gap:6px}
  .topbar-right .status-pill span{display:none}
  .topbar-right .btn span{display:none}
  .topbar-right .btn{padding:7px 10px}
  .page{padding:14px}
  .stats-grid{grid-template-columns:1fr 1fr;gap:10px;margin-bottom:16px}
  .stat-number{font-size:28px}
  .form-row-2{grid-template-columns:1fr;gap:14px}
  .panel-head{padding:12px 14px}
  .panel-body{padding:14px}
  .panel-title{font-size:13px}
  .file-table-wrap{overflow-x:auto;-webkit-overflow-scrolling:touch}
  .file-table{min-width:0;width:100%}
  .file-table th:nth-child(4),.file-table td:nth-child(4){display:none}
  .file-table th:nth-child(2),.file-table td:nth-child(2){display:none}
  .drop-title{font-size:18px}
  .toast-stack{bottom:88px;right:12px;left:12px}
  .toast{min-width:0;width:100%}
}
@media(max-width:480px){
  .stats-grid{grid-template-columns:1fr}
  .stat-number{font-size:26px}
  .login-wrap{padding:32px 20px;width:96%}
  .modal{padding:20px 16px}
  .modal-title{font-size:20px;margin-bottom:16px}
  .page{padding:10px}
  .topbar-right .status-pill{display:none}
}
</style>
</head>
<body>
<!-- Login overlay -->
<div id="loginOverlay">
  <div class="login-wrap">
    <div class="login-logo">
      <div class="login-hex">
        <svg viewBox="0 0 24 24"><path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5"/></svg>
      </div>
      <span class="login-brand">Vortex</span>
    </div>
    <div class="login-tagline">Hosting Platform</div>
    <div class="auth-tabs">
      <div class="auth-tab active" id="tabLogin" onclick="switchAuthMode('login')">Sign In</div>
      <div class="auth-tab" id="tabRegister" onclick="switchAuthMode('register')">Register</div>
    </div>
    <div class="form-group" style="margin-bottom:12px">
      <input class="form-input" id="authUsername" placeholder="Username" autocomplete="username" style="font-size:14px" onkeydown="if(event.key==='Enter')submitAuth()">
    </div>
    <div class="form-group" style="margin-bottom:20px">
      <input type="password" class="form-input" id="authPassword" placeholder="Password" autocomplete="current-password" style="font-size:14px" onkeydown="if(event.key==='Enter')submitAuth()">
    </div>
    <button class="btn btn-amber" id="authBtn" style="width:100%;padding:13px;font-size:14px;letter-spacing:.5px" onclick="submitAuth()">Sign In</button>
  </div>
</div>

<div class="sidebar-overlay" onclick="toggleSidebar()"></div>

<div id="app">
  <!-- Sidebar -->
  <aside class="sidebar">
    <button class="sidebar-close-btn" onclick="toggleSidebar()">✕</button>
    <div class="logo">
      <div class="logo-mark">
        <div class="logo-hex">
          <svg viewBox="0 0 24 24" style="width:15px;height:15px;fill:none;stroke:#000;stroke-width:2.5;stroke-linecap:round"><path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5"/></svg>
        </div>
        <span class="logo-name">Vortex</span>
      </div>
      <div class="logo-sub">Hosting Platform</div>
    </div>

    <nav class="nav">
      <div class="nav-section">System</div>
      <div class="nav-item active" data-page="dashboard" onclick="navTo('dashboard',this)">
        <span class="nav-icon">⬡</span> Dashboard
      </div>
      <div class="nav-item" data-page="console" onclick="navTo('console',this)">
        <span class="nav-icon">▸</span> Console
      </div>
      <div class="nav-item" data-page="files" onclick="navTo('files',this)">
        <span class="nav-icon">⊞</span> Files
      </div>
      <div class="nav-section">Configure</div>
      <div class="nav-item" data-page="env" onclick="navTo('env',this)">
        <span class="nav-icon">◎</span> Environment
      </div>
      <div class="nav-item" data-page="settings" onclick="navTo('settings',this)">
        <span class="nav-icon">◈</span> Settings
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
      <div class="new-bot-btn-icon">+</div>
      Deploy New Instance
    </div>
    <div class="bot-list" id="botList"></div>

    <div class="sidebar-footer">
      <button class="logout-btn" onclick="logout()">Sign Out</button>
      <span class="sidebar-clock" id="clock">00:00:00</span>
    </div>
  </aside>

  <!-- Main Content -->
  <main class="main">
    <div class="topbar">
      <div class="topbar-left">
        <button class="mobile-menu-btn" onclick="toggleSidebar()">☰</button>
        <div class="breadcrumb">
          <span class="bc-brand">Vortex</span>
          <span class="bc-sep">/</span>
          <span class="bc-page" id="tbPage">Dashboard</span>
          <span class="bc-sep">·</span>
          <span class="bc-bot" id="tbBot">Select Instance</span>
        </div>
      </div>
      <div class="topbar-right">
        <div class="status-pill offline" id="statusTag">
          <div class="status-dot"></div>
          <span id="statusText">Offline</span>
        </div>
        <button class="btn btn-mint btn-sm" onclick="startBot()"><span>▶</span> <span>Start</span></button>
        <button class="btn btn-rose btn-sm" onclick="stopBot()"><span>■</span> <span>Stop</span></button>
        <button class="btn btn-ghost btn-sm" onclick="restartBot()"><span>↺</span> <span>Restart</span></button>
      </div>
    </div>

    <!-- Dashboard -->
    <div class="page active" id="page-dashboard">
      <div class="stats-grid">
        <div class="stat-card stat-card-mint">
          <div class="stat-label"><span class="stat-label-dot"></span>Status</div>
          <div class="stat-number offline" id="sStat">Offline</div>
          <div class="stat-sub" id="sStatSub">No active process</div>
        </div>
        <div class="stat-card stat-card-amber">
          <div class="stat-label"><span class="stat-label-dot" style="background:var(--amber)"></span>Uptime</div>
          <div class="stat-number amber" id="sUptime">—</div>
          <div class="stat-sub">HH:MM:SS</div>
        </div>
        <div class="stat-card stat-card-blue">
          <div class="stat-label"><span class="stat-label-dot" style="background:var(--blue)"></span>CPU</div>
          <div class="stat-number" style="color:var(--blue)" id="sCpu">—</div>
          <div class="stat-sub">Utilization</div>
        </div>
        <div class="stat-card stat-card-rose">
          <div class="stat-label"><span class="stat-label-dot" style="background:var(--rose)"></span>Memory</div>
          <div class="stat-number" style="color:var(--rose);font-size:28px" id="sMem">—</div>
          <div class="stat-sub">RAM Used</div>
        </div>
      </div>

      <div class="panel">
        <div class="panel-head">
          <div class="panel-title">
            <div class="panel-title-icon">▶</div>
            Launch Control
          </div>
          <span class="panel-badge">Operations</span>
        </div>
        <div class="panel-body">
          <div style="max-width:360px;margin-bottom:20px">
            <div class="form-group" style="margin:0">
              <label class="form-label">Startup File</label>
              <input class="form-input" id="sfInput" value="main.py" placeholder="main.py">
            </div>
          </div>
          <div class="btn-row">
            <button class="btn btn-amber" onclick="startBot()">▶ Start Process</button>
            <button class="btn btn-rose" onclick="stopBot()">■ Stop</button>
            <button class="btn btn-ghost" onclick="restartBot()">↺ Restart</button>
            <button class="btn btn-ghost" onclick="killBot()" style="margin-left:auto;color:var(--rose)">✕ Force Kill</button>
          </div>
        </div>
      </div>

      <div class="panel">
        <div class="panel-head">
          <div class="panel-title"><div class="panel-title-icon">≡</div> Live Output</div>
          <button class="btn btn-ghost btn-sm" onclick="navTo('console',null)">Full Console →</button>
        </div>
        <div class="term-titlebar">
          <div class="term-traffic">
            <div class="term-traffic-dot" style="background:#FF5F57"></div>
            <div class="term-traffic-dot" style="background:#FFBD2E"></div>
            <div class="term-traffic-dot" style="background:#28CA41"></div>
          </div>
          <div class="term-name">stdout</div>
        </div>
        <div class="terminal" id="miniTerm" style="height:220px"></div>
      </div>
    </div>

    <!-- Console -->
    <div class="page" id="page-console">
      <div class="panel">
        <div class="panel-head">
          <div class="panel-title"><div class="panel-title-icon">_</div> Process Console</div>
          <div class="btn-row">
            <button class="btn btn-ghost btn-sm" onclick="clearConsole()">⊘ Clear</button>
            <button class="btn btn-ghost btn-sm" onclick="exportLogs()">↓ Export</button>
          </div>
        </div>
        <div class="term-titlebar">
          <div class="term-traffic">
            <div class="term-traffic-dot" style="background:#FF5F57"></div>
            <div class="term-traffic-dot" style="background:#FFBD2E"></div>
            <div class="term-traffic-dot" style="background:#28CA41"></div>
          </div>
          <div class="term-name" id="termTitle">No Instance Selected</div>
        </div>
        <div class="terminal" id="mainTerm" style="height:460px"></div>
        <div class="term-input-area">
          <span class="term-prompt">❯</span>
          <input class="term-input" id="termIn" placeholder="Send input to stdin..." onkeydown="if(event.key==='Enter')sendInput()">
          <button class="btn btn-amber btn-sm" onclick="sendInput()">Send</button>
        </div>
      </div>
    </div>

    <!-- Files -->
    <div class="page" id="page-files">
      <input type="file" multiple id="fileUploadInput" style="display:none" onchange="handleUpload(this.files,false)">
      <input type="file" webkitdirectory directory multiple id="folderUploadInput" style="display:none" onchange="handleUpload(this.files,true)">
      <div class="panel">
        <div class="panel-head">
          <div class="panel-title"><div class="panel-title-icon">⊞</div> File Manager</div>
          <div class="btn-row">
            <button class="btn btn-ghost btn-sm" onclick="openNewFileModal()">+ New File</button>
            <button class="btn btn-ghost btn-sm" onclick="loadFiles()">↻ Refresh</button>
          </div>
        </div>
        <div class="file-table-wrap">
          <table class="file-table">
            <thead>
              <tr>
                <th>Filename</th>
                <th>Type</th>
                <th>Size</th>
                <th>Modified</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody id="fileList"></tbody>
          </table>
        </div>
        <div style="padding:16px;border-top:1px solid var(--border)">
          <div class="drop-zone" id="dropZone">
            <span class="drop-icon">⬆</span>
            <div class="drop-title">Drop files here</div>
            <div class="drop-hint" style="margin-bottom:18px">ZIP auto-extracted · Folder structure preserved</div>
            <div style="display:flex;gap:10px;justify-content:center;flex-wrap:wrap;position:relative;z-index:5">
              <button class="btn btn-amber" onclick="event.stopPropagation();document.getElementById('fileUploadInput').click()">Upload Files</button>
              <button class="btn btn-ghost" onclick="event.stopPropagation();document.getElementById('folderUploadInput').click()">Upload Folder</button>
            </div>
          </div>
          <div id="uploadProgress" style="margin-top:10px"></div>
        </div>
      </div>
    </div>

    <!-- Environment -->
    <div class="page" id="page-env">
      <div class="panel">
        <div class="panel-head">
          <div class="panel-title"><div class="panel-title-icon">◎</div> Environment Variables</div>
          <button class="btn btn-amber btn-sm" onclick="saveEnv()">Save Variables</button>
        </div>
        <div class="panel-body">
          <p style="font-family:var(--font-mono);font-size:11px;color:var(--text-3);letter-spacing:.5px;margin-bottom:20px">Injected into the process environment at startup.</p>
          <div class="env-row" style="margin-bottom:10px">
            <span style="font-family:var(--font-mono);font-size:10px;letter-spacing:2px;color:var(--text-3);text-transform:uppercase">Key</span>
            <span style="font-family:var(--font-mono);font-size:10px;letter-spacing:2px;color:var(--text-3);text-transform:uppercase">Value</span>
            <span></span>
          </div>
          <div id="envRows"></div>
          <button class="btn btn-ghost btn-sm" onclick="addEnvRow('','')" style="margin-top:12px">+ Add Row</button>
        </div>
      </div>
    </div>

    <!-- Settings -->
    <div class="page" id="page-settings">
      <div class="panel">
        <div class="panel-head">
          <div class="panel-title"><div class="panel-title-icon">◈</div> Instance Settings</div>
        </div>
        <div class="panel-body">
          <div class="form-row-2">
            <div class="form-group"><label class="form-label">Instance Name</label><input class="form-input" id="stName" placeholder="My Server"></div>
            <div class="form-group"><label class="form-label">Startup File</label><input class="form-input" id="stStartup" placeholder="main.py"></div>
          </div>
          <div class="form-group"><label class="form-label">Crash Recovery</label>
            <select class="form-select" id="stAR">
              <option value="false">Disabled</option>
              <option value="true">Auto-restart on crash</option>
            </select>
          </div>
          <button class="btn btn-amber" onclick="saveSettings()">Save Configuration</button>

          <div class="section-divider" id="accessMgmtTitle">Access Management</div>
          <div id="accessMgmtSection">
            <div class="form-group">
              <label class="form-label">Grant Access</label>
              <div style="display:flex;gap:10px">
                <input class="form-input" id="newSubuser" placeholder="Enter username...">
                <button class="btn btn-ghost" onclick="addSubuser()">Grant</button>
              </div>
            </div>
            <div id="subuserList"></div>
          </div>
        </div>
      </div>
      <div class="danger-zone" id="dangerZoneSection">
        <div style="font-family:var(--font-mono);font-size:10px;letter-spacing:2px;text-transform:uppercase;color:var(--rose);margin-bottom:8px;font-weight:600">⚠ Danger Zone</div>
        <div style="font-size:13px;color:var(--text-2);margin-bottom:14px">Permanently deletes this instance and all associated files. This cannot be undone.</div>
        <button class="btn btn-rose" onclick="deleteBot()">Destroy Instance</button>
      </div>
    </div>

    <!-- Resources -->
    <div class="page" id="page-resources">
      <div class="panel">
        <div class="panel-head">
          <div class="panel-title"><div class="panel-title-icon">▣</div> System Resources</div>
          <span class="panel-badge" style="color:var(--mint);border-color:rgba(77,255,204,0.25);background:var(--mint-dim)">Live Telemetry</span>
        </div>
        <div class="panel-body">
          <div class="res-item">
            <div class="res-header">
              <span class="res-label">CPU Utilization</span>
              <span class="res-value" id="rCpu" style="color:var(--blue)">—</span>
            </div>
            <div class="res-track"><div class="res-fill" id="pCpu" style="width:0%;background:var(--blue)"></div></div>
          </div>
          <div class="res-item">
            <div class="res-header">
              <span class="res-label">Memory Usage</span>
              <span class="res-value" id="rMem" style="color:var(--amber)">—</span>
            </div>
            <div class="res-track"><div class="res-fill" id="pMem" style="width:0%;background:var(--amber)"></div></div>
          </div>
          <div class="res-item">
            <div class="res-header">
              <span class="res-label">Disk Usage</span>
              <span class="res-value" id="rDsk" style="color:var(--mint)">—</span>
            </div>
            <div class="res-track"><div class="res-fill" id="pDsk" style="width:0%;background:var(--mint)"></div></div>
          </div>
        </div>
      </div>
    </div>
  </main>

  <!-- Mobile Bottom Nav -->
  <nav class="mobile-bottom-nav">
    <div class="m-nav-item" onclick="toggleSidebar()">
      <span class="m-nav-icon">⊞</span>
      <span class="m-nav-label">Instances</span>
    </div>
    <div class="m-nav-item active" data-page="dashboard" onclick="navTo('dashboard',this)">
      <span class="m-nav-icon">⬡</span>
      <span class="m-nav-label">Dash</span>
    </div>
    <div class="m-nav-item" data-page="console" onclick="navTo('console',this)">
      <span class="m-nav-icon">▸</span>
      <span class="m-nav-label">Console</span>
    </div>
    <div class="m-nav-item" data-page="files" onclick="navTo('files',this)">
      <span class="m-nav-icon">◑</span>
      <span class="m-nav-label">Files</span>
    </div>
    <div class="m-nav-item" data-page="settings" onclick="navTo('settings',this)">
      <span class="m-nav-icon">◈</span>
      <span class="m-nav-label">Config</span>
    </div>
  </nav>
</div>

<div class="toast-stack" id="toastTray"></div>

<!-- Create Modal -->
<div class="modal-veil" id="mCreate">
  <div class="modal">
    <div class="modal-title">Deploy <span class="modal-title-accent">Instance</span></div>
    <div class="form-group"><label class="form-label">Instance Name</label><input class="form-input" id="mName" placeholder="Project Alpha"></div>
    <div class="form-group"><label class="form-label">Startup File</label><input class="form-input" id="mFile" value="main.py" placeholder="main.py"></div>
    <div class="modal-footer">
      <button class="btn btn-ghost" onclick="closeModal('mCreate')">Cancel</button>
      <button class="btn btn-amber" onclick="createBot()">Initialize</button>
    </div>
  </div>
</div>

<!-- Editor Modal -->
<div class="modal-veil" id="mEditor">
  <div class="modal wide">
    <div class="modal-title">Edit <span class="modal-title-accent" id="edName">File</span></div>
    <textarea class="code-editor" id="edContent"></textarea>
    <div class="modal-footer">
      <button class="btn btn-ghost" onclick="closeModal('mEditor')">Discard</button>
      <button class="btn btn-amber" onclick="saveFile()">Save Changes</button>
    </div>
  </div>
</div>

<!-- New File Modal -->
<div class="modal-veil" id="mNewFile">
  <div class="modal">
    <div class="modal-title">Create <span class="modal-title-accent">File</span></div>
    <div class="form-group"><label class="form-label">Filename</label><input class="form-input" id="nfName" placeholder="src/app.py"></div>
    <div class="form-group"><label class="form-label">Initial Content</label><textarea class="form-textarea" id="nfContent" placeholder="# Start writing..." style="height:140px"></textarea></div>
    <div class="modal-footer">
      <button class="btn btn-ghost" onclick="closeModal('mNewFile')">Cancel</button>
      <button class="btn btn-amber" onclick="createNewFile()">Create</button>
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
const PAGE_NAMES={dashboard:'Dashboard',console:'Console',files:'File Manager',env:'Environment',settings:'Settings',resources:'Resources'};
function navTo(name,el){
  document.querySelectorAll('.sidebar .nav-item').forEach(n=>n.classList.remove('active'));
  const d=document.querySelector(`.sidebar .nav-item[data-page="${name}"]`);if(d)d.classList.add('active');
  document.querySelectorAll('.mobile-bottom-nav .m-nav-item').forEach(n=>n.classList.remove('active'));
  const m=document.querySelector(`.mobile-bottom-nav .m-nav-item[data-page="${name}"]`);if(m)m.classList.add('active');
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
  const p=document.getElementById('page-'+name);if(p)p.classList.add('active');
  document.getElementById('tbPage').textContent=PAGE_NAMES[name]||name;
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
  document.getElementById('authBtn').textContent=mode==='login'?'Sign In':'Create Account';
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
  if(!entries.length){el.innerHTML='<div style="padding:20px;text-align:center;color:var(--text-3);font-family:var(--font-mono);font-size:11px;letter-spacing:1px">No instances</div>';return}
  entries.forEach(([id,b])=>{
    const d=document.createElement('div');d.className='bot-item'+(id===curBot?' active':'');
    const s=b.status||'offline',sh=b.is_shared?`<span class="bot-shared-badge">shared</span>`:'';
    d.innerHTML=`<div class="bot-dot ${s}"></div><div class="bot-info"><div class="bot-name">${escH(b.name||id)}</div><div class="bot-state ${s}">${s}</div></div>${sh}`;
    d.onclick=()=>{selectBot(id);if(window.innerWidth<=860&&document.querySelector('.sidebar').classList.contains('open'))toggleSidebar()};
    el.appendChild(d);
  });
}
function selectBot(id){
  curBot=id;const b=botRegistry[id];
  document.getElementById('tbBot').textContent=b?.name||id;
  document.getElementById('sfInput').value=b?.startup_file||'main.py';
  document.getElementById('termTitle').textContent=(b?.name||id)+' — stdout';
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
  document.getElementById('statusTag').className='status-pill '+(on?'online':'offline');
  document.getElementById('statusText').textContent=on?'Online':'Offline';
  document.getElementById('sStat').textContent=on?'Online':'Offline';
  document.getElementById('sStat').className='stat-number '+(on?'online':'offline');
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
async function startBot(){if(!curBot)return;const sf=document.getElementById('sfInput').value.trim()||'main.py';await apiFetch(`/api/bot/${curBot}/start`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({startup_file:sf})});toast('Starting process…','info')}
async function stopBot(){if(!curBot)return;await apiFetch(`/api/bot/${curBot}/stop`,{method:'POST'});toast('Process stopped','success')}
async function restartBot(){if(!curBot)return;await apiFetch(`/api/bot/${curBot}/stop`,{method:'POST'});toast('Restarting…','info');setTimeout(startBot,800)}
async function killBot(){if(!curBot)return;await apiFetch(`/api/bot/${curBot}/kill`,{method:'POST'});toast('Force killed','error')}
async function deleteBot(){
  if(!curBot||!confirm('Permanently destroy this instance?'))return;
  await apiFetch(`/api/bot/${curBot}`,{method:'DELETE'});delete botRegistry[curBot];curBot=null;
  document.getElementById('tbBot').textContent='Select Instance';
  ['mainTerm','miniTerm'].forEach(i=>document.getElementById(i).innerHTML='');
  applyStatus('offline');renderBotList();document.getElementById('botCount').textContent=Object.keys(botRegistry).length;toast('Instance destroyed','error');
}
function appendLog(msg,level,ts){
  const tagMap={system:'sys',error:'err',success:'ok',warn:'warn',default:'out'};
  const tag=tagMap[level]||'out',t=ts||new Date().toTimeString().slice(0,8);
  const row=`<div class="log-row"><span class="log-ts">${escH(t)}</span><span class="log-badge ${tag}">${tag}</span><span class="log-msg ${tag}">${escH(msg)}</span></div>`;
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
const EXT_ICONS={py:'🐍',js:'⚡',json:'{}',txt:'≡',md:'#',zip:'⊞',env:'◎',sh:'$',ts:'⟨⟩',html:'<>',css:'◐',yml:'⚙',yaml:'⚙'};
const EXT_COLORS={py:'#4EC9B0',js:'#E8A855',json:'#5B8DEF',txt:'#9E9EA8',md:'#C792EA',zip:'#FF4D6D',env:'#E8A855',sh:'#4DFFCC',ts:'#5B8DEF',html:'#E06C75',css:'#56B6C2'};
async function loadFiles(){
  const tb=document.getElementById('fileList');
  if(!curBot){tb.innerHTML=`<tr><td colspan="5"><div style="padding:40px;text-align:center;color:var(--text-3);font-family:var(--font-mono);font-size:11px">Select an instance first</div></td></tr>`;return}
  const r=await apiFetch(`/api/bot/${curBot}/files`);if(!r)return;const files=await r.json();
  if(!files.length){tb.innerHTML=`<tr><td colspan="5"><div style="padding:40px;text-align:center;color:var(--text-3);font-family:var(--font-mono);font-size:11px">Directory is empty</div></td></tr>`;return}
  tb.innerHTML=files.map(f=>{
    const ext=f.name.split('.').pop().toLowerCase(),c=EXT_COLORS[ext]||'#6B6B7A',ic=EXT_ICONS[ext]||'□',jn=JSON.stringify(f.name);
    return `<tr>
      <td><div class="file-name-link" onclick='editFile(${jn})'><span class="file-icon">${ic}</span><span class="file-name-text" title="${escH(f.name)}">${escH(f.name)}</span></div></td>
      <td><span class="file-type-chip" style="color:${c};border-color:${c}30">.${ext}</span></td>
      <td style="color:var(--text-3)">${escH(f.size)}</td>
      <td style="color:var(--text-3);font-size:11px">${escH(f.modified)}</td>
      <td><div class="btn-row" style="flex-wrap:nowrap;gap:4px">
        <button class="btn btn-ghost btn-sm" onclick='editFile(${jn})' title="Edit" style="padding:5px 8px">✏</button>
        <button class="btn btn-ghost btn-sm" onclick='dlFile(${jn})' title="Download" style="padding:5px 8px">↓</button>
        <button class="btn btn-rose btn-sm" onclick='delFile(${jn})' title="Delete" style="padding:5px 8px">✕</button>
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
    wrap.innerHTML=`<span style="color:var(--amber);flex-shrink:0">⬆</span><span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:11px" title="${escH(relPath)}">${escH(relPath)}</span><div class="upload-bar-wrap"><div class="upload-bar-fill" id="${sid}" style="width:0%"></div></div><span id="${sid}st" style="font-size:10.5px;color:var(--text-3);flex-shrink:0;min-width:28px;text-align:right">0%</span>`;
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
      if(b){b.style.width='100%';b.style.background='var(--mint)'}if(s){s.textContent='✓';s.style.color='var(--mint)'}
      ok++;setTimeout(()=>wrap.remove(),2500);
    }catch(err){
      const b=document.getElementById(sid),s=document.getElementById(sid+'st');
      if(b){b.style.width='100%';b.style.background='var(--rose)'}if(s){s.textContent='✕';s.style.color='var(--rose)'}
      wrap.style.borderColor='rgba(255,77,109,0.3)';fail++;
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
  d.innerHTML=`<input class="env-field env-key" placeholder="KEY" value="${escH(k)}"><input class="env-field" placeholder="value" value="${escH(v)}"><button class="btn btn-rose btn-sm" onclick="this.parentElement.remove()" style="padding:6px 10px">✕</button>`;
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
  if(!b.is_shared){const r=await apiFetch(`/api/bot/${curBot}/subusers`);if(r){const users=await r.json();const c=document.getElementById('subuserList');c.innerHTML='';users.forEach(u=>{const div=document.createElement('div');div.className='subuser-row';div.innerHTML=`<span style="color:var(--text-2)">${escH(u)}</span><button class="btn btn-rose btn-sm" onclick="removeSubuser('${escH(u)}')">Revoke</button>`;c.appendChild(div)})}}
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