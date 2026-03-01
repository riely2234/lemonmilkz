"""
ZentroHost v4.0 — Industrial Luxury Edition
Install: pip install flask flask-socketio psutil werkzeug
Run:     python zentrohost.py
"""
import logging, os, subprocess, sys, threading, zipfile, json, time, shutil, psutil
from logging import Formatter, StreamHandler, getLogger
from flask import Flask, render_template_string, request, jsonify, send_file
from flask_socketio import SocketIO
from werkzeug.utils import secure_filename

log = getLogger('zentrohost')
log.setLevel(logging.INFO)
_h = StreamHandler()
_h.setFormatter(Formatter('%(asctime)s %(levelname)s %(message)s'))
log.addHandler(_h)

app = Flask(__name__)
app.secret_key = os.urandom(24)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

bots = {}
BOTS_DIR = os.path.join(os.getcwd(), "zentro_bots")
CONFIG_FILE = os.path.join(os.getcwd(), "zentro_config.json")
os.makedirs(BOTS_DIR, exist_ok=True)

def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE) as f: return json.load(f)
    return {}

def save_config(cfg):
    with open(CONFIG_FILE, 'w') as f: json.dump(cfg, f, indent=2)

def get_bot_dir(bot_id):
    p = os.path.join(BOTS_DIR, bot_id); os.makedirs(p, exist_ok=True); return p

def emit_log(bot_id, msg, level="default"):
    socketio.emit('console_log', {'bot_id': bot_id, 'msg': msg, 'level': level})
    entry = {'msg': msg, 'level': level, 'time': time.strftime('%H:%M:%S')}
    bots.setdefault(bot_id, {}).setdefault('logs', []).append(entry)
    if len(bots[bot_id]['logs']) > 500: bots[bot_id]['logs'] = bots[bot_id]['logs'][-500:]

def is_running(bot_id):
    return bot_id in bots and bots[bot_id].get('process') is not None and bots[bot_id]['process'].poll() is None

def start_bot(bot_id, startup_file=None):
    cfg = load_config(); bot_cfg = cfg.get(bot_id, {}); bot_dir = get_bot_dir(bot_id)
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
    else: emit_log(bot_id, '[Error] Only .py / .js supported.', 'error'); return
    env = os.environ.copy(); env.update(bot_cfg.get('env', {}))
    emit_log(bot_id, f'[System] Starting {startup_file}...', 'system')
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.PIPE, text=True, cwd=bot_dir, env=env)
        bots.setdefault(bot_id, {}).update({'process': proc, 'startup_file': startup_file, 'start_time': time.time()})
        bot_cfg['startup_file'] = startup_file; cfg[bot_id] = bot_cfg; save_config(cfg)
        socketio.emit('status_update', {'bot_id': bot_id, 'status': 'online'})
        def _read():
            for line in iter(proc.stdout.readline, ''): emit_log(bot_id, line.rstrip(), 'default')
            proc.wait()
            socketio.emit('status_update', {'bot_id': bot_id, 'status': 'offline'})
            emit_log(bot_id, f'[System] Exited code {proc.returncode}.', 'system')
            if bots.get(bot_id, {}).get('auto_restart') and proc.returncode != 0:
                emit_log(bot_id, '[System] Auto-restart in 3s...', 'system'); time.sleep(3); start_bot(bot_id, startup_file)
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
            socketio.emit('status_update', {'bot_id': bot_id, 'status': 'offline'})

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ZENTROHOST</title>
<script src="https://cdn.socket.io/4.0.1/socket.io.min.js"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=DM+Mono:ital,wght@0,300;0,400;0,500;1,300&family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;0,9..40,600;1,9..40,300&display=swap" rel="stylesheet">

<style>
/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   RESET & ROOT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

:root {
  --black:    #0e0d0b;
  --charcoal: #1a1916;
  --surface:  #211f1c;
  --surface2: #2a2724;
  --surface3: #332f2b;
  --line:     #3a3630;
  --line2:    #4a453f;
  --gold:     #c9a84c;
  --gold2:    #e8c878;
  --gold-dim: #7a6530;
  --amber:    #f0a030;
  --ember:    #e05c20;
  --green:    #6abf69;
  --red:      #e05252;
  --blue:     #6a9fcf;
  --cream:    #f0ead8;
  --text:     #d4cfc4;
  --text2:    #8a837a;
  --text3:    #5a5550;
  --mono:     'DM Mono', monospace;
  --sans:     'DM Sans', sans-serif;
  --display:  'Bebas Neue', sans-serif;
}

@media (max-width: 768px) {
  body { flex-direction: column; overflow-y: auto; height: auto; }
  .sidebar {
    width: 100%; min-width: 100%; height: auto;
    border-right: none; border-bottom: 1px solid var(--line);
    position: sticky; top: 0; z-index: 100;
  }
  .sidebar-content { display: none; }
  .sidebar.open .sidebar-content { display: flex; flex-direction: column; }
  .main { height: auto; overflow: visible; }
  .topbar { padding: 10px 15px; font-size: 14px; flex-wrap: wrap; height: auto; }
  .stats-row { grid-template-columns: 1fr 1fr; gap: 8px; }
  .stat-value { font-size: 20px; }
  .page { padding: 15px; }
  .tb-bot { max-width: 100%; width: 100%; margin-top: 5px; }
  .tb-controls { width: 100%; justify-content: space-between; margin-top: 10px; flex-wrap: wrap; }
  .mobile-menu-btn {
    display: block !important;
    background: none; border: 1px solid var(--line2);
    color: var(--gold); padding: 5px 10px; border-radius: 4px;
    font-family: var(--mono); font-size: 10px;
  }
}
.mobile-menu-btn { display: none; }

html, body { height: 100%; overflow: hidden; background: var(--black); }
body { display: flex; font-family: var(--sans); color: var(--text); cursor: default; }

/* Custom cursor */
* { cursor: default; }
button, .clickable, [onclick] { cursor: pointer; }

/* Noise texture overlay */
body::before {
  content: '';
  position: fixed; inset: 0; z-index: 9999; pointer-events: none;
  background-image: url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='noise'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23noise)' opacity='0.04'/%3E%3C/svg%3E");
  opacity: 0.35;
  mix-blend-mode: overlay;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   SCROLLBAR
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
::-webkit-scrollbar { width: 4px; height: 4px; }
::-webkit-scrollbar-track { background: var(--charcoal); }
::-webkit-scrollbar-thumb { background: var(--line2); border-radius: 2px; }
::-webkit-scrollbar-thumb:hover { background: var(--gold-dim); }

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   SIDEBAR
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
.sidebar {
  width: 248px;
  min-width: 248px;
  height: 100vh;
  background: var(--charcoal);
  border-right: 1px solid var(--line);
  display: flex;
  flex-direction: column;
  overflow: hidden;
  flex-shrink: 0;
  position: relative;
}

/* Gold accent line at top */
.sidebar::before {
  content: '';
  position: absolute;
  top: 0; left: 0; right: 0; height: 2px;
  background: linear-gradient(90deg, var(--gold-dim), var(--gold), var(--gold-dim));
}

/* LOGO */
.logo {
  padding: 22px 20px 18px;
  border-bottom: 1px solid var(--line);
}

.logo-lockup {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 2px;
}

.logo-badge {
  width: 34px; height: 34px;
  background: var(--gold);
  display: flex; align-items: center; justify-content: center;
  clip-path: polygon(50% 0%, 100% 25%, 100% 75%, 50% 100%, 0% 75%, 0% 25%);
  flex-shrink: 0;
}

.logo-badge span { font-family: var(--display); font-size: 15px; color: #000; line-height: 1; }

.logo-wordmark {
  font-family: var(--display);
  font-size: 24px;
  letter-spacing: 2px;
  color: var(--cream);
  line-height: 1;
}

.logo-tagline {
  font-family: var(--mono);
  font-size: 9px;
  color: var(--text3);
  letter-spacing: 3px;
  text-transform: uppercase;
  margin-left: 44px;
}

/* NAV */
.nav { padding: 14px 10px; flex-shrink: 0; }

.nav-label {
  font-family: var(--mono);
  font-size: 9px;
  letter-spacing: 3px;
  color: var(--text3);
  text-transform: uppercase;
  padding: 8px 12px 5px;
  display: flex;
  align-items: center;
  gap: 8px;
}

.nav-label::after {
  content: '';
  flex: 1;
  height: 1px;
  background: var(--line);
}

.nav-item {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 9px 12px;
  border-radius: 4px;
  font-size: 13px;
  font-weight: 500;
  color: var(--text2);
  cursor: pointer;
  transition: all 0.15s;
  margin-bottom: 1px;
  position: relative;
  border-left: 2px solid transparent;
  letter-spacing: 0.2px;
}

.nav-item:hover { background: var(--surface); color: var(--text); }

.nav-item.active {
  background: var(--surface);
  color: var(--gold);
  border-left-color: var(--gold);
  font-weight: 600;
}

.nav-glyph {
  width: 18px;
  font-family: var(--mono);
  font-size: 12px;
  text-align: center;
  opacity: 0.7;
}

.nav-item.active .nav-glyph { opacity: 1; color: var(--gold); }

/* BOT LIST */
.bot-section-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 12px 20px 8px;
  flex-shrink: 0;
}

.bot-section-label {
  font-family: var(--mono);
  font-size: 9px;
  letter-spacing: 3px;
  color: var(--text3);
  text-transform: uppercase;
}

.bot-count-badge {
  background: var(--surface2);
  border: 1px solid var(--line2);
  border-radius: 3px;
  padding: 1px 6px;
  font-family: var(--mono);
  font-size: 10px;
  color: var(--gold);
  font-weight: 500;
}

.new-bot-btn {
  margin: 0 10px 8px;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 7px;
  padding: 8px 12px;
  border: 1px dashed var(--line2);
  border-radius: 4px;
  font-size: 12px;
  font-weight: 600;
  color: var(--text3);
  cursor: pointer;
  transition: all 0.15s;
  letter-spacing: 0.3px;
}

.new-bot-btn:hover {
  border-color: var(--gold-dim);
  color: var(--gold);
  background: rgba(201, 168, 76, 0.04);
}

.bot-list { flex: 1; overflow-y: auto; padding: 0 10px 8px; }

.bot-item {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 9px 12px;
  border-radius: 4px;
  cursor: pointer;
  transition: all 0.15s;
  margin-bottom: 1px;
  border-left: 2px solid transparent;
}

.bot-item:hover { background: var(--surface); }

.bot-item.active {
  background: var(--surface);
  border-left-color: var(--amber);
}

.bot-indicator {
  position: relative;
  width: 7px; height: 7px;
  border-radius: 50%;
  flex-shrink: 0;
}

.bot-indicator.online {
  background: var(--green);
  box-shadow: 0 0 0 0 rgba(106, 191, 105, 0.4);
  animation: sonar 2.5s ease-out infinite;
}

.bot-indicator.offline { background: var(--text3); }

@keyframes sonar {
  0%   { box-shadow: 0 0 0 0 rgba(106,191,105,0.4); }
  70%  { box-shadow: 0 0 0 8px rgba(106,191,105,0); }
  100% { box-shadow: 0 0 0 0 rgba(106,191,105,0); }
}

.bot-info { flex: 1; min-width: 0; }

.bot-name {
  font-size: 12px;
  font-weight: 600;
  color: var(--text2);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  transition: color 0.15s;
}

.bot-item.active .bot-name,
.bot-item:hover .bot-name { color: var(--text); }

.bot-status-text {
  font-family: var(--mono);
  font-size: 9px;
  color: var(--text3);
  letter-spacing: 0.5px;
  margin-top: 1px;
}

.bot-item.active .bot-status-text { color: var(--amber); }

/* SIDEBAR FOOTER */
.sidebar-footer {
  padding: 12px 20px;
  border-top: 1px solid var(--line);
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-shrink: 0;
}

.footer-brand {
  font-family: var(--display);
  font-size: 11px;
  letter-spacing: 3px;
  color: var(--text3);
}

.footer-clock {
  font-family: var(--mono);
  font-size: 11px;
  color: var(--gold);
  letter-spacing: 1px;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   MAIN PANEL
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
.main { flex: 1; display: flex; flex-direction: column; height: 100vh; overflow: hidden; min-width: 0; }

/* TOPBAR */
.topbar {
  height: 54px;
  background: var(--charcoal);
  border-bottom: 1px solid var(--line);
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 24px;
  flex-shrink: 0;
  gap: 16px;
}

.tb-breadcrumb {
  display: flex;
  align-items: center;
  gap: 8px;
  min-width: 0;
}

.tb-section {
  font-family: var(--mono);
  font-size: 10px;
  letter-spacing: 2px;
  text-transform: uppercase;
  color: var(--text3);
}

.tb-slash { color: var(--line2); font-weight: 300; }

.tb-page {
  font-family: var(--display);
  font-size: 18px;
  letter-spacing: 2px;
  color: var(--cream);
  line-height: 1;
}

.tb-bot {
  font-family: var(--mono);
  font-size: 11px;
  color: var(--gold);
  letter-spacing: 0.5px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  max-width: 180px;
}

.tb-controls {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-shrink: 0;
}

/* STATUS INDICATOR */
.status-tag {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 5px 12px;
  border-radius: 2px;
  font-family: var(--mono);
  font-size: 10px;
  font-weight: 500;
  letter-spacing: 2px;
  text-transform: uppercase;
  transition: all 0.3s;
}

.status-tag.online {
  background: rgba(106, 191, 105, 0.08);
  border: 1px solid rgba(106, 191, 105, 0.25);
  color: var(--green);
}

.status-tag.offline {
  background: rgba(224, 82, 82, 0.06);
  border: 1px solid rgba(224, 82, 82, 0.2);
  color: var(--red);
}

.status-led {
  width: 5px; height: 5px;
  border-radius: 50%;
}

.status-tag.online .status-led {
  background: var(--green);
  animation: ledBlink 1.5s ease-in-out infinite;
}

.status-tag.offline .status-led { background: var(--red); }

@keyframes ledBlink {
  0%,100% { opacity: 1; box-shadow: 0 0 4px var(--green); }
  50%      { opacity: 0.4; box-shadow: none; }
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   BUTTONS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
.btn {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  padding: 7px 14px;
  border-radius: 3px;
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.8px;
  text-transform: uppercase;
  cursor: pointer;
  border: 1px solid transparent;
  font-family: var(--sans);
  transition: all 0.12s;
  transform-origin: center;
  user-select: none;
}

.btn:active { transform: scale(0.97) translateY(1px); }

.btn-gold {
  background: var(--gold);
  color: #0e0d0b;
  border-color: var(--gold);
}
.btn-gold:hover { background: var(--gold2); border-color: var(--gold2); }

.btn-green {
  background: rgba(106,191,105,0.1);
  color: var(--green);
  border-color: rgba(106,191,105,0.3);
}
.btn-green:hover { background: rgba(106,191,105,0.18); }

.btn-red {
  background: rgba(224,82,82,0.1);
  color: var(--red);
  border-color: rgba(224,82,82,0.25);
}
.btn-red:hover { background: rgba(224,82,82,0.18); }

.btn-amber {
  background: rgba(240,160,48,0.1);
  color: var(--amber);
  border-color: rgba(240,160,48,0.25);
}
.btn-amber:hover { background: rgba(240,160,48,0.18); }

.btn-ghost {
  background: var(--surface);
  color: var(--text2);
  border-color: var(--line);
}
.btn-ghost:hover { background: var(--surface2); color: var(--text); border-color: var(--line2); }

.btn-sm { padding: 5px 10px; font-size: 10px; }
.btn-row { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   PAGES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
.page { flex: 1; overflow-y: auto; padding: 22px 24px; display: none; }
.page.active { display: block; animation: fadeSlide 0.2s ease; }
.page::-webkit-scrollbar { width: 3px; }
.page::-webkit-scrollbar-thumb { background: var(--line2); }

@keyframes fadeSlide {
  from { opacity: 0; transform: translateY(6px); }
  to   { opacity: 1; transform: translateY(0); }
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   STATS ROW
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
.stats-row {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 12px;
  margin-bottom: 18px;
}

.stat-block {
  background: var(--surface);
  border: 1px solid var(--line);
  border-top: 2px solid var(--line2);
  border-radius: 3px;
  padding: 14px 16px 12px;
  transition: border-top-color 0.3s;
}

.stat-block:hover { border-top-color: var(--gold-dim); }
.stat-block.s-gold  { border-top-color: var(--gold); }
.stat-block.s-green { border-top-color: var(--green); }
.stat-block.s-amber { border-top-color: var(--amber); }
.stat-block.s-red   { border-top-color: var(--red); }

.stat-label {
  font-family: var(--mono);
  font-size: 8px;
  letter-spacing: 3px;
  text-transform: uppercase;
  color: var(--text3);
  margin-bottom: 8px;
}

.stat-value {
  font-family: var(--display);
  font-size: 28px;
  letter-spacing: 1px;
  line-height: 1;
  margin-bottom: 4px;
}

.sv-gold   { color: var(--gold); }
.sv-green  { color: var(--green); }
.sv-amber  { color: var(--amber); }
.sv-red    { color: var(--red); }
.sv-blue   { color: var(--blue); }

.stat-sub {
  font-family: var(--mono);
  font-size: 9px;
  color: var(--text3);
  letter-spacing: 0.5px;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   PANELS (Cards)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
.panel {
  background: var(--surface);
  border: 1px solid var(--line);
  border-radius: 3px;
  margin-bottom: 14px;
  overflow: hidden;
}

.panel-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 12px 18px;
  border-bottom: 1px solid var(--line);
  background: var(--charcoal);
}

.panel-title {
  display: flex;
  align-items: center;
  gap: 10px;
  font-family: var(--display);
  font-size: 16px;
  letter-spacing: 2px;
  color: var(--cream);
}

.panel-tag {
  font-family: var(--mono);
  font-size: 8px;
  letter-spacing: 2px;
  text-transform: uppercase;
  color: var(--text3);
  padding: 2px 7px;
  border: 1px solid var(--line2);
  border-radius: 2px;
}

.panel-body { padding: 18px; }

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   TERMINAL
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
.terminal {
  background: #0a0908;
  border: 1px solid var(--line);
  border-radius: 2px;
  padding: 14px 16px;
  overflow-y: auto;
  font-family: var(--mono);
  font-size: 11.5px;
  line-height: 1.85;
}

.terminal::-webkit-scrollbar { width: 3px; }
.terminal::-webkit-scrollbar-thumb { background: var(--line2); }

/* Terminal top bar */
.term-chrome {
  background: var(--charcoal);
  border: 1px solid var(--line);
  border-bottom: none;
  border-radius: 2px 2px 0 0;
  padding: 7px 14px;
  display: flex;
  align-items: center;
  gap: 8px;
}

.term-dot {
  width: 8px; height: 8px;
  border-radius: 50%;
}
.term-dot-red   { background: #e05252; }
.term-dot-amber { background: #f0a030; }
.term-dot-green { background: #6abf69; }

.term-title {
  flex: 1;
  text-align: center;
  font-family: var(--mono);
  font-size: 9px;
  letter-spacing: 2px;
  color: var(--text3);
  text-transform: uppercase;
}

.terminal.attached {
  border-radius: 0 0 2px 2px;
}

/* Log lines */
.log-row {
  display: flex;
  align-items: baseline;
  gap: 10px;
  line-height: 1.6;
  padding: 1px 0;
}

.log-ts {
  font-size: 9px;
  color: var(--text3);
  flex-shrink: 0;
  letter-spacing: 0.5px;
}

.log-tag {
  font-size: 8px;
  letter-spacing: 1.5px;
  text-transform: uppercase;
  padding: 1px 5px;
  border-radius: 1px;
  flex-shrink: 0;
  font-weight: 500;
}

.log-tag.sys  { background: rgba(106,159,207,0.15); color: var(--blue); }
.log-tag.err  { background: rgba(224,82,82,0.15);   color: var(--red); }
.log-tag.ok   { background: rgba(106,191,105,0.12); color: var(--green); }
.log-tag.warn { background: rgba(240,160,48,0.12);  color: var(--amber); }
.log-tag.out  { background: rgba(255,255,255,0.04); color: var(--text3); }

.log-msg {
  flex: 1;
  word-break: break-all;
  font-size: 11.5px;
}

.log-msg.sys  { color: #6a9fcf; }
.log-msg.err  { color: #e07070; }
.log-msg.ok   { color: #7abf7a; }
.log-msg.warn { color: #f0b050; }
.log-msg.out  { color: #9a9087; }

/* Term input */
.term-input-wrap {
  display: flex;
  align-items: center;
  gap: 10px;
  background: #0a0908;
  border: 1px solid var(--line);
  border-radius: 2px;
  padding: 8px 14px;
  margin-top: 8px;
  transition: border-color 0.15s;
}

.term-input-wrap:focus-within { border-color: var(--gold-dim); }

.term-prompt-sym {
  font-family: var(--mono);
  font-size: 13px;
  color: var(--gold);
  flex-shrink: 0;
  user-select: none;
}

.term-input {
  flex: 1;
  background: none;
  border: none;
  outline: none;
  font-family: var(--mono);
  font-size: 12px;
  color: var(--cream);
  caret-color: var(--gold);
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   FILE TABLE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
.file-table { width: 100%; border-collapse: collapse; }

.file-table thead th {
  font-family: var(--mono);
  font-size: 8px;
  letter-spacing: 2.5px;
  text-transform: uppercase;
  color: var(--text3);
  padding: 9px 16px;
  border-bottom: 1px solid var(--line2);
  text-align: left;
  font-weight: 500;
}

.file-table tbody td {
  padding: 9px 16px;
  font-size: 12px;
  border-bottom: 1px solid var(--line);
  vertical-align: middle;
}

.file-table tbody tr { transition: background 0.1s; }
.file-table tbody tr:hover td { background: var(--surface2); }
.file-table tbody tr:last-child td { border-bottom: none; }

.file-name-cell {
  display: flex;
  align-items: center;
  gap: 8px;
  cursor: pointer;
  color: var(--gold);
  font-weight: 500;
  font-family: var(--mono);
  font-size: 12px;
  transition: color 0.12s;
}

.file-name-cell:hover { color: var(--gold2); text-decoration: underline; }

.file-ext-badge {
  font-family: var(--mono);
  font-size: 8px;
  letter-spacing: 1px;
  text-transform: uppercase;
  padding: 1px 5px;
  border-radius: 1px;
  border: 1px solid var(--line2);
  color: var(--text3);
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   UPLOAD ZONE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
.drop-zone {
  border: 1px dashed var(--line2);
  border-radius: 3px;
  padding: 30px 24px;
  text-align: center;
  cursor: pointer;
  transition: all 0.2s;
  position: relative;
  background: var(--charcoal);
}

.drop-zone:hover, .drop-zone.dragging {
  border-color: var(--gold-dim);
  background: rgba(201, 168, 76, 0.03);
}

.drop-zone input {
  position: absolute; inset: 0;
  opacity: 0; cursor: pointer;
  width: 100%; height: 100%;
}

.drop-icon {
  font-size: 28px;
  margin-bottom: 10px;
  display: block;
  filter: grayscale(0.3);
}

.drop-headline {
  font-family: var(--display);
  font-size: 18px;
  letter-spacing: 2px;
  color: var(--text);
  margin-bottom: 4px;
}

.drop-sub {
  font-family: var(--mono);
  font-size: 10px;
  letter-spacing: 1.5px;
  color: var(--text3);
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   FORMS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
.form-group { margin-bottom: 14px; }

.form-label {
  display: flex;
  align-items: center;
  gap: 8px;
  font-family: var(--mono);
  font-size: 8px;
  letter-spacing: 2.5px;
  text-transform: uppercase;
  color: var(--text3);
  margin-bottom: 6px;
}

.form-label::after {
  content: '';
  flex: 1;
  height: 1px;
  background: var(--line);
}

.form-input, .form-select, .form-textarea {
  width: 100%;
  background: var(--charcoal);
  border: 1px solid var(--line);
  border-bottom: 2px solid var(--line2);
  border-radius: 2px;
  padding: 9px 12px;
  font-size: 13px;
  color: var(--text);
  font-family: var(--sans);
  outline: none;
  transition: all 0.15s;
}

.form-input:focus, .form-select:focus, .form-textarea:focus {
  border-color: var(--line2);
  border-bottom-color: var(--gold);
  background: var(--surface);
}

.form-select option { background: #1a1916; }

.form-textarea {
  resize: vertical;
  font-family: var(--mono);
  font-size: 12px;
  min-height: 80px;
  line-height: 1.6;
}

.form-row-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   ENV ROWS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
.env-row {
  display: grid;
  grid-template-columns: 1fr 1.5fr auto;
  gap: 8px;
  margin-bottom: 8px;
  align-items: center;
}

.env-field {
  background: var(--charcoal);
  border: 1px solid var(--line);
  border-bottom: 2px solid var(--line2);
  border-radius: 2px;
  padding: 7px 10px;
  font-family: var(--mono);
  font-size: 12px;
  color: var(--text);
  outline: none;
  width: 100%;
  transition: border-bottom-color 0.15s;
}

.env-field:focus { border-bottom-color: var(--gold); }
.env-field.key-field { color: var(--amber); }

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   RESOURCE BARS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
.res-item { margin-bottom: 22px; }

.res-header {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  margin-bottom: 6px;
}

.res-name {
  font-family: var(--mono);
  font-size: 9px;
  letter-spacing: 2px;
  text-transform: uppercase;
  color: var(--text2);
}

.res-value {
  font-family: var(--display);
  font-size: 16px;
  letter-spacing: 1px;
}

.res-track {
  height: 3px;
  background: var(--surface2);
  border-radius: 1px;
  overflow: visible;
  position: relative;
}

.res-fill {
  height: 100%;
  border-radius: 1px;
  transition: width 0.7s cubic-bezier(0.4,0,0.2,1);
  position: relative;
}

.res-fill::after {
  content: '';
  position: absolute;
  right: -1px; top: -2px;
  width: 7px; height: 7px;
  border-radius: 50%;
  background: inherit;
  box-shadow: 0 0 6px currentColor;
}

.res-fill.gold   { background: var(--gold);  color: var(--gold); }
.res-fill.amber  { background: var(--amber); color: var(--amber); }
.res-fill.red    { background: var(--red);   color: var(--red); }
.res-fill.green  { background: var(--green); color: var(--green); }

/* Sub info */
.res-sub {
  font-family: var(--mono);
  font-size: 9px;
  color: var(--text3);
  margin-top: 5px;
  letter-spacing: 0.5px;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   MODAL
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
.modal-veil {
  display: none;
  position: fixed; inset: 0;
  background: rgba(10,9,8,0.82);
  z-index: 1000;
  align-items: center;
  justify-content: center;
  backdrop-filter: blur(3px);
}

.modal-veil.open { display: flex; }

.modal-box {
  background: var(--charcoal);
  border: 1px solid var(--line2);
  border-top: 2px solid var(--gold);
  border-radius: 3px;
  padding: 26px;
  width: 100%;
  max-width: 460px;
  max-height: 90vh;
  overflow-y: auto;
  animation: modalSlide 0.18s cubic-bezier(0.34,1.56,0.64,1);
}

.modal-box.wide { max-width: 700px; }

@keyframes modalSlide {
  from { transform: scale(0.92) translateY(12px); opacity: 0; }
  to   { transform: scale(1) translateY(0); opacity: 1; }
}

.modal-heading {
  font-family: var(--display);
  font-size: 22px;
  letter-spacing: 2px;
  color: var(--cream);
  margin-bottom: 20px;
  display: flex;
  align-items: center;
  gap: 10px;
}

.modal-heading-accent { color: var(--gold); }

.modal-footer {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
  margin-top: 20px;
  padding-top: 16px;
  border-top: 1px solid var(--line);
}

/* Code editor */
.code-editor {
  width: 100%;
  min-height: 400px;
  background: #0a0908;
  border: 1px solid var(--line);
  border-left: 2px solid var(--gold-dim);
  border-radius: 2px;
  padding: 14px 16px;
  font-family: var(--mono);
  font-size: 12px;
  color: var(--cream);
  resize: vertical;
  outline: none;
  line-height: 1.75;
  caret-color: var(--gold);
  transition: border-left-color 0.15s;
}

.code-editor:focus { border-left-color: var(--gold); }

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   DIVIDERS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
.divider {
  border: none;
  border-top: 1px solid var(--line);
  margin: 16px 0;
}

.divider-label {
  display: flex;
  align-items: center;
  gap: 10px;
  font-family: var(--mono);
  font-size: 8px;
  letter-spacing: 2px;
  text-transform: uppercase;
  color: var(--text3);
  margin: 16px 0 14px;
}

.divider-label::before, .divider-label::after {
  content: '';
  flex: 1;
  height: 1px;
  background: var(--line);
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   DANGER ZONE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
.danger-block {
  border: 1px solid rgba(224,82,82,0.2);
  border-left: 2px solid var(--red);
  background: rgba(224,82,82,0.03);
  border-radius: 3px;
  padding: 16px 18px;
  margin-top: 14px;
}

.danger-label {
  font-family: var(--mono);
  font-size: 8px;
  letter-spacing: 2px;
  text-transform: uppercase;
  color: rgba(224,82,82,0.6);
  margin-bottom: 8px;
}

.danger-desc {
  font-size: 12px;
  color: var(--text2);
  line-height: 1.5;
  margin-bottom: 12px;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   TOASTS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
.toast-tray {
  position: fixed;
  bottom: 22px; right: 22px;
  z-index: 9998;
  display: flex;
  flex-direction: column;
  gap: 8px;
  pointer-events: none;
}

.toast {
  background: var(--charcoal);
  border: 1px solid var(--line2);
  border-left: 3px solid var(--gold);
  border-radius: 2px;
  padding: 10px 16px;
  font-size: 12px;
  font-weight: 500;
  color: var(--text);
  animation: toastPop 0.2s ease;
  display: flex;
  align-items: center;
  gap: 10px;
  min-width: 220px;
  pointer-events: all;
  font-family: var(--sans);
}

.toast.success { border-left-color: var(--green); }
.toast.error   { border-left-color: var(--red); }
.toast.info    { border-left-color: var(--blue); }

.toast-icon { font-size: 14px; flex-shrink: 0; }

@keyframes toastPop {
  from { transform: translateX(16px) scale(0.95); opacity: 0; }
  to   { transform: translateX(0) scale(1); opacity: 1; }
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   EMPTY STATES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
.empty {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 48px 24px;
  text-align: center;
  color: var(--text3);
}

.empty-glyph {
  font-family: var(--display);
  font-size: 48px;
  letter-spacing: 4px;
  color: var(--line2);
  margin-bottom: 12px;
  line-height: 1;
}

.empty-text {
  font-size: 12px;
  color: var(--text3);
  line-height: 1.6;
  font-family: var(--mono);
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   UPLOAD PROGRESS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
.upload-item {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 8px 12px;
  background: var(--charcoal);
  border: 1px solid var(--line);
  border-radius: 2px;
  margin-top: 8px;
  font-family: var(--mono);
  font-size: 11px;
  color: var(--text2);
}

.upload-bar-wrap {
  flex: 1;
  height: 2px;
  background: var(--line);
  border-radius: 1px;
  overflow: hidden;
}

.upload-bar-fill {
  height: 100%;
  background: var(--gold);
  border-radius: 1px;
  transition: width 0.3s ease;
}
</style>
</head>
<body>

<!-- ═══════════════════════════════════
     SIDEBAR
═══════════════════════════════════ -->
<aside class="sidebar">

  <div class="logo">
    <div class="logo-lockup">
      <div class="logo-badge"><span>Z</span></div>
      <span class="logo-wordmark">ZENTROHOST</span>
      <button class="mobile-menu-btn" onclick="document.querySelector('.sidebar').classList.toggle('open')">MENU</button>
    </div>
    <div class="logo-tagline">BOT HOSTING PANEL</div>
  </div>

  <div class="sidebar-content">
    <nav class="nav">
    <div class="nav-label">Interface</div>
    <div class="nav-item active" data-page="dashboard" onclick="navTo('dashboard',this)">
      <span class="nav-glyph">◈</span> Dashboard
    </div>
    <div class="nav-item" data-page="console" onclick="navTo('console',this)">
      <span class="nav-glyph">$</span> Console
    </div>
    <div class="nav-item" data-page="files" onclick="navTo('files',this)">
      <span class="nav-glyph">≡</span> File Manager
    </div>
    <div class="nav-label" style="margin-top:8px">Configuration</div>
    <div class="nav-item" data-page="env" onclick="navTo('env',this)">
      <span class="nav-glyph">⊛</span> Environment
    </div>
    <div class="nav-item" data-page="settings" onclick="navTo('settings',this)">
      <span class="nav-glyph">⚙</span> Settings
    </div>
    <div class="nav-item" data-page="resources" onclick="navTo('resources',this)">
      <span class="nav-glyph">▣</span> Resources
    </div>
  </nav>

  <div class="bot-section-header">
    <span class="bot-section-label">Instances</span>
    <span class="bot-count-badge" id="botCount">0</span>
  </div>

  <div class="new-bot-btn" onclick="openCreateModal()">
    <span style="font-size:14px;font-weight:300">+</span>
    <span style="font-family:var(--mono);font-size:10px;letter-spacing:1.5px;text-transform:uppercase">New Instance</span>
  </div>

  <div class="bot-list" id="botList"></div>

  <div class="sidebar-footer">
    <span class="footer-brand">ZHOST</span>
    <span class="footer-clock" id="clock">00:00:00</span>
  </div>
  </div>
</aside>

<!-- ═══════════════════════════════════
     MAIN
═══════════════════════════════════ -->
<main class="main">

  <!-- TOPBAR -->
  <div class="topbar">
    <div class="tb-breadcrumb">
      <span class="tb-section">ZENTROHOST</span>
      <span class="tb-slash">/</span>
      <span class="tb-page" id="tbPage">DASHBOARD</span>
    </div>
    <div class="tb-controls">
      <div class="status-tag offline" id="statusTag">
        <div class="status-led"></div>
        <span id="statusText">OFFLINE</span>
      </div>
      <div class="tb-bot" id="tbBot">— select instance —</div>
      <div class="btn-row">
        <button class="btn btn-green btn-sm" onclick="startBot()">▶ START</button>
        <button class="btn btn-red btn-sm" onclick="stopBot()">■ STOP</button>
        <button class="btn btn-amber btn-sm" onclick="restartBot()">↺ RESTART</button>
      </div>
    </div>
  </div>

  <!-- PAGE: DASHBOARD -->
  <div class="page active" id="page-dashboard">
    <div class="stats-row">
      <div class="stat-block s-gold">
        <div class="stat-label">Process Status</div>
        <div class="stat-value sv-red" id="sStat">OFFLINE</div>
        <div class="stat-sub" id="sStatSub">no active process</div>
      </div>
      <div class="stat-block s-amber">
        <div class="stat-label">Uptime</div>
        <div class="stat-value sv-amber" id="sUptime">—</div>
        <div class="stat-sub">hh:mm:ss</div>
      </div>
      <div class="stat-block">
        <div class="stat-label">System CPU</div>
        <div class="stat-value sv-blue" id="sCpu">—</div>
        <div class="stat-sub">current load</div>
      </div>
      <div class="stat-block">
        <div class="stat-label">Memory Used</div>
        <div class="stat-value sv-gold" id="sMem">—</div>
        <div class="stat-sub">system memory</div>
      </div>
    </div>

    <div class="panel">
      <div class="panel-head">
        <div class="panel-title">LAUNCH CONTROL</div>
        <span class="panel-tag">Quick Actions</span>
      </div>
      <div class="panel-body">
        <div class="form-row-2" style="margin-bottom:16px">
          <div class="form-group" style="margin:0">
            <label class="form-label">Startup File</label>
            <input class="form-input" id="sfInput" value="main.py" placeholder="main.py or index.js">
          </div>
          <div class="form-group" style="margin:0">
            <label class="form-label">Runtime</label>
            <select class="form-select" id="rtSelect">
              <option value="auto">Auto Detect</option>
              <option value="python">Python 3</option>
              <option value="node">Node.js</option>
            </select>
          </div>
        </div>
        <div class="btn-row">
          <button class="btn btn-green" onclick="startBot()">▶ Start Process</button>
          <button class="btn btn-red" onclick="stopBot()">■ Stop</button>
          <button class="btn btn-amber" onclick="restartBot()">↺ Restart</button>
          <button class="btn btn-ghost" onclick="killBot()" style="margin-left:auto">☠ Force Kill</button>
        </div>
      </div>
    </div>

    <div class="panel">
      <div class="panel-head">
        <div class="panel-title">LIVE OUTPUT</div>
        <button class="btn btn-ghost btn-sm" onclick="navTo('console',null)">Full Console →</button>
      </div>
      <div class="panel-body" style="padding:14px">
        <div class="term-chrome">
          <div class="term-dot term-dot-red"></div>
          <div class="term-dot term-dot-amber"></div>
          <div class="term-dot term-dot-green"></div>
          <div class="term-title">stdout — live feed</div>
        </div>
        <div class="terminal attached" id="miniTerm" style="height:190px"></div>
      </div>
    </div>
  </div>

  <!-- PAGE: CONSOLE -->
  <div class="page" id="page-console">
    <div class="panel">
      <div class="panel-head">
        <div class="panel-title">PROCESS CONSOLE</div>
        <div class="btn-row">
          <button class="btn btn-ghost btn-sm" onclick="clearConsole()">⊘ Clear</button>
          <button class="btn btn-ghost btn-sm" onclick="exportLogs()">↓ Export</button>
        </div>
      </div>
      <div style="padding:14px 18px 0">
        <div class="term-chrome">
          <div class="term-dot term-dot-red"></div>
          <div class="term-dot term-dot-amber"></div>
          <div class="term-dot term-dot-green"></div>
          <div class="term-title" id="termTitle">no instance selected</div>
        </div>
      </div>
      <div style="padding:0 18px 18px">
        <div class="terminal attached" id="mainTerm" style="height:440px"></div>
        <div class="term-input-wrap">
          <span class="term-prompt-sym">❯</span>
          <input class="term-input" id="termIn" placeholder="Send to stdin..." onkeydown="if(event.key==='Enter')sendInput()">
          <button class="btn btn-ghost btn-sm" onclick="sendInput()">Send</button>
        </div>
      </div>
    </div>
  </div>

  <!-- PAGE: FILES -->
  <div class="page" id="page-files">
    <div class="panel">
      <div class="panel-head">
        <div class="panel-title">FILE SYSTEM</div>
        <div class="btn-row">
          <button class="btn btn-ghost btn-sm" onclick="openNewFileModal()">+ New File</button>
          <button class="btn btn-gold btn-sm" onclick="loadFiles()">↻ Refresh</button>
        </div>
      </div>
      <div style="overflow-x:auto">
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
    </div>

    <div class="panel">
      <div class="panel-head">
        <div class="panel-title">UPLOAD FILES</div>
        <span class="panel-tag">.py .js .json .zip +more</span>
      </div>
      <div class="panel-body">
        <div class="drop-zone" id="dropZone">
          <input type="file" multiple id="fileUploadInput" onchange="handleUpload(this.files)">
          <span class="drop-icon">⬆</span>
          <div class="drop-headline">DROP FILES HERE</div>
          <div class="drop-sub">OR CLICK TO BROWSE · ZIP ARCHIVES EXTRACTED AUTOMATICALLY</div>
        </div>
        <div id="uploadProgress"></div>
      </div>
    </div>
  </div>

  <!-- PAGE: ENV -->
  <div class="page" id="page-env">
    <div class="panel">
      <div class="panel-head">
        <div class="panel-title">ENVIRONMENT VARS</div>
        <button class="btn btn-gold btn-sm" onclick="saveEnv()">Save Variables</button>
      </div>
      <div class="panel-body">
        <p style="font-family:var(--mono);font-size:10px;color:var(--text3);letter-spacing:.5px;line-height:1.7;margin-bottom:16px">
          Variables are injected into the process environment at startup.<br>
          Ideal for API keys, tokens, and configuration secrets.
        </p>
        <div class="env-row" style="margin-bottom:6px;grid-template-columns:1fr 1.5fr auto">
          <span style="font-family:var(--mono);font-size:8px;letter-spacing:2px;color:var(--text3);text-transform:uppercase">KEY</span>
          <span style="font-family:var(--mono);font-size:8px;letter-spacing:2px;color:var(--text3);text-transform:uppercase">VALUE</span>
          <span></span>
        </div>
        <div id="envRows"></div>
        <button class="btn btn-ghost btn-sm" onclick="addEnvRow('','')" style="margin-top:4px">+ Add Row</button>
      </div>
    </div>
  </div>

  <!-- PAGE: SETTINGS -->
  <div class="page" id="page-settings">
    <div class="panel">
      <div class="panel-head">
        <div class="panel-title">INSTANCE CONFIG</div>
      </div>
      <div class="panel-body">
        <div class="form-group">
          <label class="form-label">Instance Name</label>
          <input class="form-input" id="stName" placeholder="My Bot">
        </div>
        <div class="form-group">
          <label class="form-label">Startup File</label>
          <input class="form-input" id="stStartup" placeholder="main.py">
        </div>
        <div class="form-group">
          <label class="form-label">Crash Recovery</label>
          <select class="form-select" id="stAR">
            <option value="false">Disabled — manual restart only</option>
            <option value="true">Auto Restart — on non-zero exit code</option>
          </select>
        </div>
        <button class="btn btn-gold" onclick="saveSettings()">Save Configuration</button>
      </div>
    </div>

    <div class="danger-block">
      <div class="danger-label">⚠ Danger Zone</div>
      <div class="danger-desc">Permanently deletes this instance and all associated files from the filesystem. This action is irreversible.</div>
      <button class="btn btn-red" onclick="deleteBot()">☠ Destroy Instance</button>
    </div>
  </div>

  <!-- PAGE: RESOURCES -->
  <div class="page" id="page-resources">
    <div class="panel">
      <div class="panel-head">
        <div class="panel-title">SYSTEM RESOURCES</div>
        <span class="panel-tag" style="color:var(--gold)">Live · 3s Poll</span>
      </div>
      <div class="panel-body">
        <div class="res-item">
          <div class="res-header">
            <span class="res-name">CPU Usage</span>
            <span class="res-value sv-gold" id="rCpu">—</span>
          </div>
          <div class="res-track"><div class="res-fill gold" id="pCpu" style="width:0%"></div></div>
          <div class="res-sub" id="rCpuSub">measuring...</div>
        </div>
        <div class="res-item">
          <div class="res-header">
            <span class="res-name">Memory</span>
            <span class="res-value sv-amber" id="rMem">—</span>
          </div>
          <div class="res-track"><div class="res-fill amber" id="pMem" style="width:0%"></div></div>
          <div class="res-sub" id="rMemSub">measuring...</div>
        </div>
        <div class="res-item">
          <div class="res-header">
            <span class="res-name">Disk</span>
            <span class="res-value sv-blue" id="rDsk">—</span>
          </div>
          <div class="res-track"><div class="res-fill green" id="pDsk" style="width:0%"></div></div>
          <div class="res-sub" id="rDskSub">measuring...</div>
        </div>
      </div>
    </div>
  </div>

</main>

<!-- TOAST TRAY -->
<div class="toast-tray" id="toastTray"></div>

<!-- MODAL: CREATE BOT -->
<div class="modal-veil" id="mCreate">
  <div class="modal-box">
    <div class="modal-heading">NEW <span class="modal-heading-accent">INSTANCE</span></div>
    <div class="form-group">
      <label class="form-label">Instance Name</label>
      <input class="form-input" id="mName" placeholder="My Awesome Bot">
    </div>
    <div class="form-group">
      <label class="form-label">Startup File</label>
      <input class="form-input" id="mFile" value="main.py" placeholder="main.py or index.js">
    </div>
    <div class="modal-footer">
      <button class="btn btn-ghost" onclick="closeModal('mCreate')">Cancel</button>
      <button class="btn btn-gold" onclick="createBot()">Create Instance</button>
    </div>
  </div>
</div>

<!-- MODAL: FILE EDITOR -->
<div class="modal-veil" id="mEditor">
  <div class="modal-box wide">
    <div class="modal-heading">EDIT <span class="modal-heading-accent" id="edName">FILE</span></div>
    <textarea class="code-editor" id="edContent"></textarea>
    <div class="modal-footer">
      <button class="btn btn-ghost" onclick="closeModal('mEditor')">Discard</button>
      <button class="btn btn-gold" onclick="saveFile()">Save File</button>
    </div>
  </div>
</div>

<!-- MODAL: NEW FILE -->
<div class="modal-veil" id="mNewFile">
  <div class="modal-box">
    <div class="modal-heading">NEW <span class="modal-heading-accent">FILE</span></div>
    <div class="form-group">
      <label class="form-label">Filename</label>
      <input class="form-input" id="nfName" placeholder="main.py">
    </div>
    <div class="form-group">
      <label class="form-label">Initial Content</label>
      <textarea class="form-textarea" id="nfContent" placeholder="# Start coding..." style="height:120px"></textarea>
    </div>
    <div class="modal-footer">
      <button class="btn btn-ghost" onclick="closeModal('mNewFile')">Cancel</button>
      <button class="btn btn-gold" onclick="createNewFile()">Create</button>
    </div>
  </div>
</div>

<!-- ═══════════════════════════════════
     JAVASCRIPT
═══════════════════════════════════ -->
<script>
const sock = io();
let curBot = null, bots = {}, uptimeIv = null, resIv = null, startTimes = {};

// ── Clock ──────────────────────────────────
setInterval(() => {
  document.getElementById('clock').textContent = new Date().toTimeString().slice(0,8);
}, 1000);

// ── Sockets ────────────────────────────────
sock.on('console_log', ({bot_id, msg, level}) => {
  if (bot_id !== curBot) return;
  appendLog(msg, level);
});

sock.on('status_update', ({bot_id, status}) => {
  if (bots[bot_id]) bots[bot_id].status = status;
  renderBotList();
  if (bot_id === curBot) applyStatus(status);
  if (status === 'online') startTimes[bot_id] = Date.now();
  else delete startTimes[bot_id];
});

// ── Navigation ─────────────────────────────
const PAGE_NAMES = {
  dashboard:'DASHBOARD', console:'CONSOLE', files:'FILE MANAGER',
  env:'ENVIRONMENT', settings:'SETTINGS', resources:'RESOURCES'
};

function navTo(name, el) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  const p = document.getElementById('page-' + name);
  if (p) p.classList.add('active');
  if (el) el.classList.add('active');
  else { const n = document.querySelector(`[data-page="${name}"]`); if(n) n.classList.add('active'); }
  document.getElementById('tbPage').textContent = PAGE_NAMES[name] || name.toUpperCase();
  if (name === 'files') loadFiles();
  if (name === 'env') loadEnv();
  if (name === 'settings') loadSettings();
  if (name === 'resources') startRes(); else stopRes();
}

// ── Bots ───────────────────────────────────
async function loadBots() {
  const r = await fetch('/api/bots');
  bots = await r.json();
  renderBotList();
  document.getElementById('botCount').textContent = Object.keys(bots).length;
}

function renderBotList() {
  const el = document.getElementById('botList');
  el.innerHTML = '';
  const entries = Object.entries(bots);
  if (!entries.length) {
    el.innerHTML = '<div class="empty" style="padding:20px"><div class="empty-text">No instances yet</div></div>';
    return;
  }
  entries.forEach(([id, b]) => {
    const d = document.createElement('div');
    d.className = 'bot-item' + (id === curBot ? ' active' : '');
    const status = b.status || 'offline';
    d.innerHTML = `
      <div class="bot-indicator ${status}"></div>
      <div class="bot-info">
        <div class="bot-name">${escH(b.name||id)}</div>
        <div class="bot-status-text">${status.toUpperCase()}</div>
      </div>
    `;
    d.onclick = () => selectBot(id);
    el.appendChild(d);
  });
}

function selectBot(id) {
  curBot = id;
  const b = bots[id];
  document.getElementById('tbBot').textContent = b?.name || id;
  document.getElementById('sfInput').value = b?.startup_file || 'main.py';
  document.getElementById('termTitle').textContent = (b?.name || id) + ' — stdout';
  document.getElementById('mainTerm').innerHTML = '';
  document.getElementById('miniTerm').innerHTML = '';
  applyStatus(b?.status || 'offline');
  renderBotList();
  loadBotLogs();
  startUptime();
}

async function loadBotLogs() {
  if (!curBot) return;
  const r = await fetch(`/api/bot/${curBot}/logs`);
  const logs = await r.json();
  ['mainTerm','miniTerm'].forEach(id => document.getElementById(id).innerHTML = '');
  logs.forEach(({msg,level,time:ts}) => appendLog(msg, level, ts));
}

function applyStatus(s) {
  const tag = document.getElementById('statusTag');
  const txt = document.getElementById('statusText');
  const sv = document.getElementById('sStat');
  const sub = document.getElementById('sStatSub');
  const online = s === 'online';
  tag.className = 'status-tag ' + (online ? 'online' : 'offline');
  txt.textContent = online ? 'ONLINE' : 'OFFLINE';
  sv.textContent = online ? 'ONLINE' : 'OFFLINE';
  sv.className = 'stat-value ' + (online ? 'sv-green' : 'sv-red');
  sub.textContent = online ? 'process running' : 'no active process';
  if (!online) document.getElementById('sUptime').textContent = '—';
}

function openCreateModal() {
  document.getElementById('mCreate').classList.add('open');
  setTimeout(() => document.getElementById('mName').focus(), 80);
}

async function createBot() {
  const name = document.getElementById('mName').value.trim();
  const file = document.getElementById('mFile').value.trim() || 'main.py';
  if (!name) { toast('Instance name is required', 'error'); return; }
  const r = await fetch('/api/bots', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name,startup_file:file})});
  const b = await r.json();
  bots[b.id] = b;
  closeModal('mCreate');
  document.getElementById('mName').value = '';
  renderBotList();
  document.getElementById('botCount').textContent = Object.keys(bots).length;
  selectBot(b.id);
  toast(`Instance "${name}" created`, 'success');
}

async function deleteBot() {
  if (!curBot || !confirm('Destroy instance and all files? This is permanent.')) return;
  await fetch(`/api/bot/${curBot}`, {method:'DELETE'});
  delete bots[curBot]; curBot = null;
  document.getElementById('tbBot').textContent = '— select instance —';
  renderBotList();
  document.getElementById('botCount').textContent = Object.keys(bots).length;
  toast('Instance destroyed', 'error');
}

// ── Process Controls ───────────────────────
async function startBot() {
  if (!curBot) { toast('Select an instance first', 'error'); return; }
  const sf = document.getElementById('sfInput').value.trim() || 'main.py';
  await fetch(`/api/bot/${curBot}/start`, {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({startup_file:sf})});
  toast('Starting process...', 'info');
}

async function stopBot() {
  if (!curBot) return;
  await fetch(`/api/bot/${curBot}/stop`, {method:'POST'});
  toast('Process stopped', 'success');
}

async function restartBot() {
  if (!curBot) return;
  await stopBot();
  setTimeout(startBot, 1400);
  toast('Restarting...', 'info');
}

async function killBot() {
  if (!curBot) return;
  await fetch(`/api/bot/${curBot}/kill`, {method:'POST'});
  toast('Force killed', 'error');
}

// ── Console ────────────────────────────────
function appendLog(msg, level, ts) {
  const tagMap = {system:'sys',error:'err',success:'ok',warn:'warn',default:'out'};
  const tag = tagMap[level] || 'out';
  const t = ts || new Date().toTimeString().slice(0,8);

  ['mainTerm','miniTerm'].forEach(id => {
    const el = document.getElementById(id);
    const row = document.createElement('div');
    row.className = 'log-row';
    row.innerHTML = `<span class="log-ts">${t}</span><span class="log-tag ${tag}">${tag.toUpperCase()}</span><span class="log-msg ${tag}">${escH(msg)}</span>`;
    el.appendChild(row);
    el.scrollTop = el.scrollHeight;
  });
}

function escH(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function clearConsole() {
  ['mainTerm','miniTerm'].forEach(id => document.getElementById(id).innerHTML='');
  toast('Console cleared', 'info');
}

function exportLogs() {
  const lines = Array.from(document.getElementById('mainTerm').querySelectorAll('.log-row'))
    .map(r => r.textContent.trim()).join('\n');
  const a = document.createElement('a');
  a.href = 'data:text/plain;charset=utf-8,' + encodeURIComponent(lines);
  a.download = `${curBot||'zentro'}-${Date.now()}.log`;
  a.click();
  toast('Log file exported', 'success');
}

async function sendInput() {
  if (!curBot) return;
  const v = document.getElementById('termIn').value;
  document.getElementById('termIn').value = '';
  await fetch(`/api/bot/${curBot}/input`, {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({input:v+'\n'})});
}

// ── Files ──────────────────────────────────
const EXT_COLORS = {py:'#3b8',js:'#fc0',json:'#88f',md:'#f80',txt:'#aaa',sh:'#0cf',zip:'#f64',env:'#fa0',ts:'#26f'};

async function loadFiles() {
  const tb = document.getElementById('fileList');
  if (!curBot) {
    tb.innerHTML = `<tr><td colspan="5"><div class="empty"><div class="empty-glyph">FILES</div><div class="empty-text">Select an instance to browse files</div></div></td></tr>`;
    return;
  }
  const r = await fetch(`/api/bot/${curBot}/files`);
  const files = await r.json();
  if (!files.length) {
    tb.innerHTML = `<tr><td colspan="5"><div class="empty"><div class="empty-glyph">EMPTY</div><div class="empty-text">No files yet.<br>Upload something to get started.</div></div></td></tr>`;
    return;
  }
  tb.innerHTML = files.map(f => {
    const ext = f.name.split('.').pop().toLowerCase();
    const c = EXT_COLORS[ext] || '#888';
    return `<tr>
      <td><div class="file-name-cell" onclick="editFile('${escH(f.name)}')">${fileGlyph(ext)} ${escH(f.name)}</div></td>
      <td><span class="file-ext-badge" style="color:${c}">.${ext}</span></td>
      <td style="font-family:var(--mono);font-size:11px;color:var(--text3)">${f.size}</td>
      <td style="font-family:var(--mono);font-size:11px;color:var(--text3)">${f.modified}</td>
      <td>
        <div class="btn-row">
          <button class="btn btn-ghost btn-sm" onclick="editFile('${escH(f.name)}')">✏</button>
          <button class="btn btn-ghost btn-sm" onclick="dlFile('${escH(f.name)}')">↓</button>
          <button class="btn btn-red btn-sm" onclick="delFile('${escH(f.name)}')">⊘</button>
        </div>
      </td>
    </tr>`;
  }).join('');
}

function fileGlyph(ext) {
  const g = {py:'🐍',js:'⚡',json:'{}',txt:'≡',md:'#',zip:'⊞',env:'⊛',sh:'$',ts:'⟨⟩'};
  return `<span style="font-family:var(--mono);font-size:10px;opacity:.6">${g[ext]||'□'}</span>`;
}

async function editFile(name) {
  const r = await fetch(`/api/bot/${curBot}/file/${encodeURIComponent(name)}`);
  const d = await r.json();
  document.getElementById('edName').textContent = name;
  document.getElementById('edContent').value = d.content;
  document.getElementById('edContent').dataset.fn = name;
  document.getElementById('mEditor').classList.add('open');
}

async function saveFile() {
  const name = document.getElementById('edContent').dataset.fn;
  await fetch(`/api/bot/${curBot}/file/${encodeURIComponent(name)}`, {
    method:'PUT', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({content: document.getElementById('edContent').value})
  });
  closeModal('mEditor'); loadFiles();
  toast(`${name} saved`, 'success');
}

function openNewFileModal() {
  if (!curBot) { toast('Select an instance first', 'error'); return; }
  document.getElementById('mNewFile').classList.add('open');
}

async function createNewFile() {
  const name = document.getElementById('nfName').value.trim();
  if (!name) { toast('Filename required', 'error'); return; }
  await fetch(`/api/bot/${curBot}/file/${encodeURIComponent(name)}`, {
    method:'PUT', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({content: document.getElementById('nfContent').value})
  });
  closeModal('mNewFile');
  document.getElementById('nfName').value=''; document.getElementById('nfContent').value='';
  loadFiles(); toast(`${name} created`, 'success');
}

async function delFile(name) {
  if (!confirm(`Delete ${name}?`)) return;
  await fetch(`/api/bot/${curBot}/file/${encodeURIComponent(name)}`, {method:'DELETE'});
  loadFiles(); toast(`${name} deleted`, 'success');
}

function dlFile(name) { window.location.href = `/api/bot/${curBot}/file/${encodeURIComponent(name)}/download`; }

async function handleUpload(files) {
  if (!curBot) { toast('Select an instance first', 'error'); return; }
  const prog = document.getElementById('uploadProgress');
  for (const file of files) {
    const fd = new FormData(); fd.append('file', file);
    const wrap = document.createElement('div');
    wrap.className = 'upload-item';
    const safeid = 'up_' + Math.random().toString(36).slice(2);
    wrap.innerHTML = `<span style="flex-shrink:0">↑</span><span style="flex:0 0 140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${escH(file.name)}</span><div class="upload-bar-wrap"><div class="upload-bar-fill" id="${safeid}" style="width:0%"></div></div>`;
    prog.appendChild(wrap);
    await fetch(`/api/bot/${curBot}/upload`, {method:'POST',body:fd});
    const bar = document.getElementById(safeid);
    if (bar) bar.style.width = '100%';
    setTimeout(() => wrap.remove(), 2000);
  }
  loadFiles(); toast(`${files.length} file(s) uploaded`, 'success');
}

const dz = document.getElementById('dropZone');
dz.addEventListener('dragover', e => { e.preventDefault(); dz.classList.add('dragging'); });
dz.addEventListener('dragleave', () => dz.classList.remove('dragging'));
dz.addEventListener('drop', e => { e.preventDefault(); dz.classList.remove('dragging'); handleUpload(e.dataTransfer.files); });

// ── Env ────────────────────────────────────
async function loadEnv() {
  if (!curBot) return;
  const r = await fetch(`/api/bot/${curBot}/env`);
  const env = await r.json();
  const c = document.getElementById('envRows'); c.innerHTML='';
  const entries = Object.entries(env);
  if (entries.length) entries.forEach(([k,v]) => addEnvRow(k,v));
  else addEnvRow('','');
}

function addEnvRow(k='', v='') {
  const d = document.createElement('div'); d.className='env-row';
  d.innerHTML = `<input class="env-field key-field" placeholder="KEY" value="${escH(k)}"><input class="env-field" placeholder="value" value="${escH(v)}"><button class="btn btn-red btn-sm" onclick="this.parentElement.remove()" style="padding:5px 8px">✕</button>`;
  document.getElementById('envRows').appendChild(d);
}

async function saveEnv() {
  if (!curBot) return;
  const env={};
  document.querySelectorAll('.env-row').forEach(r => {
    const k=r.querySelector('.key-field')?.value.trim(), v=r.querySelectorAll('.env-field')[1]?.value;
    if(k) env[k]=v||'';
  });
  await fetch(`/api/bot/${curBot}/env`, {method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(env)});
  toast('Environment saved', 'success');
}

// ── Settings ───────────────────────────────
function loadSettings() {
  if (!curBot) return;
  const b = bots[curBot]||{};
  document.getElementById('stName').value = b.name || '';
  document.getElementById('stStartup').value = b.startup_file || 'main.py';
  document.getElementById('stAR').value = b.auto_restart ? 'true' : 'false';
}

async function saveSettings() {
  if (!curBot) return;
  const data = {
    name: document.getElementById('stName').value.trim(),
    startup_file: document.getElementById('stStartup').value.trim() || 'main.py',
    auto_restart: document.getElementById('stAR').value === 'true'
  };
  const r = await fetch(`/api/bot/${curBot}/settings`, {method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});
  const upd = await r.json();
  bots[curBot] = {...bots[curBot], ...upd};
  document.getElementById('tbBot').textContent = data.name || curBot;
  renderBotList();
  toast('Configuration saved', 'success');
}

// ── Uptime ─────────────────────────────────
function startUptime() {
  clearInterval(uptimeIv);
  uptimeIv = setInterval(() => {
    if (curBot && startTimes[curBot]) {
      const s = Math.floor((Date.now()-startTimes[curBot])/1000);
      const h=Math.floor(s/3600), m=Math.floor(s%3600/60), sec=s%60;
      document.getElementById('sUptime').textContent =
        `${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}:${String(sec).padStart(2,'0')}`;
    }
  }, 1000);
}

// ── Resources ──────────────────────────────
function startRes() { stopRes(); fetchRes(); resIv = setInterval(fetchRes, 3000); }
function stopRes() { clearInterval(resIv); }

async function fetchRes() {
  const r = await fetch('/api/resources');
  const d = await r.json();
  document.getElementById('rCpu').textContent = d.cpu + '%';
  document.getElementById('rCpuSub').textContent = `${d.cpu}% utilization`;
  const cpuCls = d.cpu>80?'red':d.cpu>60?'amber':'gold';
  document.getElementById('pCpu').className=`res-fill ${cpuCls}`; document.getElementById('pCpu').style.width=d.cpu+'%';
  document.getElementById('rMem').textContent = d.mem_used;
  document.getElementById('rMemSub').textContent = `${d.mem_used} of ${d.mem_total} (${d.mem_pct}%)`;
  const memCls = d.mem_pct>85?'red':d.mem_pct>65?'amber':'green';
  document.getElementById('pMem').className=`res-fill ${memCls}`; document.getElementById('pMem').style.width=d.mem_pct+'%';
  document.getElementById('rDsk').textContent = d.disk_pct+'%';
  document.getElementById('rDskSub').textContent = `${d.disk_used} of ${d.disk_total}`;
  const dskCls = d.disk_pct>90?'red':d.disk_pct>70?'amber':'green';
  document.getElementById('pDsk').className=`res-fill ${dskCls}`; document.getElementById('pDsk').style.width=d.disk_pct+'%';
  document.getElementById('sCpu').textContent = d.cpu+'%';
  document.getElementById('sMem').textContent = d.mem_used;
}

// ── Toast ──────────────────────────────────
function toast(msg, type='success') {
  const tray = document.getElementById('toastTray');
  const icons = {success:'✓', error:'✕', info:'ℹ'};
  const t = document.createElement('div');
  t.className = `toast ${type}`;
  t.innerHTML = `<span class="toast-icon">${icons[type]||'·'}</span>${escH(msg)}`;
  tray.appendChild(t);
  setTimeout(() => {
    t.style.transition='all .25s ease'; t.style.opacity='0'; t.style.transform='translateX(10px)';
    setTimeout(()=>t.remove(), 300);
  }, 3200);
}

// ── Modal ──────────────────────────────────
function closeModal(id) { document.getElementById(id).classList.remove('open'); }
document.querySelectorAll('.modal-veil').forEach(m => m.addEventListener('click', e => { if(e.target===m) m.classList.remove('open'); }));

// ── Init ───────────────────────────────────
loadBots();
fetchRes();
setInterval(fetchRes, 5000);
</script>
</body>
</html>"""


# ═══════════════════════════════════════════════
#  API ROUTES
# ═══════════════════════════════════════════════

@app.route('/')
def index(): return render_template_string(HTML)

@app.route('/api/bots', methods=['GET'])
def get_bots():
    cfg = load_config()
    return jsonify({bid: {'id':bid,'name':bc.get('name',bid),'startup_file':bc.get('startup_file','main.py'),'status':'online' if is_running(bid) else 'offline','auto_restart':bc.get('auto_restart',False)} for bid,bc in cfg.items()})

@app.route('/api/bots', methods=['POST'])
def create_bot_route():
    data = request.json or {}
    bid = f"bot_{int(time.time())}"
    cfg = load_config()
    cfg[bid] = {'name':data.get('name','New Bot'),'startup_file':data.get('startup_file','main.py'),'auto_restart':False,'env':{}}
    save_config(cfg); get_bot_dir(bid)
    return jsonify({'id':bid,**cfg[bid],'status':'offline'})

@app.route('/api/bot/<bid>', methods=['DELETE'])
def del_bot(bid):
    stop_bot(bid); cfg=load_config(); cfg.pop(bid,None); save_config(cfg)
    bd=get_bot_dir(bid)
    if os.path.exists(bd): shutil.rmtree(bd)
    bots.pop(bid,None); return jsonify({'ok':True})

@app.route('/api/bot/<bid>/start', methods=['POST'])
def start_route(bid):
    data=request.json or {}; threading.Thread(target=start_bot,args=(bid,data.get('startup_file')),daemon=True).start(); return jsonify({'ok':True})

@app.route('/api/bot/<bid>/stop', methods=['POST'])
def stop_route(bid): stop_bot(bid); return jsonify({'ok':True})

@app.route('/api/bot/<bid>/kill', methods=['POST'])
def kill_route(bid):
    if bid in bots and bots[bid].get('process'):
        try: bots[bid]['process'].kill(); emit_log(bid,'[System] Force killed.','error')
        except: pass
    return jsonify({'ok':True})

@app.route('/api/bot/<bid>/input', methods=['POST'])
def input_route(bid):
    inp=(request.json or {}).get('input','')
    if bid in bots and bots[bid].get('process'):
        p=bots[bid]['process']
        if p.poll() is None and p.stdin:
            try: p.stdin.write(inp); p.stdin.flush()
            except: pass
    return jsonify({'ok':True})

@app.route('/api/bot/<bid>/logs')
def logs_route(bid): return jsonify(bots.get(bid,{}).get('logs',[]))

@app.route('/api/bot/<bid>/files')
def files_route(bid):
    bd=get_bot_dir(bid); out=[]
    for f in sorted(os.listdir(bd)):
        fp=os.path.join(bd,f)
        if os.path.isfile(fp):
            sz=os.path.getsize(fp)
            s=f"{sz}B" if sz<1024 else f"{sz//1024}KB" if sz<1024**2 else f"{sz//1024//1024}MB"
            out.append({'name':f,'size':s,'modified':time.strftime('%Y-%m-%d %H:%M',time.localtime(os.path.getmtime(fp)))})
    return jsonify(out)

@app.route('/api/bot/<bid>/file/<path:fn>', methods=['GET'])
def get_file(bid,fn):
    fp=os.path.join(get_bot_dir(bid),secure_filename(fn))
    if not os.path.exists(fp): return jsonify({'content':''})
    try: return jsonify({'content':open(fp,encoding='utf-8',errors='replace').read()})
    except: return jsonify({'content':'[Binary]'})

@app.route('/api/bot/<bid>/file/<path:fn>', methods=['PUT'])
def put_file(bid,fn):
    fp=os.path.join(get_bot_dir(bid),secure_filename(fn))
    with open(fp,'w',encoding='utf-8') as f: f.write((request.json or {}).get('content',''))
    return jsonify({'ok':True})

@app.route('/api/bot/<bid>/file/<path:fn>', methods=['DELETE'])
def del_file(bid,fn):
    fp=os.path.join(get_bot_dir(bid),secure_filename(fn))
    if os.path.exists(fp): os.remove(fp)
    return jsonify({'ok':True})

@app.route('/api/bot/<bid>/file/<path:fn>/download')
def dl_file(bid,fn): return send_file(os.path.join(get_bot_dir(bid),secure_filename(fn)),as_attachment=True)

@app.route('/api/bot/<bid>/upload', methods=['POST'])
def upload_route(bid):
    if 'file' not in request.files: return jsonify({'error':'no file'}),400
    file=request.files['file']; bd=get_bot_dir(bid); fname=secure_filename(file.filename); sp=os.path.join(bd,fname); file.save(sp)
    if fname.endswith('.zip'):
        try:
            with zipfile.ZipFile(sp,'r') as zf:
                for m in zf.namelist():
                    mp=os.path.abspath(os.path.join(bd,m))
                    if mp.startswith(os.path.abspath(bd)): zf.extract(m,bd)
            os.remove(sp); emit_log(bid,f'[System] Extracted {fname}','system')
        except Exception as e: emit_log(bid,f'[Error] ZIP: {e}','error')
    else: emit_log(bid,f'[System] Uploaded {fname}','system')
    return jsonify({'ok':True})

@app.route('/api/bot/<bid>/env', methods=['GET'])
def get_env(bid): return jsonify(load_config().get(bid,{}).get('env',{}))

@app.route('/api/bot/<bid>/env', methods=['PUT'])
def put_env(bid):
    cfg=load_config(); cfg.setdefault(bid,{})['env']=request.json or {}; save_config(cfg); return jsonify({'ok':True})

@app.route('/api/bot/<bid>/settings', methods=['PUT'])
def put_settings(bid):
    data=request.json or {}; cfg=load_config()
    cfg.setdefault(bid,{}).update({'name':data.get('name',cfg.get(bid,{}).get('name',bid)),'startup_file':data.get('startup_file','main.py'),'auto_restart':data.get('auto_restart',False)})
    save_config(cfg)
    if bid in bots: bots[bid]['auto_restart']=data.get('auto_restart',False)
    return jsonify(cfg[bid])

@app.route('/api/resources')
def resources():
    cpu=psutil.cpu_percent(interval=0.3); mem=psutil.virtual_memory(); disk=psutil.disk_usage('/')
    def fmt(b): return f"{b//1024//1024}MB" if b<1024**3 else f"{b/1024**3:.1f}GB"
    return jsonify({'cpu':round(cpu,1),'mem_used':fmt(mem.used),'mem_total':fmt(mem.total),'mem_pct':round(mem.percent,1),'disk_used':fmt(disk.used),'disk_total':fmt(disk.total),'disk_pct':round(disk.percent,1)})

if __name__ == '__main__':
    print('\n' + '━'*52)
    print('  ZENTROHOST v4.0  ·  Industrial Luxury Edition')
    print('  http://localhost:5000')
    print('━'*52 + '\n')
    socketio.run(app, host='0.0.0.0', port=5000, debug=False)

# CSS for Mobile UI (Tailwind-like or custom media queries)
MOBILE_CSS = r"""
@media (max-width: 768px) {
  body { flex-direction: column; overflow-y: auto; height: auto; }
  .sidebar {
    width: 100%; min-width: 100%; height: auto;
    border-right: none; border-bottom: 1px solid var(--line);
    position: sticky; top: 0; z-index: 100;
  }
  .sidebar.collapsed .nav, .sidebar.collapsed .bot-list, .sidebar.collapsed .new-bot-btn, .sidebar.collapsed .sidebar-footer {
    display: none;
  }
  .main { height: auto; overflow: visible; }
  .topbar { position: sticky; top: 0; z-index: 90; padding: 0 15px; }
  .stats-row { grid-template-columns: 1fr 1fr; }
  .page { padding: 15px; }
  .panel-body { padding: 12px; }
  .terminal { height: 300px; }
  .tb-bot { max-width: 120px; }
  .logo { padding: 12px 15px; display: flex; align-items: center; justify-content: space-between; }
  .mobile-toggle { display: block !important; color: var(--gold); font-size: 20px; background: none; border: none; }
}
.mobile-toggle { display: none; }
"""