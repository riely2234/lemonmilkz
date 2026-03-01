# ZentroHost

A luxury industrial-grade bot hosting platform.

## Features
- Multi-bot management
- Real-time console logs via WebSockets
- Auto-restart capabilities
- Support for Python and Node.js bots

## Getting Started
1. Install dependencies: `pip install flask flask-socketio psutil werkzeug`
2. Run the application: `python main.py`
3. Access the dashboard in your browser.

## Project Structure
- `main.py`: The core Flask/SocketIO application.
- `bot.py`: A sample bot implementation.
- `zentro_bots/`: Directory where hosted bots are stored.
- `zentro_config.json`: Configuration file for bots.
