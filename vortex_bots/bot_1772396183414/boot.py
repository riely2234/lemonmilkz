import os
import shutil
import sys
import asyncio

async def run_command(command):
    process = await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await process.communicate()

    if stdout:
        print(stdout.decode().strip())
    if stderr:
        print(stderr.decode().strip())

    return process.returncode

async def main():
    for folder in [".pythonlibs", ".local", "venv", "__pycache__", ".venv"]:
        if os.path.exists(folder):
            shutil.rmtree(folder)

    print("\x1b[1;36mINSTALLING UV\x1b[0m")
    await run_command(f"{sys.executable} -m pip install --upgrade pip")
    await run_command(f"{sys.executable} -m pip install uv")

    print("\x1b[1;36mCREATING UV VENV\x1b[0m")
    await run_command("uv venv")

    print("\x1b[1;32mINSTALLING REQUIREMENTS\x1b[0m")
    await run_command("uv pip install -r requirements.txt")

    print("\x1b[1;33mRUNNING BOT\x1b[0m")
    os.chdir("src")
    await run_command("uv run main.py")

if __name__ == "__main__":
    asyncio.run(main())