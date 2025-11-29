import subprocess
import time
import os
import sys

print("Starting gather_lighter_data.py...")
# Ensure we flush output buffers to catch print statements
env = os.environ.copy()
env["PYTHONUNBUFFERED"] = "1"

process = subprocess.Popen(
    [sys.executable, "gather_lighter_data.py"],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
    env=env
)

print(f"Process started with PID: {process.pid}")
print("Waiting for 30 seconds to gather data...")

try:
    time.sleep(30)
except KeyboardInterrupt:
    print("Interrupted by user")

print("Stopping gather_lighter_data.py...")
process.terminate()

try:
    stdout, stderr = process.communicate(timeout=10)
    print("Process terminated gracefully.")
except subprocess.TimeoutExpired:
    print("Process did not terminate, killing...")
    process.kill()
    stdout, stderr = process.communicate()

print("\n--- STDOUT (last 20 lines) ---")
lines = stdout.splitlines()
for line in lines[-20:]:
    print(line)

print("\n--- STDERR (last 20 lines) ---")
err_lines = stderr.splitlines()
for line in err_lines[-20:]:
    print(line)
