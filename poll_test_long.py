import urllib.request
import json
import time
import sys
from datetime import datetime

print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting long-running poll of test queue (every 5 minutes)...")
start_time = time.time()
poll_interval = 300  # 5 minutes

while True:
    try:
        with urllib.request.urlopen("http://127.0.0.1:8025/test") as resp:
            data = json.loads(resp.read().decode("utf-8"))
            print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] --- NEW TASK RECEIVED ---")
            print(data["message"])
            sys.exit(0)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            # Queue is empty, wait 5 minutes
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Queue empty. Waiting 5 minutes...")
            time.sleep(poll_interval)
        else:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] HTTP Error {e.code}. Waiting 5 minutes...")
            time.sleep(poll_interval)
    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Connection error: {e}. Waiting 5 minutes...")
        time.sleep(poll_interval)
