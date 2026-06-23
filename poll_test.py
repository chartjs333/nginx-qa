import urllib.request
import json
import time
import sys

print("Polling test queue for new task...")
start_time = time.time()
while time.time() - start_time < 300: # Poll for up to 5 minutes
    try:
        with urllib.request.urlopen("http://127.0.0.1:8025/test") as resp:
            data = json.loads(resp.read().decode("utf-8"))
            print("\n--- NEW TASK RECEIVED ---")
            print(data["message"])
            sys.exit(0)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            # Queue is empty, wait
            sys.stdout.write(".")
            sys.stdout.flush()
            time.sleep(5)
        else:
            print("\nHTTP Error:", e.code)
            time.sleep(5)
    except Exception as e:
        print("\nConnection error:", e)
        time.sleep(5)

print("\nTimeout waiting for new task.")
sys.exit(1)
