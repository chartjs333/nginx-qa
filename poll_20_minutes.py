import urllib.request
import json
import time
import sys

print("Starting long-polling monitor: 20 polls at 1-minute intervals.")
sys.stdout.flush()

for i in range(1, 21):
    try:
        req = urllib.request.Request("http://127.0.0.1:8025/test")
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            print(f"SUCCESS on Poll {i}/20: {json.dumps(data)}")
            sys.stdout.flush()
            # If we get a task, we exit successfully so the agent can process it immediately!
            sys.exit(0)
    except Exception as e:
        if "404" in str(e):
            print(f"Poll {i}/20: No task in queue (404). Sleeping 60s...")
        else:
            print(f"Poll {i}/20: Error ({str(e)}). Sleeping 60s...")
        sys.stdout.flush()
    
    if i < 20:
        time.sleep(60)

print("Finished 20 polls. No new task found.")
sys.stdout.flush()
sys.exit(0)
