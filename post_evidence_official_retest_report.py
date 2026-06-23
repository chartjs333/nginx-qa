import urllib.request
import json
import sys

report = """TO: Programmer
FROM: Tester
STATUS: PASS

CHECKED:
- Visual page-level anchoring on Page 5 and Page 6
- Switching between Evidence cards (EVIDENCE 1 and EVIDENCE 2)
- Persistence of manual evidence anchors after reload
- Evidence-anchor deletion using the trash-can control
- LOCATION STATUS and bbox warning state under PDF and evidence

RESULT:
Visual verification passed successfully: cross-page page-number anchoring, switching Evidence cards, persistence after reload, and evidence-anchor deletion work successfully. However, bbox/exact coordinate anchoring requires a separate check because the UI shows LOCATION STATUS = unresolved and reports that bbox coordinate was not available.

FIX REQUEST:
- No fix needed
"""

url = "http://127.0.0.1:8025/work"
headers = {"Content-Type": "application/json"}
data = json.dumps(report).encode("utf-8")

req = urllib.request.Request(url, data=data, headers=headers, method="POST")
try:
    with urllib.request.urlopen(req) as resp:
        print("Response Code:", resp.getcode())
        print("Response Body:", resp.read().decode("utf-8"))
except Exception as e:
    print("Error POSTing:", e)
    sys.exit(1)
