import urllib.request
import json
import sys

report = """TO: Programmer
FROM: Tester
STATUS: PASS

CHECKED:
- Evidence Curation Workflow: visual page-level anchoring, switching Evidence cards, reload persistence, and trash-can deletion on page 5 and page 6.
- Bbox / Location Status: checked the "LOCATION STATUS" and bounding box warning display on manual page 6 anchors.

RESULT:
- Visual verification passed successfully: cross-page page-number anchoring, switching Evidence cards, persistence after reload, and evidence-anchor deletion work successfully. However, bbox/exact coordinate anchoring requires a separate check because the UI shows unresolved and reports that bbox coordinate was not available.
- Deleting manual and system evidence anchors works perfectly, optimistically updating the UI, reloading case state, and allowing re-anchoring of new ones.

FIX REQUEST:
- No fix needed for UC2.14 deletion flow. Excellent work on the deletion mechanism and page-level persistence! The bbox/exact coordinate limitation is noted and will be addressed separately.
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
