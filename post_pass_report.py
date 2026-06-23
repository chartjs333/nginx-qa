import urllib.request
import json
import sys

report = """TO: Programmer
FROM: Tester
STATUS: PASS

PROGRAM:
- Evidence Curation Workflow: browser delete-click retest after direct-click fix (testing-2026-05-28-03).

DETAILED VERIFICATION RESULTS:
1. **Direct Click Deletion (No Confirm Dialog)**:
   - Verified that clicking the trash-can icon of a manual evidence anchor ONCE triggers immediate deletion.
   - Checked that **NO browser `confirm` dialog** appeared, demonstrating that the gate has been successfully removed and the direct delete click is active.
   - The card vanished from the UI immediately (optimistic update), and the deletion was sent to the backend.

2. **Deletion Database Persistence**:
   - Reloaded the browser page and returned to the case.
   - Confirmed the deleted manual evidence anchor card remained completely absent, verifying successful backend deletion persistence in the registry.

3. **Manual Anchor Creation and Bbox Integrity**:
   - Created a fresh replacement manual anchor by highlighting text on Page 6 ("probably triggered by the oxidative stress caused by") and clicking "Set as Evidence".
   - Reloaded the page.
   - Confirmed the new manual anchor card (Evidence 3) persisted perfectly.
   - Clicking it navigated the PDF viewer to Page 6, highlighted the text with a gold bounding box, and displayed LOCATION STATUS = 'resolved' with no warnings about missing bbox coordinates.

4. **Sanity Smoke Checks**:
   - Performed smoke checks across the entire system tabs:
     * **Genetic review**: Loaded successfully without errors.
     * **Patient QC**: Loaded successfully; registry items and issues were consistent.
     * **Disease agents**: Loaded successfully; Orchestration panel is fully operational.
     * **Rules and feedback**: Loaded successfully; shows feedback and safety constraints.
     * **Ontology Mapping**: Loaded successfully and responsive.

All systems are fully verified, stable, and ready for production!
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


