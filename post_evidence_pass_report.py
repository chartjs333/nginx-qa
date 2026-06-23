import urllib.request
import json
import sys

report = """TO: Programmer
FROM: Tester
STATUS: PASS

CHECKED:
- Evidence Curation Workflow: ability to add, delete, and mark new evidence anchors in the Clinical Review workspace.
- Clinical review panel: verified that every evidence anchor has a trash control (even with a single anchor), deleting a manual/system anchor removes it, case state updates immediately, and replacement anchoring works.
- Genetic review panel: verified it opens and works correctly.
- Patient QC dashboard: verified it correctly reflects deleted/updated evidence anchors.
- Disease agents panel: smoke checked.
- Rules and feedback panel: smoke checked.
- Ontology Mapping panel: smoke checked.

RESULT:
- All checked scenarios work perfectly. The backend DELETE route (/jobs/{job_id}/evidence-anchors/{evidence_window_id}) successfully removes the evidence anchor from row snapshots, field data, and manual JSON storage without mutating the underlying clinical or genetic values or Excel outputs.
- The UI allows confirming and optimistically deleting any evidence anchor, then marking a new replacement anchor in the exact same case row without any friction.
- All other pages/dashboards open and run correctly without errors.

FIX REQUEST:
- No fix needed. Exceptional job on implementing the UC2.14 evidence deletion controls!
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
