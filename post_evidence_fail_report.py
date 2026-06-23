import urllib.request
import json
import sys

report = """TO: Programmer
FROM: Tester
STATUS: FAIL

CHECKED:
- Evidence Curation Workflow: ability to add, delete, and mark new evidence anchors in the Clinical Review workspace.

PROBLEMS:
- problem: The system does not support deleting or removing manually attached or system-extracted evidence anchors. There is no delete button or trash can icon in the Evidence UI, and no HTTP DELETE endpoint exists in the backend API to handle this action.
- page/url: https://gp2admin.neuro.uni-luebeck.de/extractor/ (Clinical review tab)
- endpoint/action: ClinicalReviewPanel.tsx / PdfEvidenceViewer.tsx
- request/input: Attempting to delete or edit an existing manual/system evidence anchor.
- expected: The clinician should be able to view a list of evidence anchors attached to the current review case, with a delete/remove button next to each anchor to remove it, and a corresponding DELETE endpoint in the backend API.
- actual: There is no delete button in the UI, only a list of anchors to select/view. The backend (main.py) does not define any endpoint for deleting evidence anchors.
- clinical/genetic risk: High clinical risk. If a clinician manually marks the wrong text as evidence or if the system extracts incorrect evidence, there is no way to remove or correct it. This results in dirty/incorrect evidence being permanently saved in the curation database, leading to invalid clinical validation exports.
- steps to reproduce:
  1. Login and go to the Clinical review panel.
  2. Select any symptom card in the Review Queue.
  3. Try to delete an existing evidence anchor. Observe that there is no delete/remove button.
  4. Perform text selection in the PDF viewer to manually attach a new evidence anchor.
  5. Try to remove the newly attached manual anchor. Observe that it is impossible.
- console errors: None.
- network errors: None.
- screenshot/video/HAR evidence: saved in C:\\Users\\madoev\\.gemini\\antigravity\\brain\\79a77507-97fd-4913-b0bc-2baa518df79b\\extractor_dashboard_1779972048401.png

FIX REQUEST:
1. Implement a backend HTTP DELETE route in main.py to remove a manual or system evidence anchor from a case row.
2. Update the frontend UI (ClinicalReviewPanel.tsx or PdfEvidenceViewer.tsx) to render a delete/remove button (e.g., trash can icon) next to each evidence anchor in the active case. When clicked, call the backend DELETE API to remove it, update the state, and refresh the UI.

RETEST AFTER FIX:
- Verify that a clinician can select an evidence anchor, click a delete button, confirm that it is removed from both the UI list and the backend database, and successfully mark a new anchor to replace it.
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
