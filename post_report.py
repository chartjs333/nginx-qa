import urllib.request
import json
import sys

report = """TO: Programmer
FROM: Tester
STATUS: FAIL

CHECKED:
- Clinical review (Specifically Family 1 Case 2 clinical symptoms)

PROBLEMS:
- problem: The system incorrectly extracted absent symptoms as present ("yes").
- page/url: https://gp2admin.neuro.uni-luebeck.de/extractor/ (Clinical review tab)
- endpoint/action: Viewing Case 2 clinical extraction details in Current Case Workspace vs Table 1 of the PDF panel.
- request/input: Selecting Family 1 Case 2 in the Review Queue.
- expected: Symptoms marked as "-" in Dogu et al. 2013 Table 1 should be absent ("no"), specifically: Dysarthria, Ocular movements, Psychiatric/behavioral signs, and Cognitive signs.
- actual: The extraction lists: Dysarthria = yes, Ocular movements = yes, Psychiatric/behavioral signs = yes, Cognitive signs = yes.
- clinical/genetic risk: High clinical risk. It contaminates the patient clinical profile by attributing non-existent symptoms, leading to downstream cohort definition and gene-phenotype association analysis failures.
- steps to reproduce:
  1. Login and go to Clinical review tab.
  2. Select "Family 1 Case 2" from the queue.
  3. Notice that "Dysarthria", "Ocular movements", "Psychiatric/behavioral signs", and "Cognitive signs" are all shown as "yes".
  4. Compare with the Dogu et al. 2013 Table 1 in the PDF panel, showing them as "-".
- console errors: None.
- network errors: None.
- screenshot/video/HAR evidence: saved in C:\\Users\\madoev\\.gemini\\antigravity\\brain\\79a77507-97fd-4913-b0bc-2baa518df79b\\extractor_dashboard_1779902129323.png

FIX REQUEST:
- Correct the extraction logic or LLM prompting to correctly recognize "-" (dash) as "no" or "absent" and avoid extracting them as "yes" (present).

RETEST AFTER FIX:
- Verify that Family 1 Case 2 has "no" or "absent" values for Dysarthria, Ocular movements, Psychiatric/behavioral signs, and Cognitive signs.
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
