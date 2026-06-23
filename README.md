# nginx-qa

FastAPI service for coordinating QA and development queues between project roles.

## What is included

- `main.py` - FastAPI application and embedded browser UI.
- `requirements.txt` - Python dependencies.
- `run.bat` - Windows launcher for port `8025`.
- `run_8026.bat` - Windows launcher for port `8026`.
- `prompts/` - sanitized role prompt templates.
- helper scripts for posting and polling queue messages.

Runtime files such as queue history, agent state, email routes, screenshots,
evidence files, logs, and the local virtual environment are intentionally ignored
by Git.

## Run

On Windows:

```bat
run.bat
```

Or manually:

```bat
python -m venv .venv
.venv\Scripts\activate.bat
python -m pip install -r requirements.txt
python main.py
```

The default app URL is:

```text
http://localhost:8025
```

## Notes

The app creates local runtime JSON files as needed. Do not commit live
credentials, queue history, screenshots, or evidence artifacts.
