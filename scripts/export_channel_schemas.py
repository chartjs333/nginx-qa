"""Generate/review the additive Bridge schemas without editing frozen runtime v1/v2."""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from nginx_qa.channel_models import Binding, BridgePolicy, CreateRequest, Event


def main():
    destination = ROOT / "docs" / "agent-channel-bridge" / "schemas"
    for name, model in {
        "create-request-v1": CreateRequest,
        "event-v1": Event,
        "binding-v1": Binding,
        "config-v1": BridgePolicy,
    }.items():
        schema = model.model_json_schema()
        schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
        schema["$id"] = (
            "https://github.com/chartjs333/nginx-qa/agent-channel-bridge/" + name
        )
        content = json.dumps(schema, indent=2, sort_keys=True) + "\n"
        path = destination / (name + ".schema.json")
        if "--check" in sys.argv:
            if path.read_text() != content:
                raise SystemExit(f"Schema differs: {path.name}")
        else:
            destination.mkdir(parents=True, exist_ok=True)
            path.write_text(content)


if __name__ == "__main__":
    main()
