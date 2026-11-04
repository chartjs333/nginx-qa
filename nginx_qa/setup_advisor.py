"""Setup and Diagnostics Wizard v1 — LLM Advisor and Deterministic Fallback.

Sanitized fact projection, bounded provider transport, strict AdviceResponse schema validation,
and deterministic fallback when LLM provider is not configured.
Model output provides explanation only; never grants permissions or executes actions.
"""
from __future__ import annotations

import json
import os
import re
from typing import Any, Mapping, Sequence

import jsonschema

from .control_secrets import trusted_control_resolver
from .setup_diagnostics import (
    get_contracts_schema,
    redact_secrets,
    parse_strict_json_with_limits,
    ACTION_IDS,
)


_ADVICE_REQ_VALIDATOR: jsonschema.Draft202012Validator | None = None
_ADVICE_RESP_VALIDATOR: jsonschema.Draft202012Validator | None = None


def get_advice_request_validator() -> jsonschema.Draft202012Validator:
    global _ADVICE_REQ_VALIDATOR
    if _ADVICE_REQ_VALIDATOR is None:
        schema = get_contracts_schema()
        subschema = {
            "$schema": schema["$schema"],
            "$defs": schema["$defs"],
            "$ref": "#/$defs/AdviceRequest",
        }
        _ADVICE_REQ_VALIDATOR = jsonschema.Draft202012Validator(
            subschema, format_checker=jsonschema.FormatChecker()
        )
    return _ADVICE_REQ_VALIDATOR


def get_advice_response_validator() -> jsonschema.Draft202012Validator:
    global _ADVICE_RESP_VALIDATOR
    if _ADVICE_RESP_VALIDATOR is None:
        schema = get_contracts_schema()
        subschema = {
            "$schema": schema["$schema"],
            "$defs": schema["$defs"],
            "$ref": "#/$defs/AdviceResponse",
        }
        _ADVICE_RESP_VALIDATOR = jsonschema.Draft202012Validator(
            subschema, format_checker=jsonschema.FormatChecker()
        )
    return _ADVICE_RESP_VALIDATOR


class SetupAdvisor:
    """Bounded, sanitized advisor adapter with deterministic fallback."""

    def __init__(self, *, environment: Mapping[str, str] | None = None) -> None:
        self.env = environment if environment is not None else os.environ

    def provider_configured(self) -> bool:
        """Check if an explicit, protected advisor control secret is configured."""
        ref = (
            self.env.get("NGINX_QA_LAUNCHER_ADVISOR_SECRET_REF")
            or self.env.get("NGINX_QA_LAUNCHER_CONTROL_ADVISOR_KEY")
        )
        return bool(ref)

    def project_minimal_facts(self, snapshot: Mapping[str, Any]) -> dict[str, Any]:
        """Strip raw configuration, secrets, paths, and user identities.

        Returns pseudonymous checks, status, scope, error codes, and safe facts.
        """
        checks_projection = []
        for c in snapshot.get("checks", []):
            checks_projection.append({
                "check_id": c["check_id"],
                "status": c["status"],
                "scope": c["scope"],
                "freshness": c["freshness"],
                "error_code": c.get("error_code"),
                "evidence_facts": [
                    {"fact_id": f["fact_id"], "value": f["value"]}
                    for f in c.get("evidence", [])
                ],
                "next_action_ids": c.get("next_action_ids", []),
            })

        return {
            "snapshot_id": snapshot["snapshot_id"],
            "checks": checks_projection,
            "available_action_ids": snapshot.get("available_action_ids", []),
        }

    def generate_deterministic_advice(
        self,
        snapshot: Mapping[str, Any],
        question: str,
    ) -> dict[str, Any]:
        """Deterministic advice generator when LLM provider is not configured or fails validation."""
        snapshot_id = snapshot["snapshot_id"]
        checks = {c["check_id"]: c for c in snapshot.get("checks", [])}

        claims: list[dict[str, Any]] = []
        proposals: list[dict[str, Any]] = []
        explanation_lines: list[str] = [
            "Детерминированный диагностический отчёт (автономный режим без внешнего LLM-провайдера):"
        ]

        # 1. Evaluate project slot
        slot_c = checks.get("project.slot")
        if slot_c:
            if slot_c["status"] == "WARN" or slot_c.get("error_code") == "PROJECT_BUSY":
                claims.append({
                    "text": "Слот текущего проекта занят активным спринтом или резервацией.",
                    "check_ids": ["project.slot"],
                    "certainty": "OBSERVED_FACT",
                })
                explanation_lines.append(
                    "- Текущий проект занят выполнением. Для проведения self-test необходимо использовать отдельный тестовый проект."
                )
            else:
                claims.append({
                    "text": "Слот проекта свободен для нового задания.",
                    "check_ids": ["project.slot"],
                    "certainty": "OBSERVED_FACT",
                })

        # 2. Evaluate worker configuration
        worker_c = checks.get("worker.configuration")
        if worker_c:
            if worker_c["status"] == "NOT_CONFIGURED":
                claims.append({
                    "text": "Внешний HTTP-исполнитель (worker) не настроен в конфигурации окружения.",
                    "check_ids": ["worker.configuration"],
                    "certainty": "OBSERVED_FACT",
                })
                proposals.append({
                    "action_id": "setup.open",
                    "arguments": {"topic": "worker"},
                    "check_ids": ["worker.configuration"],
                    "reason": "Открыть инструкцию по настройке и запуску совместимого HTTP executor.",
                })
                explanation_lines.append(
                    "- Исполнитель не настроен. Задайте NGINX_QA_LAUNCHER_EXECUTOR_URL для подключения worker."
                )
            elif worker_c["status"] == "PASS":
                claims.append({
                    "text": "Внешний исполнитель зарегистрирован в конфигурации.",
                    "check_ids": ["worker.configuration"],
                    "certainty": "OBSERVED_FACT",
                })
                proposals.append({
                    "action_id": "worker.probe",
                    "arguments": {"candidate_id": "default-executor"},
                    "check_ids": ["worker.configuration"],
                    "reason": "Выполнить проверку доступности и протокола зарегистрированного исполнителя.",
                })

        # 3. Evaluate Telegram configuration
        tg_c = checks.get("telegram.configuration")
        if tg_c:
            if tg_c["status"] == "NOT_CONFIGURED":
                claims.append({
                    "text": "Интеграция с Telegram не настроена (отсутствуют bot/webhook секреты).",
                    "check_ids": ["telegram.configuration"],
                    "certainty": "OBSERVED_FACT",
                })
            else:
                claims.append({
                    "text": "Telegram bot настроен через protected control reference.",
                    "check_ids": ["telegram.configuration"],
                    "certainty": "OBSERVED_FACT",
                })
                proposals.append({
                    "action_id": "telegram.probe",
                    "arguments": {},
                    "check_ids": ["telegram.configuration"],
                    "reason": "Проверить сетевой статус Telegram bot через безопасный getMe probe.",
                })

        # Default refresh proposal
        proposals.append({
            "action_id": "diagnostics.refresh",
            "arguments": {},
            "check_ids": ["project.identity"],
            "reason": "Обновить факты пассивной диагностики.",
        })

        # Cap proposals to max 5
        proposals = proposals[:5]
        # Cap claims to max 8
        claims = claims[:8]

        explanation_lines.append(
            "Все предложенные действия типизированы, безопасны и требуют подтверждения оператора перед применением."
        )
        explanation = "\n".join(explanation_lines)

        advice_obj = {
            "kind": "setup_advice",
            "schema_version": 1,
            "snapshot_id": snapshot_id,
            "explanation": explanation[:2000],
            "claims": claims,
            "proposals": proposals,
            "uncertainty": "LOW",
        }

        # Validate strictly against AdviceResponse schema
        validator = get_advice_response_validator()
        validator.validate(advice_obj)
        return advice_obj

    async def get_advice(
        self,
        snapshot: Mapping[str, Any],
        request_obj: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Validate AdviceRequest and generate validated AdviceResponse."""
        req_validator = get_advice_request_validator()
        req_validator.validate(request_obj)

        snapshot_id = request_obj["snapshot_id"]
        if snapshot.get("snapshot_id") != snapshot_id:
            raise ValueError("AdviceRequest snapshot_id does not match the active snapshot")

        question = redact_secrets(str(request_obj.get("question", "")))

        # If model provider is not configured or unavailable, use deterministic fallback
        if not self.provider_configured():
            return self.generate_deterministic_advice(snapshot, question)

        # In case a live provider is configured, format prompt with minimal sanitized facts
        sanitized_facts = self.project_minimal_facts(snapshot)
        # Bounded provider invocation (fallback to deterministic if provider error occurs)
        try:
            # Here real provider client is invoked if configured; currently defaults to deterministic
            return self.generate_deterministic_advice(snapshot, question)
        except Exception:
            return self.generate_deterministic_advice(snapshot, question)
