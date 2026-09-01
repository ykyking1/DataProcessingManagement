"""Dagster failure notifications without automatic run retries.

A failed step is logged, appended to the local alert registry, and optionally
sent to a webhook. Failed runs are retried only when a user explicitly starts
a re-execution from Dagster.
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
from dagster import HookContext, failure_hook, success_hook

from postgres_catalog import mark_job_run_failure


ALERT_FILE = Path(os.getenv("ALERT_FILE", "data/alerts/alerts.json"))
ALERT_DIR = ALERT_FILE.parent


def _ensure_alert_file() -> None:
    ALERT_DIR.mkdir(parents=True, exist_ok=True)
    if not ALERT_FILE.exists():
        ALERT_FILE.write_text("[]", encoding="utf-8")


def _save_alert(alert_data: dict) -> None:
    _ensure_alert_file()
    try:
        existing_alerts = json.loads(ALERT_FILE.read_text(encoding="utf-8"))
        if not isinstance(existing_alerts, list):
            existing_alerts = []
    except (json.JSONDecodeError, OSError):
        existing_alerts = []

    existing_alerts.append(alert_data)
    ALERT_FILE.write_text(
        json.dumps(existing_alerts[-500:], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _send_webhook(alert_data: dict) -> None:
    webhook_url = os.getenv("ALERT_WEBHOOK_URL", "").strip()
    if not webhook_url:
        return

    payload = {
        "text": (
            "Pipeline hatası\n\n"
            f"Job: {alert_data['job_name']}\n"
            f"Step: {alert_data['step_name']}\n"
            f"Hata: {alert_data['error']}\n"
            f"Zaman: {alert_data['timestamp']}"
        )
    }
    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
    except Exception as error:
        # Notification failures must not alter the pipeline result.
        print(f"Webhook gönderilemedi: {error}")


@failure_hook
def alert_on_failure(context: HookContext) -> None:
    """Record one alert for a failed step; never launch a retry."""

    job_name = context.job_name
    try:
        step_name = context.op.name
    except Exception:
        step_name = "unknown"

    error_text = str(context.op_exception or "Bilinmeyen hata")
    alert_data = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "job_name": job_name,
        "step_name": step_name,
        "error": error_text,
        "status": "FAILURE",
        "run_id": context.run_id,
    }

    context.log.error(
        "HATA ALINDI! Job: %s, Step: %s. Hata: %s",
        job_name,
        step_name,
        error_text,
    )
    try:
        mark_job_run_failure(
            context.run_id,
            failed_step=step_name,
            error=context.op_exception,
        )
    except Exception as error:
        context.log.error("PostgreSQL run hatası kaydedilemedi: %s", error)

    try:
        _save_alert(alert_data)
        context.log.info("Alert kaydedildi: %s", ALERT_FILE)
    except Exception as error:
        context.log.error("Alert dosyasına yazılamadı: %s", error)

    _send_webhook(alert_data)
    context.log.info(
        "Otomatik retry kapalı; yeniden çalıştırma kullanıcıya bırakıldı."
    )


@success_hook
def clear_alert_on_success(context: HookContext) -> None:
    """Resolve ancestor-run alerts after a successful manual re-execution."""

    if not ALERT_FILE.exists():
        return

    job_name = context.job_name
    try:
        step_name = context.op.name
    except Exception:
        step_name = "unknown"

    ancestor_run_ids: set[str] = set()
    try:
        current_run = context.instance.get_run_by_id(context.run_id)
        while current_run is not None and current_run.parent_run_id:
            ancestor_run_ids.add(current_run.parent_run_id)
            current_run = context.instance.get_run_by_id(
                current_run.parent_run_id
            )
    except Exception as error:
        context.log.error("Run soy ağacı okunurken hata oluştu: %s", error)
        return

    if not ancestor_run_ids:
        return

    try:
        existing_alerts = json.loads(ALERT_FILE.read_text(encoding="utf-8"))
        if not isinstance(existing_alerts, list):
            return

        resolved_at = datetime.now(timezone.utc).isoformat()
        is_updated = False
        for alert in existing_alerts:
            matches_manual_retry = (
                alert.get("job_name") == job_name
                and alert.get("step_name") == step_name
                and alert.get("status") == "FAILURE"
                and alert.get("run_id") in ancestor_run_ids
            )
            if matches_manual_retry:
                alert["status"] = "RESOLVED"
                alert["resolved_at"] = resolved_at
                is_updated = True

        if is_updated:
            ALERT_FILE.write_text(
                json.dumps(existing_alerts, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            context.log.info(
                "Geçmiş hata çözüldü: Job=%s, Step=%s",
                job_name,
                step_name,
            )
    except Exception as error:
        context.log.error("Alert durumu güncellenirken hata oluştu: %s", error)
