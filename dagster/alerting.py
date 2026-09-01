"""Dagster failure notifications without automatic run retries.

A failed step is logged, appended to an alert registry kept as a single JSON
object in MinIO, and optionally sent to a webhook. Failed runs are retried
only when a user explicitly starts a re-execution from Dagster.

The same MinIO object is read by the Streamlit dashboard ("🚨 Alertler"
tab), so Dagster and the dashboard no longer share a bind-mounted file.
Bucket / object are configurable with MINIO_ALERTS_BUCKET /
MINIO_ALERTS_OBJECT; the MinIO connection settings are the same ones
assets.create_minio_client() reads (MINIO_ENDPOINT, MINIO_ACCESS_KEY, ...).
"""

import io
import json
import os
from datetime import datetime, timezone

import requests
from dagster import HookContext, failure_hook, success_hook
from minio.error import S3Error

from assets import create_minio_client
from postgres_catalog import mark_job_run_failure


ALERTS_BUCKET = os.getenv("MINIO_ALERTS_BUCKET", "pipeline-alerts")
ALERTS_OBJECT = os.getenv("MINIO_ALERTS_OBJECT", "alerts.json")
ALERTS_URI = f"s3://{ALERTS_BUCKET}/{ALERTS_OBJECT}"
MAX_ALERTS = 500


def _load_alerts() -> list:
    """Return the alert list stored in MinIO ([] if bucket/object missing)."""

    client = create_minio_client()
    try:
        response = client.get_object(ALERTS_BUCKET, ALERTS_OBJECT)
    except S3Error as error:
        if error.code in {"NoSuchKey", "NoSuchBucket"}:
            return []
        raise

    try:
        payload = json.loads(response.read())
    finally:
        response.close()
        response.release_conn()

    return payload if isinstance(payload, list) else []


def _store_alerts(alerts: list) -> None:
    """Overwrite the MinIO alert object (creating the bucket if needed)."""

    client = create_minio_client()
    if not client.bucket_exists(ALERTS_BUCKET):
        client.make_bucket(ALERTS_BUCKET)

    body = json.dumps(alerts, ensure_ascii=False, indent=2).encode("utf-8")
    client.put_object(
        ALERTS_BUCKET,
        ALERTS_OBJECT,
        io.BytesIO(body),
        length=len(body),
        content_type="application/json",
    )


def _save_alert(alert_data: dict) -> None:
    # Dosya sürümündeki gibi oku-değiştir-yaz. Eşzamanlı yazımlar için
    # kilit yok; tek bir pipeline run'ının hook'ları pratikte sıralı
    # çalıştığı için dosya sürümüyle aynı garanti korunur.
    alerts = _load_alerts()
    alerts.append(alert_data)
    _store_alerts(alerts[-MAX_ALERTS:])


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
        context.log.info("Alert kaydedildi: %s", ALERTS_URI)
    except Exception as error:
        context.log.error("Alert MinIO'ya yazılamadı (%s): %s", ALERTS_URI, error)

    _send_webhook(alert_data)
    context.log.info(
        "Otomatik retry kapalı; yeniden çalıştırma kullanıcıya bırakıldı."
    )


@success_hook
def clear_alert_on_success(context: HookContext) -> None:
    """Resolve ancestor-run alerts after a successful manual re-execution."""

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
        existing_alerts = _load_alerts()
        if not existing_alerts:
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
            _store_alerts(existing_alerts)
            context.log.info(
                "Geçmiş hata çözüldü: Job=%s, Step=%s",
                job_name,
                step_name,
            )
    except Exception as error:
        context.log.error("Alert durumu güncellenirken hata oluştu: %s", error)
