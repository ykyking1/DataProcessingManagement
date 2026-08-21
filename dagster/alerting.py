"""
Dagster Alerting Sistemi

Pipeline içerisindeki herhangi bir step başarısız olduğunda:

1. Dagster loglarına hata yazılır.
2. data/alerts/alerts.json dosyasına hata kaydedilir.
3. İstenirse WEBHOOK_URL üzerinden Slack / Teams vb. sistemlere bildirim
   gönderilebilir.

Webhook kullanımı için:

Windows PowerShell:

$env:ALERT_WEBHOOK_URL="https://..."

Webhook kullanmak istemiyorsan bu değişkeni boş bırakabilirsin.
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import os

import requests

from dagster import failure_hook, success_hook, HookContext


# ---------------------------------------------------------------------------
# Alert kayıt dizini
# ---------------------------------------------------------------------------

ALERT_DIR = Path("data/alerts")
ALERT_FILE = ALERT_DIR / "alerts.json"


# ---------------------------------------------------------------------------
# Alert dosyasını hazırla
# ---------------------------------------------------------------------------

def _ensure_alert_file():
    """
    Alert klasörü ve JSON dosyası yoksa oluşturur.
    """

    ALERT_DIR.mkdir(parents=True, exist_ok=True)

    if not ALERT_FILE.exists():
        ALERT_FILE.write_text(
            "[]",
            encoding="utf-8",
        )


# ---------------------------------------------------------------------------
# Alert kaydet
# ---------------------------------------------------------------------------

def _save_alert(alert_data: dict):
    """
    Alert bilgisini JSON dosyasına ekler.
    """

    _ensure_alert_file()

    try:
        existing_alerts = json.loads(
            ALERT_FILE.read_text(encoding="utf-8")
        )

        if not isinstance(existing_alerts, list):
            existing_alerts = []

    except (json.JSONDecodeError, OSError):
        existing_alerts = []

    existing_alerts.append(alert_data)

    # Çok büyümesini engellemek için son 500 alert tutuluyor.
    existing_alerts = existing_alerts[-500:]

    ALERT_FILE.write_text(
        json.dumps(
            existing_alerts,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Webhook
# ---------------------------------------------------------------------------

def _send_webhook(alert_data: dict):
    """
    ALERT_WEBHOOK_URL tanımlıysa webhook gönderir.

    Slack / Teams gibi sistemler için kullanılabilir.
    """

    webhook_url = os.environ.get(
        "ALERT_WEBHOOK_URL",
        "",
    ).strip()

    if not webhook_url:
        return

    payload = {
        "text": (
            f"🚨 Pipeline Hatası\n\n"
            f"Job: {alert_data['job_name']}\n"
            f"Step: {alert_data['step_name']}\n"
            f"Hata: {alert_data['error']}\n"
            f"Zaman: {alert_data['timestamp']}"
        )
    }

    try:
        response = requests.post(
            webhook_url,
            json=payload,
            timeout=10,
        )

        response.raise_for_status()

    except Exception as exc:
        # Webhook hatası pipeline'ın kendisini tekrar bozmasın.
        print(
            f"Webhook gönderilemedi: {exc}"
        )


# ---------------------------------------------------------------------------
# Dagster Failure Hook
# ---------------------------------------------------------------------------

@failure_hook
def alert_on_failure(context: HookContext):
    """
    Dagster job'ındaki herhangi bir step başarısız olduğunda çalışır.
    """

    job_name = context.job_name

    try:
        step_name = context.op.name
    except Exception:
        step_name = "unknown"

    error_msg = context.op_exception

    if error_msg is None:
        error_msg = "Bilinmeyen hata"

    error_text = str(error_msg)

    timestamp = datetime.now(
        timezone.utc
    ).isoformat()

    alert_data = {
        "timestamp": timestamp,
        "job_name": job_name,
        "step_name": step_name,
        "error": error_text,
        "status": "FAILURE",
        "run_id": context.run_id,
    }

    # -----------------------------------------------------------------------
    # Dagster log
    # -----------------------------------------------------------------------

    context.log.error(
        f"HATA ALINDI! "
        f"Job: {job_name}, "
        f"Step: {step_name}. "
        f"Hata: {error_text}"
    )

    # -----------------------------------------------------------------------
    # Dashboard'ın okuyacağı alert dosyasına kaydet
    # -----------------------------------------------------------------------

    try:
        _save_alert(alert_data)

        context.log.info(
            f"Alert kaydedildi: {ALERT_FILE}"
        )

    except Exception as exc:
        context.log.error(
            f"Alert dosyasına yazılamadı: {exc}"
        )

    # -----------------------------------------------------------------------
    # Opsiyonel webhook
    # -----------------------------------------------------------------------

    _send_webhook(alert_data)


# ---------------------------------------------------------------------------
# Dagster Success Hook
# ---------------------------------------------------------------------------

@success_hook
def clear_alert_on_success(context: HookContext):
    """
    Dagster job'ındaki bir step başarılı olduğunda çalışır.
    Eğer bu step için daha önce alert.json'a 'FAILURE' yazılmışsa,
    durumu 'RESOLVED' olarak günceller.
    """
    job_name = context.job_name

    try:
        step_name = context.op.name
    except Exception:
        step_name = "unknown"

    if not ALERT_FILE.exists():
        return

    try:
        existing_alerts = json.loads(ALERT_FILE.read_text(encoding="utf-8"))
        if not isinstance(existing_alerts, list):
            return

        is_updated = False

        for alert in existing_alerts:
            if (alert.get("job_name") == job_name and 
                alert.get("step_name") == step_name and 
                alert.get("status") == "FAILURE"):
                
                alert["status"] = "RESOLVED"
                alert["resolved_at"] = datetime.now(timezone.utc).isoformat()
                is_updated = True

        if is_updated:
            ALERT_FILE.write_text(
                json.dumps(existing_alerts, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            context.log.info(f"Geçmiş hata çözüldü olarak işaretlendi: Job={job_name}, Step={step_name}")

    except Exception as exc:
        context.log.error(f"Alert durumu güncellenirken hata oluştu: {exc}")