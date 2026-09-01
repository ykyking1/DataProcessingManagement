"""Dagster failure notifications with best-effort automatic recovery.

A failed step is logged, appended to an alert registry kept as a single JSON
object in MinIO, and optionally sent to a webhook. In addition, the system
tries to recover on its own: it re-executes the failed run "from failure"
through the Dagster GraphQL API up to ``AUTO_FIX_MAX_ATTEMPTS`` times. If one
attempt succeeds, ``clear_alert_on_success`` marks the alert RESOLVED and this
module also records which attempt fixed it (``auto_fix_resolved_attempt``); if
every attempt fails the alert is stamped ``auto_fix_exhausted``. The Streamlit
dashboard reads these fields to show the recovery status.

Why the retry loop runs in a *detached OS process* and not a thread:
``failure_hook`` executes synchronously inside the executor's step subprocess,
and the run only turns FAILED after that subprocess has fully exited. Dagster
refuses "re-execute from failure" until the run is actually FAILED, so the
loop has to wait for that from outside the hook. A background *thread* is not
enough either: the step subprocess cannot exit while a non-daemon thread is
alive, which deadlocks the executor and leaves the run stuck in STARTED. So
the loop is spawned as a fully detached process (see
``_launch_auto_fix_worker_process``) that polls over GraphQL and launches the
re-executions.

Set ``DAGSTER_AUTOFIX_DISABLED=1`` to turn automatic recovery off (alerts and
webhooks still fire). The same MinIO object is read by the Streamlit dashboard
("🚨 Alertler" tab); bucket / object are configurable with MINIO_ALERTS_BUCKET
/ MINIO_ALERTS_OBJECT, and the MinIO connection settings are the ones
assets.create_minio_client() reads (MINIO_ENDPOINT, MINIO_ACCESS_KEY, ...).
"""

import io
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

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
    #
    # run_id başına EN FAZLA BİR kayıt: aynı run için (ör. bir run'da
    # birden çok op başarısız olursa) hook tekrar çalışırsa yeni satır
    # eklenmez, mevcut satır güncellenir.
    alerts = _load_alerts()
    run_id = alert_data.get("run_id")
    if run_id:
        for existing in alerts:
            if existing.get("run_id") == run_id:
                existing.update(alert_data)
                _store_alerts(alerts[-MAX_ALERTS:])
                return
    alerts.append(alert_data)
    _store_alerts(alerts[-MAX_ALERTS:])


def _update_alert(run_id: str, updates: dict) -> bool:
    """Patch every stored alert whose ``run_id`` matches.

    Deliberately does NOT filter on ``status == "FAILURE"``: when an auto-fix
    attempt ends in SUCCESS, that run's own ``clear_alert_on_success`` (a
    separate process) usually flips the alert to RESOLVED before
    ``_auto_fix_failure`` gets here, and a status filter would then silently
    drop the ``auto_fix_resolved_attempt`` write -- losing the "fixed
    automatically vs. by hand" distinction on the dashboard.
    """

    try:
        alerts = _load_alerts()
    except Exception as error:
        _log(f"alert kaydı okunamadı: {error}")
        return False

    if not alerts:
        return False

    changed = False
    for alert in alerts:
        if alert.get("run_id") == run_id:
            alert.update(updates)
            changed = True

    if changed:
        try:
            _store_alerts(alerts)
        except Exception as error:
            _log(f"alert kaydı yazılamadı: {error}")
            return False

    return changed


def _mark_auto_resolved(
    original_run_id: str, attempt: int, resolved_run_id: str
) -> None:
    """Stamp the alert as auto-resolved, surviving a racing success-hook write.

    ``_auto_fix_failure`` (this detached process) and ``clear_alert_on_success``
    (the successful re-execution's own hook, a *separate* process) both do an
    unlocked read-modify-write on the single ``alerts.json`` object. A plain
    write here can be clobbered by the other side, which is exactly why the
    dashboard sometimes shows "elle çözüldü" for an auto-fixed error. So we
    re-read and re-apply until the ``auto_fix_resolved_attempt`` marker sticks.
    """

    updates = {
        "status": "RESOLVED",
        "resolved_at": datetime.now(timezone.utc).isoformat(),
        "auto_fix_resolved_attempt": attempt,
        "auto_fix_resolved_run_id": resolved_run_id,
    }
    for _ in range(4):
        _update_alert(original_run_id, updates)
        try:
            alerts = _load_alerts()
        except Exception:
            return
        stuck = any(
            alert.get("run_id") == original_run_id
            and alert.get("auto_fix_resolved_attempt") == attempt
            for alert in alerts
        )
        if stuck:
            return
        time.sleep(2)
    _log(
        "uyarı: auto_fix_resolved_attempt işareti kalıcı yazılamadı "
        "(eşzamanlı yazım yarışı olabilir)."
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


# ---------------------------------------------------------------------------
# Automatic recovery (re-execute from failure via the Dagster GraphQL API)
# ---------------------------------------------------------------------------

AUTO_FIX_MAX_ATTEMPTS = int(os.getenv("DAGSTER_AUTOFIX_MAX_ATTEMPTS", "3"))
AUTO_FIX_POLL_INTERVAL_SECONDS = int(
    os.getenv("DAGSTER_AUTOFIX_POLL_INTERVAL_SECONDS", "3")
)
# The full pipeline (Spark -> GE -> ClickHouse -> DVC) can run for a while, so
# a single attempt is given a generous ceiling before it is counted as failed.
AUTO_FIX_RUN_TIMEOUT_SECONDS = int(
    os.getenv("DAGSTER_AUTOFIX_RUN_TIMEOUT_SECONDS", "1800")
)
# Pause between attempts so a transient dependency (ClickHouse / Postgres /
# MinIO) has a chance to recover before the next re-execution is launched.
AUTO_FIX_RETRY_DELAY_SECONDS = int(
    os.getenv("DAGSTER_AUTOFIX_RETRY_DELAY_SECONDS", "15")
)

# A re-execution attempt carries this tag; alert_on_failure will NOT start a
# fresh auto-fix loop for a run that has it (that would fan out into an
# exponential retry storm). The whole loop lives in one detached process.
AUTO_FIX_ATTEMPT_TAG = "auto_fix_attempt"
AUTO_FIX_ROOT_RUN_TAG = "auto_fix_root_run_id"

_TERMINAL_RUN_STATUSES = {"SUCCESS", "FAILURE", "CANCELED"}


def _autofix_disabled() -> bool:
    return os.getenv("DAGSTER_AUTOFIX_DISABLED", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _get_graphql_url() -> str:
    return os.getenv("DAGSTER_GRAPHQL_URL", "http://localhost:3000/graphql")


def _run_graphql(query: str, variables: dict) -> dict:
    """POST a GraphQL request and return its ``data`` payload.

    Dagster's webserver returns HTTP 500 even when a resolver hands back a
    perfectly usable typed error (e.g. ``launchRunReexecution`` -> PythonError),
    so ``raise_for_status()`` is only consulted when the body is not usable
    JSON with a ``data`` key.
    """

    response = requests.post(
        _get_graphql_url(),
        json={"query": query, "variables": variables},
        timeout=10,
    )

    try:
        payload = response.json()
    except ValueError:
        response.raise_for_status()
        raise RuntimeError(
            f"GraphQL'den JSON olmayan yanıt alındı (HTTP {response.status_code})"
        )

    if payload.get("errors"):
        raise RuntimeError(
            payload["errors"][0].get("message", "GraphQL hatası")
        )

    if payload.get("data") is None:
        response.raise_for_status()
        raise RuntimeError(
            f"GraphQL'den veri alınamadı (HTTP {response.status_code})"
        )

    return payload["data"]


_LAUNCH_RUN_REEXECUTION_MUTATION = """
mutation LaunchRunReexecution(
    $parentRunId: String!,
    $extraTags: [ExecutionTag!]!,
) {
  launchRunReexecution(
    reexecutionParams: {
      parentRunId: $parentRunId,
      strategy: FROM_FAILURE,
      extraTags: $extraTags,
    }
  ) {
    __typename
    ... on LaunchRunSuccess {
      run { runId }
    }
    ... on RunConfigValidationInvalid {
      errors { message }
    }
    ... on PipelineNotFoundError { message }
    ... on RunConflict { message }
    ... on UnauthorizedError { message }
    ... on ConflictingExecutionParamsError { message }
    ... on PythonError { message }
  }
}
"""


_RUN_STATUS_QUERY = """
query RunStatus($runId: ID!) {
  runOrError(runId: $runId) {
    __typename
    ... on Run { id status }
    ... on RunNotFoundError { message }
    ... on PythonError { message }
  }
}
"""


def _launch_run_reexecution(parent_run_id: str, extra_tags: list) -> str:
    """Re-execute ``parent_run_id`` from failure; return the new run id."""

    data = _run_graphql(
        _LAUNCH_RUN_REEXECUTION_MUTATION,
        {"parentRunId": parent_run_id, "extraTags": extra_tags},
    )

    result = data["launchRunReexecution"]
    typename = result["__typename"]

    if typename == "LaunchRunSuccess":
        return result["run"]["runId"]

    if typename == "RunConfigValidationInvalid":
        errors = result.get("errors") or []
        raise RuntimeError(
            "; ".join(error.get("message", "") for error in errors)
            or "Run config geçersiz"
        )

    raise RuntimeError(
        result.get("message") or f"Re-execute başlatılamadı ({typename})"
    )


def _wait_for_run_completion(
    run_id: str,
    timeout_seconds: int = AUTO_FIX_RUN_TIMEOUT_SECONDS,
    poll_interval: int = AUTO_FIX_POLL_INTERVAL_SECONDS,
) -> str:
    """Poll until the run reaches a terminal status; return it (or "TIMEOUT")."""

    deadline = time.time() + timeout_seconds

    while True:
        try:
            data = _run_graphql(_RUN_STATUS_QUERY, {"runId": run_id})
            run_result = data["runOrError"]
            if run_result["__typename"] == "Run":
                status = run_result["status"]
                if status in _TERMINAL_RUN_STATUSES:
                    return status
        except Exception:
            # Transient GraphQL / network errors must not break the loop.
            pass

        if time.time() >= deadline:
            return "TIMEOUT"

        time.sleep(poll_interval)


def _log(message: str) -> None:
    # Runs in a detached process, so context.log is unavailable. Goes to the
    # worker process' own console, not the Dagster compute log.
    print(f"[auto_fix] {message}", flush=True)


def _auto_fix_failure(original_run_id: str) -> None:
    """Re-execute the failed run "from failure" up to AUTO_FIX_MAX_ATTEMPTS times.

    Waits for the run itself to reach FAILURE/CANCELED first (Dagster forbids
    re-execute-from-failure otherwise). A SUCCESS attempt exits early and
    records ``auto_fix_resolved_attempt``; if the launch call itself errors it
    still counts as a failed attempt and the loop continues. When every
    attempt fails the alert is stamped ``auto_fix_exhausted``.
    """

    _log(
        f"run'ın FAILURE/CANCELED durumuna geçmesi bekleniyor "
        f"(run={original_run_id})."
    )
    root_status = _wait_for_run_completion(original_run_id)

    if root_status not in ("FAILURE", "CANCELED"):
        _log(
            f"run beklenen sürede FAILURE/CANCELED durumuna geçmedi "
            f"(son durum={root_status}); otomatik düzeltme iptal edildi."
        )
        _update_alert(
            original_run_id,
            {
                "auto_fix_exhausted": True,
                "auto_fix_attempts": 0,
                "auto_fix_error": (
                    "Run, re-execute edilebilecek bir FAILURE/CANCELED "
                    f"durumuna geçmedi (son durum: {root_status})."
                ),
            },
        )
        return

    current_run_id = original_run_id
    last_error = None

    for attempt in range(1, AUTO_FIX_MAX_ATTEMPTS + 1):
        if attempt > 1:
            _log(
                f"bir sonraki denemeden önce {AUTO_FIX_RETRY_DELAY_SECONDS} "
                "saniye bekleniyor."
            )
            time.sleep(AUTO_FIX_RETRY_DELAY_SECONDS)

        _log(
            f"deneme {attempt}/{AUTO_FIX_MAX_ATTEMPTS} başlatılıyor "
            f"(run={current_run_id})."
        )
        _update_alert(original_run_id, {"auto_fix_current_attempt": attempt})

        try:
            new_run_id = _launch_run_reexecution(
                current_run_id,
                extra_tags=[
                    {"key": AUTO_FIX_ATTEMPT_TAG, "value": str(attempt)},
                    {"key": AUTO_FIX_ROOT_RUN_TAG, "value": original_run_id},
                ],
            )
        except Exception as exc:
            _log(f"deneme {attempt} başlatılamadı: {exc}")
            last_error = str(exc)
            continue

        status = _wait_for_run_completion(new_run_id)
        _log(f"deneme {attempt} sonucu = {status} (run={new_run_id}).")

        if status == "SUCCESS":
            _mark_auto_resolved(original_run_id, attempt, new_run_id)
            _log(
                f"sorun {attempt}. denemede çözüldü, kalan denemeler "
                "yapılmayacak."
            )
            return

        current_run_id = new_run_id
        last_error = None

    _log(f"{AUTO_FIX_MAX_ATTEMPTS} deneme de başarısız oldu.")
    updates = {
        "auto_fix_exhausted": True,
        "auto_fix_attempts": AUTO_FIX_MAX_ATTEMPTS,
        "auto_fix_last_run_id": current_run_id,
    }
    if last_error:
        updates["auto_fix_error"] = last_error
    _update_alert(original_run_id, updates)


def _auto_fix_failure_worker(original_run_id: str) -> None:
    """Entry point for the detached worker process; never lets an error vanish."""

    try:
        _auto_fix_failure(original_run_id)
    except Exception as exc:
        _log(f"beklenmeyen hata: {exc}")


def _launch_auto_fix_worker_process(run_id: str) -> None:
    """Spawn ``python alerting.py --auto-fix-run <run_id>`` fully detached.

    The retry loop must be independent of the failed step's own subprocess
    (see the module docstring), so this returns immediately without waiting on
    the child. On Windows DETACHED_PROCESS + CREATE_NEW_PROCESS_GROUP cut the
    child from the parent's process group; POSIX uses start_new_session.
    """

    creationflags = 0
    start_new_session = False
    if os.name == "nt":
        creationflags = (
            subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
        )
    else:
        start_new_session = True

    module_path = str(Path(__file__).resolve())
    subprocess.Popen(
        [sys.executable, module_path, "--auto-fix-run", run_id],
        cwd=str(Path(module_path).parent),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=creationflags,
        start_new_session=start_new_session,
        close_fds=True,
    )


@failure_hook
def alert_on_failure(context: HookContext) -> None:
    """Record ONE alert per original failure and kick off automatic recovery.

    Re-execution attempts launched by the auto-fix loop carry
    ``AUTO_FIX_ATTEMPT_TAG``. When one of those attempts fails again this hook
    only logs it and updates the Postgres run row -- it does NOT append a
    second alert record, re-send the webhook, or start a nested auto-fix loop.
    The single alert for the original run is updated in place by
    ``_auto_fix_failure`` (auto_fix_current_attempt / auto_fix_error /
    auto_fix_exhausted / ...), so the dashboard keeps exactly one entry per
    original run_id.
    """

    job_name = context.job_name
    try:
        step_name = context.op.name
    except Exception:
        step_name = "unknown"

    error_text = str(context.op_exception or "Bilinmeyen hata")

    try:
        run = context.instance.get_run_by_id(context.run_id)
        run_tags = (run.tags or {}) if run else {}
    except Exception as error:
        context.log.error("Run tag'leri okunamadı: %s", error)
        run_tags = {}
    is_auto_fix_attempt = bool(run_tags.get(AUTO_FIX_ATTEMPT_TAG))
    root_run_id = run_tags.get(AUTO_FIX_ROOT_RUN_TAG)

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

    if is_auto_fix_attempt:
        # Otomatik düzeltme döngüsünün başlattığı bir re-execution
        # denemesi başarısız oldu. Orijinal run için zaten bir alert var
        # ve onu _auto_fix_failure güncelliyor -- burada İKİNCİ bir kayıt
        # OLUŞTURMA, webhook'u tekrar gönderme, yeni bir döngü başlatma.
        context.log.info(
            "Otomatik düzeltme denemesi başarısız (kök run=%s); ayrı bir "
            "alert kaydı oluşturulmadı.",
            root_run_id or "?",
        )
        return

    alert_data = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "job_name": job_name,
        "step_name": step_name,
        "error": error_text,
        "status": "FAILURE",
        "run_id": context.run_id,
    }
    try:
        _save_alert(alert_data)
        context.log.info("Alert kaydedildi: %s", ALERTS_URI)
    except Exception as error:
        context.log.error("Alert MinIO'ya yazılamadı (%s): %s", ALERTS_URI, error)

    _send_webhook(alert_data)

    if _autofix_disabled():
        context.log.info(
            "Otomatik düzeltme DAGSTER_AUTOFIX_DISABLED ile kapalı; "
            "yeniden çalıştırma kullanıcıya bırakıldı."
        )
        return

    try:
        _launch_auto_fix_worker_process(context.run_id)
        context.log.info(
            "Otomatik düzeltme ayrı bir süreçte başlatıldı (en fazla %s "
            "deneme).",
            AUTO_FIX_MAX_ATTEMPTS,
        )
    except Exception as error:
        context.log.error("Otomatik düzeltme başlatılamadı: %s", error)


@success_hook
def clear_alert_on_success(context: HookContext) -> None:
    """Resolve ancestor-run alerts after a successful (manual or auto) re-execution."""

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
            matches_retry = (
                alert.get("job_name") == job_name
                and alert.get("step_name") == step_name
                and alert.get("status") == "FAILURE"
                and alert.get("run_id") in ancestor_run_ids
            )
            if matches_retry:
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


# ---------------------------------------------------------------------------
# Detached auto-fix worker entry point
# ---------------------------------------------------------------------------
#
# _launch_auto_fix_worker_process starts this file as
# ``python alerting.py --auto-fix-run <run_id>`` in a fully detached OS process
# (see that function and the module docstring). This block only runs when the
# file is executed as a script, not when imported as a Dagster code location.

if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "--auto-fix-run":
        try:
            from dotenv import load_dotenv

            load_dotenv(Path(__file__).resolve().parent / ".env")
        except Exception:
            pass
        _auto_fix_failure_worker(sys.argv[2])
