from dagster import failure_hook, HookContext
import requests

@failure_hook
def alert_on_failure(context: HookContext):
    job_name = context.job_name
    step_name = context.op.name
    error_msg = context.op_exception
    
    # Dagster loglarına hata mesajını basıyoruz
    context.log.error(f"HATA ALINDI! Job: {job_name}, Step: {step_name}. Hata: {error_msg}")
    
    # Örnek Webhook bildirimi (Slack, Teams vb. için burayı aktif edebilirsin)
    # webhook_url = "https://hooks.slack.com/services/T000.../..."
    # payload = {"text": f"🚨 Pipeline Hatası: {job_name} işinde {step_name} adımı başarısız oldu."}
    # requests.post(webhook_url, json=payload)