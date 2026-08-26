"""
Dagster Alerting Sistemi

Pipeline içerisindeki herhangi bir step başarısız olduğunda:

1. Dagster loglarına hata yazılır.
2. data/alerts/alerts.json dosyasına hata kaydedilir.
3. İstenirse WEBHOOK_URL üzerinden Slack / Teams vb. sistemlere bildirim
   gönderilebilir.
4. Sistem KULLANICIDAN BAĞIMSIZ olarak, hatalı run'ı Dagster GraphQL API
   üzerinden "re-execute from failure" ile otomatik olarak en fazla
   AUTO_FIX_MAX_ATTEMPTS kez art arda yeniden çalıştırır (bkz.
   _auto_fix_failure). Denemelerden biri başarılı olursa kalan denemeler
   yapılmaz (clear_alert_on_success zaten alert'i RESOLVED işaretler; bu
   modül ayrıca hangi denemede çözüldüğünü alert kaydına ekler). Üç
   deneme de başarısız olursa alert kaydı "auto_fix_exhausted" ile
   işaretlenir -- dashboard bunu görüp bilgi mesajı gösterir.

   ÖNEMLİ (neden ayrı bir SÜREÇTE (process) çalışıyor, thread'de DEĞİL):
   failure_hook, Dagster'ın run'ı yürüten executor döngüsünün TAM
   İÇİNDE, senkron olarak çalışır (bkz. execute_plan.py::_trigger_hook).
   Run'ın kendi durumu (Run.status) ise ancak TÜM adımlar (ve onların
   hook'ları) bittikten SONRA, executor döngüsü tamamlanınca FAILURE'a
   döner. Yani hook'un içinde bloklayıp "run FAILURE olsun" diye beklemek
   KENDİ KENDİNİ KİLİTLEYEN bir duruma yol açar ("Cannot reexecute from
   failure a run that is not failed or canceled" hatası tam olarak
   buradan geliyordu).

   İlk düzeltmede işi ayrı bir THREAD'e taşımak yeterli SANILDI, ama
   YETERSİZ çıktı: adımın kendisi zaten Dagster'ın multiprocess
   executor'ı tarafından AYRI BİR ALT SÜREÇTE (subprocess) çalıştırılıyor
   ve executor, run'ı sonlandırmadan ÖNCE o alt sürecin TAMAMEN
   kapanmasını (process exit) bekliyor. Alt sürecin içinde yaşayan
   non-daemon bir thread (run'ın FAILURE olmasını bekleyen auto-fix
   döngüsü), Python'un o alt süreci kapatmasını engelliyor -- bu da
   executor'ın hâlâ "kapanmayı bekliyorum" durumunda kalıp run'ı asla
   FAILURE'a çeviremediği YENİ bir kilitlenmeye yol açtı (gözlemlenen
   belirti: run, tüm adımları bitmiş olsa bile Dagster UI'da süresiz
   "STARTED" görünüyordu). Bu yüzden asıl deneme döngüsü (_auto_fix_
   failure), adımın alt sürecinden TAMAMEN BAĞIMSIZ, ayrıca başlatılan
   (detached) bir OS sürecinde çalıştırılır -- bkz. _launch_auto_fix_
   worker_process. Böylece adımın alt süreci hemen kapanabilir, executor
   run'ı normal şekilde bitirip FAILURE'a çevirebilir; bağımsız süreç de
   bunu GraphQL üzerinden bekleyip yeniden çalıştırmayı gerçekten
   başlatabilir.

Webhook kullanımı için:

Windows PowerShell:

$env:ALERT_WEBHOOK_URL="https://..."

Webhook kullanmak istemiyorsan bu değişkeni boş bırakabilirsin.
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import os
import subprocess
import sys
import time

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


def _update_alert(run_id: str, updates: dict) -> bool:
    """
    run_id'si eşleşen alert kaydını `updates` ile günceller.
    auto_fix_failure'ın deneme sonucunu (çözüldü / tükendi) orijinal
    alert satırına yazmak için kullanılır.

    NOT: SADECE run_id'ye göre eşleştirir, "status == FAILURE" şartı
    ARANMAZ. Önceden bu şart vardı, ama şu YARIŞ DURUMUNA (race
    condition) yol açıyordu: bir deneme SUCCESS ile bittiğinde,
    _auto_fix_failure "auto_fix_resolved_attempt" yazmaya çalışırken,
    o SUCCESS run'ının KENDİ success_hook'u (clear_alert_on_success --
    TAMAMEN AYRI bir süreçte, o run'ın kendi işlemi olarak) genelde
    daha önce davranıp alert'i zaten "RESOLVED"e çeviriyordu; bu
    durumda eski "status == FAILURE" şartı eşleşmeyip
    "auto_fix_resolved_attempt" YAZILAMIYORDU -- deneme aslında BAŞARILI
    olduğu halde dashboard'da "otomatik denemeyle mi yoksa elle mi
    çözüldü" bilgisi kayboluyordu.
    """

    if not ALERT_FILE.exists():
        return False

    try:
        existing_alerts = json.loads(
            ALERT_FILE.read_text(encoding="utf-8")
        )

        if not isinstance(existing_alerts, list):
            return False

    except (json.JSONDecodeError, OSError):
        return False

    is_updated = False

    for alert in existing_alerts:

        if alert.get("run_id") == run_id:
            alert.update(updates)
            is_updated = True

    if is_updated:
        ALERT_FILE.write_text(
            json.dumps(
                existing_alerts,
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    return is_updated


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
# Otomatik Düzeltme (Re-execute From Failure)
# ---------------------------------------------------------------------------
#
# Kullanıcının Dagster UI'a gidip elle "Re-execute from failure"a
# basmasına gerek kalmadan, hatalı run'ı Dagster'ın kendi GraphQL API'si
# üzerinden otomatik olarak yeniden çalıştırır. dashboard/app.py'nin
# kullandığı DAGSTER_GRAPHQL_URL ile aynı varsayılanı kullanır (Dagster
# webserver, varsayılan olarak localhost:3000'de GraphQL sunar).

AUTO_FIX_MAX_ATTEMPTS = 3
AUTO_FIX_POLL_INTERVAL_SECONDS = 3

AUTO_FIX_RUN_TIMEOUT_SECONDS = int(
    os.environ.get(
        "DAGSTER_AUTOFIX_RUN_TIMEOUT_SECONDS",
        "300",
    )
)

# Bir deneme bitip (SUCCESS/FAILURE/TIMEOUT) bir sonraki deneme
# başlamadan önce beklenen süre -- art arda hiç ara vermeden 3 run
# başlatmak yerine, altyapıya (ör. ClickHouse/Postgres bağlantısı gibi
# geçici bir sorunun kendi kendine düzelmesine) biraz zaman tanır.
AUTO_FIX_RETRY_DELAY_SECONDS = int(
    os.environ.get(
        "DAGSTER_AUTOFIX_RETRY_DELAY_SECONDS",
        "5",
    )
)

# Bir reexecution denemesi bu tag ile işaretlenir -- alert_on_failure bu
# tag'i taşıyan bir run'ın başarısızlığında YENİ bir otomatik düzeltme
# döngüsü BAŞLATMAZ (bu, kendi kendini tetikleyen sonsuz/üstel bir
# retry zincirine yol açardı). Döngü tamamen TEK bir hook çağrısı
# içinde, senkron olarak yürütülür (bkz. _auto_fix_failure).
AUTO_FIX_ATTEMPT_TAG = "auto_fix_attempt"
AUTO_FIX_ROOT_RUN_TAG = "auto_fix_root_run_id"

_TERMINAL_RUN_STATUSES = {
    "SUCCESS",
    "FAILURE",
    "CANCELED",
}


def _get_graphql_url() -> str:
    return os.environ.get(
        "DAGSTER_GRAPHQL_URL",
        "http://localhost:3000/graphql",
    )


def _run_graphql(query: str, variables: dict) -> dict:
    """
    NOT: Dagster'ın webserver'ı, bir GraphQL resolver'ı İÇERİDE
    yakalanabilir bir hata fırlattığında (ör. launchRunReexecution'ın
    kendi PythonError union üyesiyle döndürdüğü, yani ASLINDA bizim
    normal şekilde ele aldığımız bir durum) HTTP durum kodunu YİNE DE
    500 olarak döner -- gövde (body) tamamen geçerli ve kullanılabilir
    bir GraphQL yanıtı olsa bile. Bu yüzden `raise_for_status()` DAHA
    ÖNCE çağrılmıyor: önce gövdenin JSON olarak ayrıştırılabilir ve
    kullanılabilir bir "data" içerip içermediğine bakılıyor -- öyleyse
    HTTP durum kodundan bağımsız olarak döndürülüyor ve üst tipli
    hata (__typename) çağıran fonksiyon (ör. _launch_run_reexecution)
    tarafından normal şekilde yorumlanıyor. Yalnızca gövde hiç JSON
    değilse ya da "data" içermiyorsa asıl HTTP hatası fırlatılır.
    """

    response = requests.post(
        _get_graphql_url(),
        json={
            "query": query,
            "variables": variables,
        },
        timeout=10,
    )

    try:
        payload = response.json()
    except ValueError:
        response.raise_for_status()
        raise RuntimeError(
            f"GraphQL'den JSON olmayan yanıt alındı "
            f"(HTTP {response.status_code})"
        )

    if payload.get("errors"):
        raise RuntimeError(
            payload["errors"][0].get(
                "message",
                "GraphQL hatası",
            )
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
      run {
        runId
      }
    }

    ... on RunConfigValidationInvalid {
      errors {
        message
      }
    }

    ... on PipelineNotFoundError {
      message
    }

    ... on RunConflict {
      message
    }

    ... on UnauthorizedError {
      message
    }

    ... on ConflictingExecutionParamsError {
      message
    }

    ... on PythonError {
      message
    }
  }
}
"""


_RUN_STATUS_QUERY = """
query RunStatus($runId: ID!) {
  runOrError(runId: $runId) {
    __typename

    ... on Run {
      id
      status
    }

    ... on RunNotFoundError {
      message
    }

    ... on PythonError {
      message
    }
  }
}
"""


def _launch_run_reexecution(parent_run_id: str, extra_tags: list) -> str:
    """
    Verilen run'ı "re-execute from failure" ile yeniden başlatır ve
    yeni run'ın id'sini döner. Başarısız olursa RuntimeError fırlatır.
    """

    data = _run_graphql(
        _LAUNCH_RUN_REEXECUTION_MUTATION,
        {
            "parentRunId": parent_run_id,
            "extraTags": extra_tags,
        },
    )

    result = data["launchRunReexecution"]
    typename = result["__typename"]

    if typename == "LaunchRunSuccess":
        return result["run"]["runId"]

    if typename == "RunConfigValidationInvalid":

        errors = result.get("errors") or []

        raise RuntimeError(
            "; ".join(
                error.get("message", "")
                for error in errors
            )
            or "Run config geçersiz"
        )

    raise RuntimeError(
        result.get("message")
        or f"Re-execute başlatılamadı ({typename})"
    )


def _wait_for_run_completion(
    run_id: str,
    timeout_seconds: int = AUTO_FIX_RUN_TIMEOUT_SECONDS,
    poll_interval: int = AUTO_FIX_POLL_INTERVAL_SECONDS,
) -> str:
    """
    Run, SUCCESS/FAILURE/CANCELED gibi nihai bir duruma ulaşana kadar
    periyodik olarak GraphQL'den durumunu sorgular ve o durumu döner.
    Zaman aşımına uğrarsa "TIMEOUT" döner (istisna fırlatmaz -- otomatik
    düzeltme döngüsü bunu bir başarısız deneme olarak sayıp bir sonraki
    denemeye geçer).
    """

    deadline = time.time() + timeout_seconds

    while True:

        try:
            data = _run_graphql(
                _RUN_STATUS_QUERY,
                {
                    "runId": run_id
                },
            )

            run_result = data["runOrError"]

            if run_result["__typename"] == "Run":

                status = run_result["status"]

                if status in _TERMINAL_RUN_STATUSES:
                    return status

        except Exception:
            # Geçici bir GraphQL/ağ hatası döngüyü tamamen bozmasın --
            # deadline'a kadar tekrar denenir.
            pass

        if time.time() >= deadline:
            return "TIMEOUT"

        time.sleep(poll_interval)


def _log(message: str) -> None:
    # Arka plan thread'inde çalıştığı için context.log KULLANILAMAZ (o,
    # hook'un senkron çağrı bağlamına bağlıdır ve hook çoktan dönmüş
    # olabilir). Bu satırlar Dagster run'ının stdout'una (compute log)
    # değil, kod konumu (code location) sürecinin kendi konsoluna yazılır.
    print(f"[auto_fix] {message}")


def _auto_fix_failure(original_run_id: str) -> None:
    """
    Başarısız olan run'ı TAM OLARAK AUTO_FIX_MAX_ATTEMPTS kez art arda
    "re-execute from failure" ile otomatik olarak yeniden çalıştırmayı
    dener. Kullanıcının herhangi bir işlem yapmasına gerek yoktur.

    ÖNEMLİ: Bu fonksiyon önce run'ın KENDİSİNİN FAILURE/CANCELED
    durumuna geçmesini bekler -- Dagster, bir run FAILURE'a dönmeden
    "re-execute from failure" yapılmasına izin vermez. Bu bekleme,
    modülün başındaki "ÖNEMLİ" notunda açıklanan sebepten dolayı,
    hook'u tetikleyen senkron çağrı bağlamının DIŞINDA (ayrı bir
    thread'de) yapılmalıdır -- bkz. alert_on_failure.

    Bir deneme SUCCESS ile biterse: clear_alert_on_success (parent_run_id
    zincirini takip ederek) orijinal alert'i zaten RESOLVED işaretler; bu
    fonksiyon ayrıca kaçıncı denemede çözüldüğünü alert kaydına ekler ve
    kalan denemeleri yapmaz.

    Bir denemenin BAŞLATILMASI (launchRunReexecution çağrısı) hata
    verirse -- ör. Dagster webserver'a geçici olarak ulaşılamaması gibi
    reexecution'ın kendisiyle ilgisiz bir sorun -- bu da başarısız bir
    deneme olarak sayılır ve döngü DURMAZ, kalan denemelere devam
    edilir; sadece SUCCESS erken çıkışa neden olur. Denemeler arasında
    AUTO_FIX_RETRY_DELAY_SECONDS kadar beklenir (yeni run başlatılan
    sistemin/servisin toparlanmasına biraz zaman tanımak için).

    Üç deneme de başarısız/zaman aşımı olursa: orijinal alert kaydı
    "auto_fix_exhausted" ile işaretlenir -- dashboard bunu görüp "3 kere
    başarısız olundu" bilgisini gösterir.
    """

    _log(
        f"run'ın kendisinin FAILURE/CANCELED durumuna geçmesi "
        f"bekleniyor (run={original_run_id})."
    )

    root_status = _wait_for_run_completion(original_run_id)

    if root_status not in ("FAILURE", "CANCELED"):

        _log(
            f"run beklenen sürede FAILURE/CANCELED durumuna geçmedi "
            f"(son durum={root_status}); otomatik düzeltme iptal "
            "edildi."
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
                f"bir sonraki denemeden önce "
                f"{AUTO_FIX_RETRY_DELAY_SECONDS} saniye bekleniyor."
            )

            time.sleep(AUTO_FIX_RETRY_DELAY_SECONDS)

        _log(
            f"deneme {attempt}/{AUTO_FIX_MAX_ATTEMPTS} başlatılıyor "
            f"(run={current_run_id})."
        )

        # Dashboard'ın "şu an kaçıncı denemede" gösterebilmesi için,
        # deneme başlamadan önce alert kaydına yazılır.
        _update_alert(
            original_run_id,
            {
                "auto_fix_current_attempt": attempt,
            },
        )

        try:
            new_run_id = _launch_run_reexecution(
                current_run_id,
                extra_tags=[
                    {
                        "key": AUTO_FIX_ATTEMPT_TAG,
                        "value": str(attempt),
                    },
                    {
                        "key": AUTO_FIX_ROOT_RUN_TAG,
                        "value": original_run_id,
                    },
                ],
            )

        except Exception as exc:

            _log(
                f"deneme {attempt} başlatılamadı: {exc}"
            )

            # Yeni bir run oluşturulamadı -- bir sonraki denemede aynı
            # run'dan tekrar re-execute etmeyi dener (current_run_id
            # DEĞİŞMEZ).
            last_error = str(exc)
            continue

        status = _wait_for_run_completion(new_run_id)

        _log(
            f"deneme {attempt} sonucu = {status} (run={new_run_id})."
        )

        if status == "SUCCESS":

            _update_alert(
                original_run_id,
                {
                    "auto_fix_resolved_attempt": attempt,
                    "auto_fix_resolved_run_id": new_run_id,
                },
            )

            _log(
                f"sorun {attempt}. denemede çözüldü, kalan denemeler "
                "yapılmayacak."
            )

            return

        current_run_id = new_run_id
        last_error = None

    _log(
        f"{AUTO_FIX_MAX_ATTEMPTS} deneme de başarısız oldu."
    )

    updates = {
        "auto_fix_exhausted": True,
        "auto_fix_attempts": AUTO_FIX_MAX_ATTEMPTS,
        "auto_fix_last_run_id": current_run_id,
    }

    if last_error:
        updates["auto_fix_error"] = last_error

    _update_alert(
        original_run_id,
        updates,
    )


def _auto_fix_failure_worker(original_run_id: str) -> None:
    """
    _auto_fix_failure'ı çağırır; bağımsız süreç içinde fırlayan
    beklenmedik bir hatanın hiçbir iz bırakmadan kaybolmaması için
    try/except ile loglanır.
    """

    try:
        _auto_fix_failure(original_run_id)

    except Exception as exc:
        _log(f"beklenmeyen hata: {exc}")


def _launch_auto_fix_worker_process(run_id: str) -> None:
    """
    _auto_fix_failure'ı, başarısız olan adımın kendi alt sürecinden
    (subprocess) TAMAMEN BAĞIMSIZ, ayrıca başlatılmış (detached) yeni
    bir OS sürecinde çalıştırır -- bkz. modülün başındaki "ÖNEMLİ" notu.
    Bu modülü (`python alerting.py --auto-fix-run <run_id>`) bir alt
    süreç olarak başlatır ve HİÇ BEKLEMEDEN (Popen üzerinde wait/
    communicate çağırmadan) hemen döner; böylece çağıran hook da anında
    dönebilir ve adımın alt süreci normal şekilde kapanabilir.

    Windows'ta DETACHED_PROCESS + CREATE_NEW_PROCESS_GROUP, yeni süreci
    ebeveyninin (bu adımın alt sürecinin) süreç grubundan tamamen
    koparır -- ebeveyn kapanırken bu süreç ne sonlandırılır ne de
    ebeveynin "alt süreçlerimin hepsi kapansın" beklentisine dahil olur.
    """

    creationflags = 0
    start_new_session = False

    if os.name == "nt":
        creationflags = (
            subprocess.DETACHED_PROCESS
            | subprocess.CREATE_NEW_PROCESS_GROUP
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

    # -----------------------------------------------------------------------
    # Otomatik düzeltme -- kullanıcıdan bağımsız, otomatik olarak
    # tetiklenir (bkz. _auto_fix_failure). Bu run'ın KENDİSİ zaten bir
    # otomatik düzeltme denemesiyse (AUTO_FIX_ATTEMPT_TAG tag'i varsa),
    # burada YENİ bir döngü BAŞLATILMAZ -- o deneme zaten dıştaki
    # _auto_fix_failure çağrısının polling'i tarafından takip ediliyor.
    # Bu kontrol olmadan her başarısız deneme kendi 3 denemesini
    # başlatır ve üstel bir retry patlamasına yol açar.
    #
    # ÖNEMLİ: _auto_fix_failure bu hook'un içinde SENKRON olarak DEĞİL,
    # AYRI, TAMAMEN BAĞIMSIZ (detached) bir OS SÜRECİNDE başlatılır --
    # bkz. _launch_auto_fix_worker_process ve modülün başındaki "ÖNEMLİ"
    # notu. Bir THREAD YETERLİ DEĞİLDİR: bu adımın kendisi zaten ayrı bir
    # alt süreçte (subprocess) çalışıyor ve Dagster'ın executor'ı run'ı
    # sonlandırmadan önce o alt sürecin TAMAMEN kapanmasını bekliyor;
    # alt sürecin içinde yaşayan bir thread bu kapanmayı engelleyip run'ı
    # süresiz "STARTED" durumunda kilitler.
    # -----------------------------------------------------------------------

    try:

        run = context.instance.get_run_by_id(context.run_id)
        is_auto_fix_attempt = bool(
            run and run.tags.get(AUTO_FIX_ATTEMPT_TAG)
        )

        if not is_auto_fix_attempt:

            _launch_auto_fix_worker_process(context.run_id)

            context.log.info(
                "Otomatik düzeltme ayrı bir süreçte başlatıldı (en "
                f"fazla {AUTO_FIX_MAX_ATTEMPTS} deneme)."
            )

    except Exception as exc:
        context.log.error(
            f"Otomatik düzeltme başlatılamadı: {exc}"
        )


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

    # Bu run, daha önce başarısız olmuş bir run'ın "re-execute"i ise
    # (dashboard'daki "Dagster'da Aç ve Tekrar Çalıştır" linkiyle),
    # parent_run_id zinciri o başarısız run'lara kadar uzanır. Sadece
    # bu zincirdeki run_id'lere ait FAILURE alertlerini çözülmüş say.
    # Bağımsız (re-execute olmayan) yeni bir başarılı run, aynı
    # job/step'e ait BAŞKA alertleri otomatik çözmemeli — onlar ayrı
    # olaylardır ve kendi re-execute'larıyla çözülmelidir.
    ancestor_run_ids = set()

    try:
        current_run = context.instance.get_run_by_id(context.run_id)

        while current_run is not None and current_run.parent_run_id:
            ancestor_run_ids.add(current_run.parent_run_id)
            current_run = context.instance.get_run_by_id(current_run.parent_run_id)

    except Exception as exc:
        context.log.error(f"Run soy ağacı okunurken hata oluştu: {exc}")

    if not ancestor_run_ids:
        return

    try:
        existing_alerts = json.loads(ALERT_FILE.read_text(encoding="utf-8"))
        if not isinstance(existing_alerts, list):
            return

        is_updated = False

        for alert in existing_alerts:
            if (alert.get("job_name") == job_name and
                alert.get("step_name") == step_name and
                alert.get("status") == "FAILURE" and
                alert.get("run_id") in ancestor_run_ids):

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


# ---------------------------------------------------------------------------
# Otomatik Düzeltme Worker Girişi
# ---------------------------------------------------------------------------
#
# _launch_auto_fix_worker_process bu dosyayı `python alerting.py
# --auto-fix-run <run_id>` şeklinde AYRI, TAMAMEN BAĞIMSIZ bir OS
# süreci olarak başlatır (bkz. yukarıdaki fonksiyon ve modülün başındaki
# "ÖNEMLİ" notu). Bu blok, dosya bir Dagster kod konumu modülü olarak
# DEĞİL de doğrudan bir script olarak çalıştırıldığında devreye girer.

if __name__ == "__main__":

    if len(sys.argv) >= 3 and sys.argv[1] == "--auto-fix-run":
        _auto_fix_failure_worker(sys.argv[2])