"""
A3 - İHA Veri Platformu
Pipeline Metrikleri + Asset Kataloğu + Telemetri Gözat/Dışa Aktar

Backend:
    ClickHouse

AU-AIR telemetry kolonları:

    time
    latitude
    longitude
    altitude
    velocity_x
    velocity_y
    velocity_z
    roll
    pitch
    yaw
    image_name
    box_x
    box_y
    box_w
    box_h
    class

Dashboard bölümleri:

    1. Pipeline Metrikleri
       - Dagster GraphQL API
       - Run durumu
       - Run süresi

    2. Katalog
       - Dagster asset'leri
       - Son materialization
       - Metadata

    3. Alertler
       - Başarısız Dagster run'ları
       - failure_hook tarafından oluşturulan alert kayıtları

    4. Veri Gözat / Dışa Aktar
       - ClickHouse bağlantısı
       - Time filtresi
       - Class filtresi
       - Değer bazlı satır filtresi (örn. altitude < 23)
       - Kolon seçimi
       - Satır sayısı
       - Veri önizleme
       - CSV dışa aktarma
"""

import json
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import clickhouse_connect
import pandas as pd
import requests
import streamlit as st
from streamlit_autorefresh import st_autorefresh


# ============================================================
# STREAMLIT CONFIG
# ============================================================

st.set_page_config(
    page_title="İHA Veri Platformu - Pipeline & Katalog",
    layout="wide",
)


# ============================================================
# GENEL AYARLAR
# ============================================================

REFRESH_OPTIONS = {
    "Kapalı": 0,
    "10 sn": 10,
    "30 sn": 30,
    "60 sn": 60,
}

MAX_EXPORT_ROWS = 2_000_000


# ============================================================
# AU-AIR KOLONLARI
# ============================================================

AU_AIR_COLUMNS = [
    "time",
    "latitude",
    "longitude",
    "altitude",
    "velocity_x",
    "velocity_y",
    "velocity_z",
    "roll",
    "pitch",
    "yaw",
    "image_name",
    "box_x",
    "box_y",
    "box_w",
    "box_h",
    "class",
]


# ============================================================
# DAGSTER GRAPHQL
# ============================================================

RUNS_QUERY = """
query RecentRuns($limit: Int!) {
  runsOrError(limit: $limit) {
    __typename

    ... on Runs {
      results {
        runId
        jobName
        status
        startTime
        endTime
      }
    }

    ... on PythonError {
      message
    }
  }
}
"""


ASSET_CATALOG_QUERY = """
query AssetCatalog {
  assetNodes {
    id
    groupName
    description

    assetKey {
      path
    }

    assetMaterializations(limit: 1) {
      timestamp
      runId

      metadataEntries {
        label
        description
        __typename

        ... on TextMetadataEntry {
          text
        }

        ... on IntMetadataEntry {
          intValue
        }

        ... on FloatMetadataEntry {
          floatValue
        }

        ... on BoolMetadataEntry {
          boolValue
        }

        ... on MarkdownMetadataEntry {
          mdStr
        }

        ... on JsonMetadataEntry {
          jsonString
        }

        ... on UrlMetadataEntry {
          url
        }

        ... on PathMetadataEntry {
          path
        }
      }
    }
  }
}
"""


# ============================================================
# DAGSTER URL
# ============================================================

def get_graphql_url() -> str:
    return os.environ.get(
        "DAGSTER_GRAPHQL_URL",
        "http://localhost:3000/graphql",
    )


def get_ui_url() -> str:
    explicit = os.environ.get("DAGSTER_UI_URL")

    if explicit:
        return explicit.rstrip("/")

    return get_graphql_url().replace(
        "/graphql",
        "",
    ).rstrip("/")


# ============================================================
# CLICKHOUSE AYARLARI
# ============================================================

def get_clickhouse_host() -> str:
    return os.environ.get(
        "CLICKHOUSE_HOST",
        "localhost",
    )


def get_clickhouse_port() -> int:
    return int(
        os.environ.get(
            "CLICKHOUSE_PORT",
            "8123",
        )
    )


def get_clickhouse_user() -> str:
    return os.environ.get(
        "CLICKHOUSE_USER",
        "default",
    )


def get_clickhouse_password() -> str:
    return os.environ.get(
        "CLICKHOUSE_PASSWORD",
        "",
    )


def get_clickhouse_database() -> str:
    return os.environ.get(
        "CLICKHOUSE_DATABASE",
        "default",
    )


def get_clickhouse_table() -> str:
    return os.environ.get(
        "CLICKHOUSE_TABLE",
        "telemetry",
    )


# ============================================================
# CLICKHOUSE CLIENT
# ============================================================

def get_clickhouse_client():

    return clickhouse_connect.get_client(
        host=get_clickhouse_host(),
        port=get_clickhouse_port(),
        username=get_clickhouse_user(),
        password=get_clickhouse_password(),
        database=get_clickhouse_database(),
    )


# ============================================================
# CLICKHOUSE KONTROL
# ============================================================

@st.cache_data(ttl=30)
def check_clickhouse_connection() -> bool:

    client = get_clickhouse_client()

    result = client.query(
        "SELECT 1"
    )

    return bool(result.result_rows)


# ============================================================
# CLICKHOUSE TABLO ŞEMASI
# ============================================================

@st.cache_data(ttl=30)
def get_clickhouse_schema() -> pd.DataFrame:

    client = get_clickhouse_client()

    query = f"""
    DESCRIBE TABLE {get_clickhouse_table()}
    """

    result = client.query(query)

    rows = []

    for row in result.result_rows:

        rows.append(
            {
                "kolon": row[0],
                "tip": row[1],
                "default_type": row[2],
                "default_expression": row[3],
            }
        )

    return pd.DataFrame(rows)


@st.cache_data(ttl=30)
def get_available_columns() -> list:

    schema = get_clickhouse_schema()

    if schema.empty:
        return []

    return schema["kolon"].tolist()


# ============================================================
# SAYISAL KOLONLAR (değer bazlı satır filtresi için)
# ============================================================

NUMERIC_TYPE_HINTS = (
    "Int",
    "UInt",
    "Float",
    "Decimal",
)


@st.cache_data(ttl=30)
def get_numeric_columns() -> list:
    """
    ClickHouse şemasındaki sayısal tipteki kolonları döner.

    Örn: latitude, altitude, box_x, box_w gibi Float64/Int
    kolonları üzerinden "değeri 23'ten küçük olan satırlar" gibi
    aramalar yapılabilmesini sağlar.
    """

    schema = get_clickhouse_schema()

    if schema.empty:
        return []

    numeric_cols = [
        row["kolon"]
        for _, row in schema.iterrows()
        if str(row["tip"]).startswith(
            NUMERIC_TYPE_HINTS
        )
    ]

    return numeric_cols


# ============================================================
# CLICKHOUSE FLIGHT / CLASS BİLGİLERİ
# ============================================================

@st.cache_data(ttl=60)
def get_available_classes() -> list:

    columns = get_available_columns()

    if "class" not in columns:
        return []

    client = get_clickhouse_client()

    query = f"""
    SELECT DISTINCT class
    FROM {get_clickhouse_table()}
    ORDER BY class
    """

    result = client.query(query)

    return [
        row[0]
        for row in result.result_rows
    ]


# ============================================================
# TIME ARALIĞI
# ============================================================

def get_time_range():

    client = get_clickhouse_client()

    query = f"""
    SELECT
        min(time),
        max(time)
    FROM {get_clickhouse_table()}
    """

    result = client.query(query)

    if not result.result_rows:
        return None, None

    min_time = result.result_rows[0][0]
    max_time = result.result_rows[0][1]

    return min_time, max_time


# ============================================================
# CLICKHOUSE WHERE OLUŞTURMA
# ============================================================

# Sadece bu operatörlere izin veriliyor (SQL injection'ı önlemek için)
VALUE_FILTER_OPERATORS = {
    "<": "<",
    "<=": "<=",
    ">": ">",
    ">=": ">=",
    "=": "=",
    "!=": "!=",
}


def build_clickhouse_where(
    start_time=None,
    end_time=None,
    selected_classes=None,
    value_filters=None,
):
    """
    value_filters: [{"column": "altitude", "operator": "<", "value": 23}, ...]

    Her filtre AND ile birleştirilir (örn. "altitude < 23 AND box_w >= 50").
    Kolon adı ve operatör beyaz listeye (whitelist) karşı doğrulanır,
    değer ise ClickHouse parametre binding'i ile geçirilir; bu sayede
    SQL injection riski oluşmaz.
    """

    conditions = []
    parameters = {}

    if start_time is not None:

        conditions.append(
            "time >= {start_time:DateTime}"
        )

        parameters["start_time"] = start_time

    if end_time is not None:

        conditions.append(
            "time <= {end_time:DateTime}"
        )

        parameters["end_time"] = end_time

    if selected_classes:

        conditions.append(
            "class IN {classes:Array(String)}"
        )

        parameters["classes"] = [
            str(x)
            for x in selected_classes
        ]

    if value_filters:

        available_columns = set(
            get_available_columns()
        )

        for index, value_filter in enumerate(value_filters):

            column = value_filter.get("column")
            operator = value_filter.get("operator")
            value = value_filter.get("value")

            if column not in available_columns:
                continue

            if operator not in VALUE_FILTER_OPERATORS:
                continue

            if value is None:
                continue

            param_name = f"value_filter_{index}"

            conditions.append(
                f"`{column}` "
                f"{VALUE_FILTER_OPERATORS[operator]} "
                f"{{{param_name}:Float64}}"
            )

            parameters[param_name] = float(value)

    if not conditions:
        return "1 = 1", parameters

    return (
        " AND ".join(conditions),
        parameters,
    )


# ============================================================
# SATIR SAYISI
# ============================================================

def count_filtered_rows(
    start_time=None,
    end_time=None,
    selected_classes=None,
    value_filters=None,
):

    client = get_clickhouse_client()

    where, parameters = build_clickhouse_where(
        start_time,
        end_time,
        selected_classes,
        value_filters,
    )

    query = f"""
    SELECT count() FROM (
        SELECT DISTINCT *
        FROM {get_clickhouse_table()}
        WHERE {where}
    )
    """

    result = client.query(
        query,
        parameters=parameters,
    )

    return int(
        result.result_rows[0][0]
    )


# ============================================================
# VERİ GETİRME
# ============================================================

def fetch_filtered_telemetry(
    start_time=None,
    end_time=None,
    selected_classes=None,
    columns=None,
    value_filters=None,
):

    client = get_clickhouse_client()

    where, parameters = build_clickhouse_where(
        start_time,
        end_time,
        selected_classes,
        value_filters,
    )

    if columns:

        # Güvenlik açısından sadece mevcut kolonları kullan
        available = set(
            get_available_columns()
        )

        valid_columns = [
            col
            for col in columns
            if col in available
        ]

        if not valid_columns:
            col_expr = "*"
        else:
            col_expr = ", ".join(
                f"`{col}`"
                for col in valid_columns
            )

    else:

        col_expr = "*"

    query = f"""
    SELECT DISTINCT {col_expr}
    FROM {get_clickhouse_table()}
    WHERE {where}
    ORDER BY time
    """

    return client.query_df(
        query,
        parameters=parameters,
    )


# ============================================================
# GRAPHQL REQUEST
# ============================================================

def run_graphql(
    query: str,
    variables: dict | None = None,
) -> dict:

    response = requests.post(
        get_graphql_url(),
        json={
            "query": query,
            "variables": variables or {},
        },
        timeout=10,
    )

    response.raise_for_status()

    payload = response.json()

    if payload.get("errors"):

        raise RuntimeError(
            payload["errors"][0].get(
                "message",
                "GraphQL hatası",
            )
        )

    return payload["data"]


# ============================================================
# TIMESTAMP
# ============================================================

def epoch_to_dt(value):

    if value is None:
        return None

    value = float(value)

    if value > 10_000_000_000:
        value /= 1000

    return datetime.fromtimestamp(
        value,
        tz=timezone.utc,
    )


# ============================================================
# DAGSTER RUN'LARI
# ============================================================

@st.cache_data(ttl=10)
def fetch_runs(
    limit: int = 50,
) -> pd.DataFrame:

    data = run_graphql(
        RUNS_QUERY,
        {
            "limit": limit
        },
    )

    runs_result = data[
        "runsOrError"
    ]

    if runs_result["__typename"] == "PythonError":

        raise RuntimeError(
            runs_result["message"]
        )

    rows = []

    for run in runs_result["results"]:

        start = epoch_to_dt(
            run.get("startTime")
        )

        end = epoch_to_dt(
            run.get("endTime")
        )

        duration = None

        if start and end:

            duration = (
                end - start
            ).total_seconds()

        rows.append(
            {
                "run_id": run["runId"],
                "job": run.get("jobName"),
                "status": run["status"],
                "start": start,
                "end": end,
                "duration_sn": (
                    round(duration, 1)
                    if duration is not None
                    else None
                ),
            }
        )

    return pd.DataFrame(rows)


# ============================================================
# METADATA OKUMA
# ============================================================

def parse_json_metadata(value):

    if value is None:
        return None

    if isinstance(value, str):

        try:
            return json.loads(value)

        except Exception:
            return value

    return value


def flatten_metadata(entries):

    flat = {}

    for entry in entries or []:

        label = entry.get(
            "label",
            "?",
        )

        typename = entry.get(
            "__typename",
            "",
        )

        if typename == "TextMetadataEntry":

            value = entry.get("text")

        elif typename == "IntMetadataEntry":

            value = entry.get("intValue")

        elif typename == "FloatMetadataEntry":

            value = entry.get("floatValue")

        elif typename == "BoolMetadataEntry":

            value = entry.get("boolValue")

        elif typename == "MarkdownMetadataEntry":

            value = entry.get("mdStr")

        elif typename == "JsonMetadataEntry":

            value = parse_json_metadata(
                entry.get("jsonString")
            )

        elif typename == "UrlMetadataEntry":

            value = entry.get("url")

        elif typename == "PathMetadataEntry":

            value = entry.get("path")

        else:

            value = entry.get(
                "description"
            )

        flat[label] = value

    return flat


# ============================================================
# ASSET CATALOG
# ============================================================

@st.cache_data(ttl=10)
def fetch_asset_catalog() -> pd.DataFrame:

    data = run_graphql(
        ASSET_CATALOG_QUERY
    )

    nodes = data["assetNodes"]

    rows = []

    for node in nodes:

        key = "/".join(
            node["assetKey"]["path"]
        )

        materializations = (
            node.get(
                "assetMaterializations"
            )
            or []
        )

        latest = (
            materializations[0]
            if materializations
            else None
        )

        metadata = (
            flatten_metadata(
                latest[
                    "metadataEntries"
                ]
            )
            if latest
            else {}
        )

        rows.append(
            {
                "asset": key,
                "grup": node.get(
                    "groupName"
                ),
                "aciklama": node.get(
                    "description"
                ),
                "son_materialize": (
                    epoch_to_dt(
                        latest["timestamp"]
                    )
                    if latest
                    else None
                ),
                "run_id": (
                    latest.get("runId")
                    if latest
                    else None
                ),
                "metadata": metadata,
            }
        )

    return pd.DataFrame(rows)


# ===========================================================================
# ALERT DOSYASI
# ===========================================================================

def get_alert_file() -> Path:
    """
    Alert dosyasını farklı çalışma dizinlerinde bulmaya çalışır.

    Olası konumlar:

        dashboard/data/alerts/alerts.json

        dagster/data/alerts/alerts.json

    Ayrıca ALERT_FILE environment variable ile doğrudan
    dosya yolu verilebilir.
    """

    explicit = os.environ.get(
        "ALERT_FILE"
    )

    if explicit:

        return Path(explicit)

    current_dir = Path(__file__).resolve().parent

    candidates = [

        current_dir
        / "data"
        / "alerts"
        / "alerts.json",

        current_dir.parent
        / "dagster"
        / "data"
        / "alerts"
        / "alerts.json",

        Path.cwd()
        / "data"
        / "alerts"
        / "alerts.json",

    ]

    for candidate in candidates:

        if candidate.exists():

            return candidate

    # Varsayılan
    return candidates[1]


@st.cache_data(ttl=5)
def load_alerts() -> pd.DataFrame:

    alert_file = get_alert_file()

    if not alert_file.exists():

        return pd.DataFrame(
            columns=[
                "timestamp",
                "job_name",
                "step_name",
                "error",
                "status",
            ]
        )

    try:

        content = alert_file.read_text(
            encoding="utf-8"
        )

        alerts = json.loads(
            content
        )

    except Exception:

        return pd.DataFrame(
            columns=[
                "timestamp",
                "job_name",
                "step_name",
                "error",
                "status",
            ]
        )

    if not isinstance(
        alerts,
        list,
    ):

        return pd.DataFrame()

    if not alerts:

        return pd.DataFrame(
            columns=[
                "timestamp",
                "job_name",
                "step_name",
                "error",
                "status",
            ]
        )

    df = pd.DataFrame(
        alerts
    )

    if "timestamp" in df.columns:

        df["timestamp"] = pd.to_datetime(
            df["timestamp"],
            errors="coerce",
        )

    return df.sort_values(
        "timestamp",
        ascending=False,
    ).reset_index(drop=True)


# ============================================================
# PIPELINE KPI
# ============================================================

def render_run_kpis(
    df: pd.DataFrame,
):

    total = len(df)

    counts = (
        df["status"]
        .value_counts()
        .to_dict()
        if total
        else {}
    )

    success = counts.get(
        "SUCCESS",
        0,
    )

    failure = counts.get(
        "FAILURE",
        0,
    )

    in_progress = (
        counts.get(
            "STARTED",
            0,
        )
        + counts.get(
            "STARTING",
            0,
        )
        + counts.get(
            "QUEUED",
            0,
        )
    )

    columns = st.columns(4)

    columns[0].metric(
        "Son Run Sayısı",
        f"{total:,}",
    )

    columns[1].metric(
        "Başarılı",
        f"{success:,}",
    )

    columns[2].metric(
        "Hatalı",
        f"{failure:,}",
    )

    columns[3].metric(
        "Sürüyor / Kuyrukta",
        f"{in_progress:,}",
    )


# ============================================================
# RUN TABLOSU
# ============================================================

def render_run_table(
    df: pd.DataFrame,
):

    if df.empty:

        st.info(
            "Henüz run kaydı yok."
        )

        return

    ui_url = get_ui_url()

    display = df.copy()

    display["Dagster UI"] = display[
        "run_id"
    ].apply(
        lambda run_id:
        f"{ui_url}/runs/{run_id}"
    )

    st.dataframe(
        display[
            [
                "run_id",
                "job",
                "status",
                "start",
                "end",
                "duration_sn",
                "Dagster UI",
            ]
        ],
        use_container_width=True,
        hide_index=True,
        column_config={
            "Dagster UI":
                st.column_config.LinkColumn(
                    "Dagster UI",
                    display_text="Aç",
                )
        },
    )


# ============================================================
# RUN STATUS CHART
# ============================================================

def render_status_chart(
    df: pd.DataFrame,
):

    if df.empty:
        return

    status_counts = (
        df["status"]
        .value_counts()
        .rename_axis("status")
        .reset_index(
            name="count"
        )
    )

    st.bar_chart(
        status_counts.set_index(
            "status"
        )
    )


# ============================================================
# KATALOG
# ============================================================

def render_catalog(
    df: pd.DataFrame,
):

    if df.empty:

        st.info(
            "Henüz asset yok."
        )

        return

    search = st.text_input(
        "Asset ara"
    )

    filtered = df

    if search:

        filtered = df[
            df["asset"].str.contains(
                search,
                case=False,
                na=False,
            )
        ]

    st.dataframe(
        filtered[
            [
                "asset",
                "grup",
                "son_materialize",
                "aciklama",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

    st.caption(
        "Asset metadata detayları:"
    )

    for _, row in filtered.iterrows():

        metadata = row[
            "metadata"
        ]

        if not metadata:
            continue

        with st.expander(
            f"{row['asset']} — metadata"
        ):

            metadata_rows = []

            for key, value in metadata.items():

                if isinstance(
                    value,
                    (dict, list),
                ):

                    value = json.dumps(
                        value,
                        ensure_ascii=False,
                        indent=2,
                    )

                metadata_rows.append(
                    {
                        "alan": key,
                        "değer": value,
                    }
                )

            st.dataframe(
                pd.DataFrame(
                    metadata_rows
                ),
                use_container_width=True,
                hide_index=True,
            )

# ===========================================================================
# ALERT DASHBOARD
# ===========================================================================

def render_alerts(
    runs_df: pd.DataFrame,
) -> None:

    st.subheader(
        "🚨 Pipeline Alertleri"
    )

    alerts_df = load_alerts()

    # -----------------------------------------------------------------------
    # Zaman aralığı filtresi
    # -----------------------------------------------------------------------
    #
    # Test/backfill sırasında biriken çok sayıda eski alert, yeni gelen
    # gerçek alertleri gözden kaçırmayı kolaylaştırıyor. Bu filtre hem
    # alert listesine hem de aşağıdaki "Başarısız Run'lar" tablosuna
    # uygulanır; KPI'lar da seçilen aralığa göre güncellenir.

    TIME_FILTER_OPTIONS = [
        "Son 1 saat",
        "Bugün",
        "Tümü",
    ]

    time_filter = st.radio(
        "Zaman aralığı",
        options=TIME_FILTER_OPTIONS,
        index=0,
        horizontal=True,
        key="alert_time_filter",
    )

    now_utc = datetime.now(timezone.utc)

    def _filter_by_time(
        df: pd.DataFrame,
        time_column: str,
    ) -> pd.DataFrame:

        if df.empty or time_column not in df.columns:
            return df

        if time_filter == "Tümü":
            return df

        series = pd.to_datetime(
            df[time_column],
            errors="coerce",
            utc=True,
        )

        if time_filter == "Son 1 saat":

            cutoff = now_utc - timedelta(hours=1)

            return df[series >= cutoff]

        if time_filter == "Bugün":

            today = now_utc.date()

            return df[series.dt.date == today]

        return df

    alerts_df_filtered = _filter_by_time(
        alerts_df,
        "timestamp",
    )

    runs_df_filtered = _filter_by_time(
        runs_df,
        "start",
    )

    st.caption(
        f"Filtre: **{time_filter}** — "
        f"{len(alerts_df_filtered):,} / {len(alerts_df):,} alert gösteriliyor "
        f"(tüm zamanlarda toplam {len(alerts_df):,} kayıt var)."
    )

    # -----------------------------------------------------------------------
    # Üst KPI'lar
    # -----------------------------------------------------------------------

    total_alerts = len(
        alerts_df_filtered
    )

    failed_runs = 0

    if not runs_df_filtered.empty:

        failed_runs = int(
            (
                runs_df_filtered["status"]
                == "FAILURE"
            ).sum()
        )

    col1, col2, col3 = st.columns(3)

    col1.metric(
        "Kayıtlı Alert",
        f"{total_alerts:,}",
    )

    col2.metric(
        "Başarısız Run",
        f"{failed_runs:,}",
    )

    if total_alerts > 0:

        last_alert = alerts_df_filtered.iloc[0][
            "timestamp"
        ]

        col3.metric(
            "Son Alert",
            str(last_alert),
        )

    else:

        col3.metric(
            "Son Alert",
            "Yok",
        )

    st.divider()

    # -----------------------------------------------------------------------
    # Alert dosyası bilgisi
    # -----------------------------------------------------------------------

    alert_file = get_alert_file()

    st.caption(
        f"Alert kaynağı: `{alert_file}`"
    )

    # -----------------------------------------------------------------------
    # JSON alert kayıtları
    # -----------------------------------------------------------------------

    if not alerts_df_filtered.empty:

        st.subheader(
            "Hook Tarafından Kaydedilen Alertler"
        )

        display = alerts_df_filtered.copy()

        st.dataframe(
            display[
                [
                    "timestamp",
                    "job_name",
                    "step_name",
                    "status",
                    "error",
                ]
            ],

            use_container_width=True,

            hide_index=True,
        )

        st.divider()

        st.subheader(
            "Hata Detayı"
        )

        for index, row in alerts_df_filtered.iterrows():

            timestamp = row.get(
                "timestamp",
                "",
            )

            job_name = row.get(
                "job_name",
                "unknown",
            )

            step_name = row.get(
                "step_name",
                "unknown",
            )

            error = row.get(
                "error",
                "Bilinmeyen hata",
            )

            with st.expander(
                f"🚨 {job_name} → {step_name} | {timestamp}"
            ):

                st.markdown(
                    f"**Job:** `{job_name}`"
                )

                st.markdown(
                    f"**Step:** `{step_name}`"
                )

                st.markdown(
                    f"**Zaman:** `{timestamp}`"
                )

                st.error(
                    error
                )

    elif not alerts_df.empty:

        st.info(
            f"Seçilen zaman aralığında (**{time_filter}**) alert yok. "
            f"Tüm zamanlarda {len(alerts_df):,} kayıtlı alert var — "
            f"görmek için filtreyi **Tümü** olarak değiştirin."
        )

    else:

        st.success(
            "🎉 Henüz kayıtlı bir pipeline alert'i yok."
        )

    # -----------------------------------------------------------------------
    # Başarısız Dagster run'ları
    # -----------------------------------------------------------------------

    st.divider()

    st.subheader(
        "Dagster'daki Başarısız Run'lar"
    )

    if runs_df_filtered.empty:

        if runs_df.empty:

            st.info(
                "Henüz Dagster run bilgisi yok."
            )

        else:

            st.info(
                f"Seçilen zaman aralığında (**{time_filter}**) run yok."
            )

        return

    failures = runs_df[
        runs_df["status"]
        == "FAILURE"
    ].copy()

    if failures.empty:

        st.success(
            "Son run'lar içerisinde başarısız pipeline bulunmuyor."
        )

        return

    ui_url = get_ui_url()

    failures["Dagster UI"] = (
        failures["run_id"].apply(
            lambda rid:
                f"{ui_url}/runs/{rid}"
        )
    )

    st.dataframe(

        failures[
            [
                "run_id",
                "job",
                "status",
                "start",
                "end",
                "duration_sn",
                "Dagster UI",
            ]
        ],

        use_container_width=True,

        hide_index=True,

        column_config={
            "Dagster UI":
                st.column_config.LinkColumn(
                    "Dagster UI",
                    display_text="Aç",
                ),
        },
    )


# ============================================================
# VERİ GÖZAT / DIŞA AKTAR
# ============================================================

def render_data_export():

    st.subheader(
        "AU-AIR Telemetri Verisi"
    )

    st.caption(
        f"Backend: **ClickHouse** | "
        f"Tablo: `{get_clickhouse_database()}.{get_clickhouse_table()}`"
    )

    # --------------------------------------------------------
    # ClickHouse bağlantısı
    # --------------------------------------------------------

    try:

        check_clickhouse_connection()

    except Exception as exc:

        st.error(
            f"ClickHouse'a bağlanılamadı: {exc}"
        )

        return

    # --------------------------------------------------------
    # Şema
    # --------------------------------------------------------

    try:

        schema = get_clickhouse_schema()

    except Exception as exc:

        st.error(
            f"ClickHouse tablo şeması okunamadı: {exc}"
        )

        return

    if schema.empty:

        st.warning(
            "ClickHouse tablosunda kolon bulunamadı."
        )

        return

    # --------------------------------------------------------
    # Time aralığı
    # --------------------------------------------------------

    try:

        min_time, max_time = get_time_range()

    except Exception as exc:

        st.error(
            f"Time aralığı okunamadı: {exc}"
        )

        return

    if min_time is None or max_time is None:

        st.warning(
            "Telemetry tablosunda veri bulunamadı."
        )

        return

    st.write(
        f"**Veri aralığı:** "
        f"{min_time} → {max_time}"
    )

    st.divider()

    # --------------------------------------------------------
    # Filtreler
    # --------------------------------------------------------

    st.subheader(
        "Veri Filtreleme"
    )

    col1, col2 = st.columns(2)

    with col1:

        start_time = st.datetime_input(
            "Başlangıç zamanı",
            value=min_time,
        )

    with col2:

        end_time = st.datetime_input(
            "Bitiş zamanı",
            value=max_time,
        )

    # --------------------------------------------------------
    # Class filtresi
    # --------------------------------------------------------

    try:

        available_classes = (
            get_available_classes()
        )

    except Exception:

        available_classes = []

    selected_classes = []

    if available_classes:

        selected_classes = st.multiselect(
            "Class",
            options=available_classes,
            help="Boş bırakılırsa tüm class değerleri seçilir.",
        )

    # --------------------------------------------------------
    # Değer bazlı satır filtresi (örn. altitude < 23)
    # --------------------------------------------------------

    st.subheader(
        "Değer Bazlı Filtre (Satır Arama)"
    )

    st.caption(
        "Sayısal bir kolon için koşul tanımlayarak satır bazlı arama "
        "yapabilirsiniz. Örn: `altitude < 23` gibi. Birden fazla "
        "filtre eklenirse hepsi AND ile birleştirilir."
    )

    try:
        numeric_columns = get_numeric_columns()
    except Exception:
        numeric_columns = []

    if "value_filters" not in st.session_state:
        st.session_state["value_filters"] = []

    if "value_filter_next_id" not in st.session_state:
        st.session_state["value_filter_next_id"] = 0

    if not numeric_columns:

        st.info(
            "Tabloda sayısal (Int/Float) kolon bulunamadı, "
            "değer bazlı filtre kullanılamıyor."
        )

    else:

        if st.button(
            "➕ Filtre Ekle",
            key="add_value_filter_btn",
        ):

            st.session_state["value_filters"].append(
                {
                    "id": st.session_state["value_filter_next_id"],
                    "column": numeric_columns[0],
                    "operator": "<",
                    "value": 0.0,
                }
            )

            st.session_state["value_filter_next_id"] += 1

        rows_to_remove = []

        for filter_row in st.session_state["value_filters"]:

            row_id = filter_row["id"]

            fcol1, fcol2, fcol3, fcol4 = st.columns(
                [3, 2, 3, 1]
            )

            with fcol1:

                filter_row["column"] = st.selectbox(
                    "Kolon",
                    options=numeric_columns,
                    index=numeric_columns.index(
                        filter_row["column"]
                    )
                    if filter_row["column"] in numeric_columns
                    else 0,
                    key=f"value_filter_column_{row_id}",
                )

            with fcol2:

                operator_options = list(
                    VALUE_FILTER_OPERATORS.keys()
                )

                filter_row["operator"] = st.selectbox(
                    "Operatör",
                    options=operator_options,
                    index=operator_options.index(
                        filter_row["operator"]
                    ),
                    key=f"value_filter_operator_{row_id}",
                )

            with fcol3:

                filter_row["value"] = st.number_input(
                    "Değer",
                    value=float(filter_row["value"]),
                    key=f"value_filter_value_{row_id}",
                    format="%.4f",
                )

            with fcol4:

                st.write("")
                st.write("")

                if st.button(
                    "🗑️",
                    key=f"value_filter_remove_{row_id}",
                ):
                    rows_to_remove.append(row_id)

        if rows_to_remove:

            st.session_state["value_filters"] = [
                filter_row
                for filter_row in st.session_state["value_filters"]
                if filter_row["id"] not in rows_to_remove
            ]

            st.rerun()

        if st.session_state["value_filters"]:

            filter_summary = " AND ".join(
                f"{filter_row['column']} "
                f"{filter_row['operator']} "
                f"{filter_row['value']}"
                for filter_row in st.session_state["value_filters"]
            )

            st.caption(
                f"Aktif filtre: `{filter_summary}`"
            )

    value_filters = st.session_state.get(
        "value_filters",
        [],
    )

    # --------------------------------------------------------
    # Kolon seçimi
    # --------------------------------------------------------

    st.subheader(
        "Kolon Seçimi"
    )

    available_columns = (
        get_available_columns()
    )

    default_columns = [
        column
        for column in AU_AIR_COLUMNS
        if column in available_columns
    ]

    selected_columns = st.multiselect(
        "Gösterilecek / dışa aktarılacak kolonlar",
        options=available_columns,
        default=default_columns,
    )

    columns = (
        selected_columns
        if selected_columns
        else None
    )

    # --------------------------------------------------------
    # Tarih kontrolü
    # --------------------------------------------------------

    if start_time > end_time:

        st.error(
            "Başlangıç zamanı bitiş zamanından sonra olamaz."
        )

        return

    st.divider()

    # --------------------------------------------------------
    # SATIR SAYISI
    # --------------------------------------------------------

    if st.button(
        "Satır Sayısını Hesapla",
        type="secondary",
    ):

        try:

            with st.spinner(
                "Satır sayısı hesaplanıyor..."
            ):

                row_count = count_filtered_rows(
                    start_time=start_time,
                    end_time=end_time,
                    selected_classes=selected_classes,
                    value_filters=value_filters,
                )

            st.session_state[
                "export_row_count"
            ] = row_count

            st.success(
                f"Filtreye uyan satır sayısı: "
                f"**{row_count:,}**"
            )

        except Exception as exc:

            st.error(
                f"Sorgu çalıştırılamadı: {exc}"
            )

            return

    row_count = st.session_state.get(
        "export_row_count"
    )

    if row_count is None:
        return

    # --------------------------------------------------------
    # SONUÇ YOK
    # --------------------------------------------------------

    if row_count == 0:

        st.warning(
            "Seçilen filtrelerle eşleşen veri yok."
        )

        return

    # --------------------------------------------------------
    # EXPORT LIMIT
    # --------------------------------------------------------

    if row_count > MAX_EXPORT_ROWS:

        st.warning(
            f"{row_count:,} satır çok fazla. "
            f"Mevcut export limiti: "
            f"{MAX_EXPORT_ROWS:,} satır."
        )

        st.info(
            "Lütfen zaman aralığını veya class filtresini daraltın."
        )

        return

    # --------------------------------------------------------
    # VERİ GETİR
    # --------------------------------------------------------

    if st.button(
        "Veriyi Getir ve Önizle",
        type="primary",
    ):

        try:

            with st.spinner(
                "ClickHouse'dan veri okunuyor..."
            ):

                dataframe = fetch_filtered_telemetry(
                    start_time=start_time,
                    end_time=end_time,
                    selected_classes=selected_classes,
                    columns=columns,
                    value_filters=value_filters,
                )

            st.session_state[
                "export_df"
            ] = dataframe

        except Exception as exc:

            st.error(
                f"Veri okunamadı: {exc}"
            )

            return

    dataframe = st.session_state.get(
        "export_df"
    )

    if dataframe is None:
        return

    # --------------------------------------------------------
    # ÖNİZLEME
    # --------------------------------------------------------

    st.divider()

    st.subheader(
        "Veri Önizleme"
    )

    st.info(
        f"Toplam {len(dataframe):,} satır getirildi."
    )

    st.dataframe(
        dataframe.head(200),
        use_container_width=True,
        hide_index=True,
    )

    st.caption(
        "Yukarıdaki tablo ilk 200 satırı gösterir. "
        "CSV dosyasında seçilen tüm satırlar bulunur."
    )

    # --------------------------------------------------------
    # CSV EXPORT
    # --------------------------------------------------------

    st.divider()

    st.subheader(
        "Dışa Aktar"
    )

    csv_data = dataframe.to_csv(
        index=False
    ).encode(
        "utf-8-sig"
    )

    filename = (
        f"au_air_telemetry_"
        f"{start_time.strftime('%Y%m%d_%H%M%S')}_"
        f"{end_time.strftime('%Y%m%d_%H%M%S')}.csv"
    )

    st.download_button(
        label="CSV olarak indir",
        data=csv_data,
        file_name=filename,
        mime="text/csv",
        type="primary",
    )

    st.caption(
        f"CSV dosyası: {len(dataframe):,} satır, "
        f"{len(dataframe.columns)} kolon"
    )


# ============================================================
# ANA UYGULAMA
# ============================================================

def main():

    st.title(
        "İHA Veri Platformu — Pipeline Metrikleri & Katalog"
    )

    st.caption(
        "Kaynak: Dagster GraphQL API + ClickHouse. "
        "Katalog verisi Dagster asset materialization metadata'sından, "
        "telemetri verisi ise ClickHouse üzerinden okunmaktadır."
    )

    # ========================================================
    # SIDEBAR
    # ========================================================

    with st.sidebar:

        st.header(
            "Ayarlar"
        )

        st.caption(
            f"Dagster GraphQL: "
            f"{get_graphql_url()}"
        )

        st.caption(
            f"ClickHouse: "
            f"{get_clickhouse_host()}:{get_clickhouse_port()}"
        )

        st.caption(
            f"Tablo: "
            f"{get_clickhouse_database()}."
            f"{get_clickhouse_table()}"
        )

        st.divider()

        run_limit = st.slider(
            "Gösterilecek run sayısı",
            10,
            200,
            50,
            step=10,
        )

        refresh_label = st.selectbox(
            "Otomatik yenileme",
            list(
                REFRESH_OPTIONS.keys()
            ),
            index=2,
        )

        refresh_seconds = (
            REFRESH_OPTIONS[
                refresh_label
            ]
        )

        if st.button(
            "Şimdi yenile"
        ):

            fetch_runs.clear()
            fetch_asset_catalog.clear()
            load_alerts.clear()
            get_available_columns.clear()
            get_numeric_columns.clear()
            get_clickhouse_schema.clear()
            get_available_classes.clear()
            check_clickhouse_connection.clear()

            st.rerun()

        st.caption(
            "Son sorgu: "
            f"{datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"
        )

    # ========================================================
    # AUTO REFRESH
    # ========================================================

    if refresh_seconds:

        st_autorefresh(
            interval=refresh_seconds * 1000,
            key="dashboard_refresh",
        )

    # ========================================================
    # RUN VERİSİNİ BİR KEZ ÇEK
    # ========================================================

    try:

        runs_df = fetch_runs(
            run_limit
        )

    except Exception as exc:

        runs_df = pd.DataFrame()

        st.error(
            f"Dagster'a bağlanılamadı: {exc}"
        )

    # ========================================================
    # TABS
    # ========================================================

    tab_runs, tab_catalog, tab_alerts, tab_export = st.tabs(
        [
            "Pipeline Metrikleri",
            "Katalog",
            "🚨 Alertler",
            "Veri Gözat / Dışa Aktar",
        ]
    )

    # ========================================================
    # PIPELINE METRİKLERİ
    # ========================================================

    with tab_runs:

        if not runs_df.empty:

            render_run_kpis(
                runs_df
            )

            st.divider()

            left, right = st.columns(
                [1, 1]
            )

            with left:

                st.subheader(
                    "Durum Dağılımı"
                )

                render_status_chart(
                    runs_df
                )

            with right:

                st.subheader(
                    "Son Run'lar"
                )

            render_run_table(
                runs_df
            )
            
        else:

            st.info(
                "Gösterilecek run bulunamadı."
            )

    # ========================================================
    # KATALOG
    # ========================================================

    with tab_catalog:

        try:

            catalog_df = (
                fetch_asset_catalog()
            )

        except Exception as exc:

            st.error(
                f"Katalog okunamadı: {exc}"
            )

        else:

            render_catalog(
                catalog_df
            )
            
    # ========================================================
    # ALERTS
    # ========================================================

    with tab_alerts:
    
        render_alerts(
            runs_df
        )

    # ========================================================
    # VERİ GÖZAT / EXPORT
    # ========================================================

    with tab_export:

        render_data_export()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()