"""
A3 - İHA Veri Platformu
Pipeline Metrikleri + Asset Kataloğu + Telemetri Gözat/Dışa Aktar

Backend:
    ClickHouse (file() tablo fonksiyonuyla, dagster/data/processed
    klasöründeki parquet dosyalarını sorgu zamanında okur — veri
    ayrıca ClickHouse'un kendi depolama motoruna INSERT edilmez)

AU-AIR telemetry kolonları (native frame şeması):

    flight_id
    time
    image_name
    image_width
    image_height
    platform
    longitude
    latitude
    altitude
    linear_x
    linear_y
    linear_z
    angle_phi
    angle_theta
    angle_psi
    num_objects
    obj_human
    obj_car
    obj_truck
    obj_van
    obj_motorbike
    obj_bicycle
    obj_bus
    obj_trailer

Dashboard bölümleri:

    1. Pipeline Metrikleri
       - Dagster GraphQL API
       - Run durumu
       - Run süresi

    2. Katalog
       - Dagster asset'leri
       - Son materialization
       - Metadata
       - Metadata Geçmişi (Postgres, public.asset_metadata_history):
         asset / uçuş / tarih bazlı filtrelenebilir materialization
         geçmişi — bkz. docs/postgres_pipeline_catalog_schema.sql

    3. DVC / Veri Sürümleri
       - MinIO "pipeline-artifacts" bucket'ındaki Great Expectations
         kalite raporları (validation/<dataset>/<batch>/<etag>.json)
       - DVC uzak deposundaki (dvc-cache) .dir dizin manifestoları:
         her veri sürümünün hangi dosyalardan oluştuğu + md5'leri
       - Bağlantı: MINIO_ENDPOINT / MINIO_ACCESS_KEY / MINIO_SECRET_KEY
         (bkz. get_minio_client)

    4. Alertler
       - Başarısız Dagster run'ları (Dagster GraphQL)
       - failure_hook tarafından üretilen alert kayıtları: MinIO
         "pipeline-alerts" bucket'ındaki tek JSON nesnesi
         (MINIO_ALERTS_BUCKET / MINIO_ALERTS_OBJECT), Dagster yazar,
         dashboard salt-okunur çeker — bkz. load_alerts()

    5. Veri Gözat / Dışa Aktar
       - ClickHouse bağlantısı
       - Time filtresi
       - Saat filtresi -- günün belirli saatlerini (0-23), tarihten/
         uçuştan bağımsız olarak filtreler (toHour(time))
       - Class filtresi
       - Alan (harita) bazlı filtre -- haritada çizilen alanda uçmuş
         satırları filtreler (pointInPolygon); "dahil et" / "hariç tut"
         modu (örn. Erzurum'u çevreleyen alanı çizip "hariç tut" ile
         Erzurum DIŞINDAKİ uçuşları filtreleyebilirsiniz)
       - Değer bazlı satır filtresi (örn. altitude < 23)
       - Kolon seçimi
       - Satır sayısı
       - Veri önizleme
       - CSV / Parquet / TAB / MAT dışa aktarma
"""

import io
import json
import os
import re
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode

import clickhouse_connect
import folium
import pandas as pd
import psycopg2
import requests
import scipy.io
import streamlit as st
import streamlit.components.v1 as components
from dotenv import load_dotenv
from folium.plugins import Draw
from jinja2 import Template
from streamlit_folium import st_folium

from data_docs_viewer import (
    DataDocsReferenceError,
    inline_data_docs_bundle,
    validate_data_docs_html_key,
)

# MinIO istemcisi yalnızca "DVC / Veri Sürümleri" sekmesinde kullanılır.
# Paket kurulu değilse (eski bir imaj / requirements) uygulamanın geri
# kalanı çökmemeli -- bu yüzden import korumalı yapılır ve sekme, hata
# mesajını kullanıcıya gösterir.
try:
    from minio import Minio
    from minio.error import S3Error

    _MINIO_IMPORT_ERROR = None
except Exception as _minio_import_exc:  # pragma: no cover - ortam kaynaklı
    Minio = None
    S3Error = Exception
    _MINIO_IMPORT_ERROR = _minio_import_exc


# ============================================================
# .ENV DOSYASI
# ============================================================
#
# Dosya adı sabit "app.py" olduğu için (Streamlit çalışma dizini script'in
# bulunduğu yer olmayabilir), .env her zaman bu dosyayla aynı klasörden
# okunur. Böylece "streamlit run" nereden çalıştırılırsa çalıştırılsın
# dashboard/.env içeriği (CLICKHOUSE_* vb.) doğru şekilde yüklenir.

load_dotenv(
    Path(__file__).resolve().parent / ".env"
)


# ============================================================
# STREAMLIT CONFIG
# ============================================================

st.set_page_config(
    page_title="İHA Veri Platformu - Pipeline & Katalog",
    layout="wide",
)


# ============================================================
# ANA SEKMELER
# ============================================================
#
# st.tabs() Streamlit'te programatik/URL tabanlı olarak kontrol
# edilemediği (native sekmeler her zaman ilk sekmeden açılır) için,
# paylaşılan bir bağlantının (bkz. render_download_section, "🔗
# Paylaşılabilir Bağlantı Oluştur") doğrudan "Veri Gözat / Dışa
# Aktar" sekmesini açabilmesi amacıyla main() içinde native st.tabs()
# yerine session_state ile kontrol edilen bir st.radio "sekme"
# görünümü kullanılır (bkz. main()).

MAIN_TAB_RUNS = "Pipeline Metrikleri"
MAIN_TAB_CATALOG = "Katalog"
MAIN_TAB_DVC = "📦 DVC / Veri Sürümleri"
MAIN_TAB_ALERTS = "🚨 Alertler"
MAIN_TAB_EXPORT = "Veri Gözat / Dışa Aktar"
MAIN_TAB_FLIGHT_MAP = "🗺️ Uçuş Rotası"

MAIN_TAB_LABELS = [
    MAIN_TAB_RUNS,
    MAIN_TAB_CATALOG,
    MAIN_TAB_DVC,
    MAIN_TAB_ALERTS,
    MAIN_TAB_EXPORT,
    MAIN_TAB_FLIGHT_MAP,
]

# ============================================================
# PANELLER (Kullanıcı / Admin)
# ============================================================
#
# Dashboard iki panele ayrılır; yan paneldeki parolasız iki butonla
# geçilir (bkz. main()).
#
#   - Kullanıcı Paneli: yalnızca son kullanıcıya dönük sekmeler
#     ("Veri Gözat / Dışa Aktar" ve "Uçuş Rotası"). "Gösterilecek run
#     sayısı" slider'ı burada GÖSTERİLMEZ.
#   - Admin Paneli: tüm sekmeler + "Gösterilecek run sayısı" slider'ı.
#
# Not: parola/kimlik doğrulama YOK -- ayrım yalnızca arayüzü sadeleştirmek
# içindir, bir güvenlik sınırı değildir.

PANEL_USER = "👤 Kullanıcı Paneli"
PANEL_ADMIN = "🛠️ Admin Paneli"

PANEL_LABELS = [PANEL_USER, PANEL_ADMIN]

# Seçili panel URL'de "?panel=user|admin" olarak tutulur -- başlığa
# tıklamak (bkz. "?goto=runs") sayfayı yeniden yükleyip session_state'i
# sıfırladığı için panel bilgisinin reload'dan sağ çıkması gerekir.
PANEL_BY_CODE = {"user": PANEL_USER, "admin": PANEL_ADMIN}
PANEL_CODE_BY_LABEL = {PANEL_USER: "user", PANEL_ADMIN: "admin"}

# Kullanıcı panelinde görünecek sekmeler (sıra buradaki gibi çizilir).
USER_TAB_LABELS = [
    MAIN_TAB_EXPORT,
    MAIN_TAB_FLIGHT_MAP,
]


def _tabs_for_panel(panel: str) -> list:
    """Verilen panelde gösterilecek ana sekme etiketleri."""

    if panel == PANEL_USER:
        return USER_TAB_LABELS

    return MAIN_TAB_LABELS


def _sync_panel_to_url() -> None:
    """Seçili paneli URL'deki "?panel=..." param'ına yazar.

    Panel seçici butonuna basıldığında çağrılır -- yalnızca panel elle
    değiştirildiğinde. Her render'da URL'ye yazmak, aynı render içinde
    query param güncelleyen başka özelliklerle (ör. "🔗 Bağlantı Olarak
    Paylaş") çakışıyordu.
    """

    st.query_params["panel"] = PANEL_CODE_BY_LABEL.get(
        st.session_state.get("panel_selector"),
        "user",
    )

# ============================================================
# AU-AIR KOLONLARI
# ============================================================

AU_AIR_COLUMNS = [
    "flight_id",
    "time",
    "image_name",
    "image_width",
    "image_height",
    "platform",
    "longitude",
    "latitude",
    "altitude",
    "linear_x",
    "linear_y",
    "linear_z",
    "angle_phi",
    "angle_theta",
    "angle_psi",
    "num_objects",
    "obj_human",
    "obj_car",
    "obj_truck",
    "obj_van",
    "obj_motorbike",
    "obj_bicycle",
    "obj_bus",
    "obj_trailer",
]

# Native frame şemasında satır bazlı tek bir "class" etiketi YOK; onun
# yerine her frame için nesne türü başına bir sayaç kolonu var
# (obj_human, obj_car, ...). Panonun "Class" filtresi bu kolonlar
# üzerinden "frame'de en az 1 X var" (obj_X > 0) şeklinde çalışır.
# Anahtarlar (human, car, ...) BİLEREK İngilizce -- hem AU-AIR
# etiketleriyle hem de LLM_SINIF_ESANLAMLI'nin çıktısıyla birebir
# eşleşiyor, böylece LLM sınıf eşleştirmesi değişmeden çalışmaya devam
# ediyor. Eski, gerçek bir `class` kolonu olan tablolarda ise (bkz.
# get_available_classes / build_clickhouse_where) klasik `class IN (...)`
# davranışı korunur.
OBJ_CLASS_COLUMNS = {
    "human": "obj_human",
    "car": "obj_car",
    "truck": "obj_truck",
    "van": "obj_van",
    "motorbike": "obj_motorbike",
    "bicycle": "obj_bicycle",
    "bus": "obj_bus",
    "trailer": "obj_trailer",
}


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
    # dagster/assets/clickhouse.py bu isimde bir tabloya (aynı ortam
    # değişkeniyle, CLICKHOUSE_TABLE) INSERT yapar; burada aynı isim
    # kullanılarak iki taraf senkron kalır.
    return os.environ.get(
        "CLICKHOUSE_TABLE",
        "telemetry",
    )


def get_clickhouse_visible_view() -> str:
    """Return the view containing only successfully committed workflows."""

    return os.environ.get(
        "CLICKHOUSE_VISIBLE_VIEW",
        f"{get_clickhouse_table()}_committed",
    )


def get_processed_files_glob() -> str:
    """
    dagster/assets/processing.py, ClickHouse'a INSERT etmeye ek olarak
    işlenmiş veriyi yedek/denetim amacıyla dagster/data/processed/
    klasörüne parquet olarak da yazar. Bu fonksiyon sadece o yedek
    klasörü UI'da bilgi amaçlı göstermek için kullanılır; sorgular
    artık bu dosyaları değil, doğrudan ClickHouse tablosunu okur
    (bkz. get_clickhouse_source()).
    """
    return os.environ.get(
        "CLICKHOUSE_PROCESSED_GLOB",
        "processed/*.parquet",
    )


def get_clickhouse_source() -> str:
    """
    Sorgulanacak veri kaynağını ClickHouse tablosunun tam adı
    (database.table) olarak döner. dagster/assets/clickhouse.py,
    processed_telemetry çıktısını bu tabloya INSERT eder; dashboard
    ise aynı tabloyu sorgu zamanında okur. Bu fonksiyonun döndürdüğü
    ifade, aşağıdaki tüm SELECT/DESCRIBE sorgularında `FROM` yerine
    geçer.
    """
    return (
        f"`{get_clickhouse_database()}`.`{get_clickhouse_visible_view()}`"
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
# MinIO / DVC ARTEFAKT AYARLARI
# ============================================================
#
# "DVC / Veri Sürümleri" sekmesi, pipeline'ın MinIO'ya yazdığı JSON
# artefaktlarını (Great Expectations kalite raporları) ve DVC uzak
# deposundaki (dvc-cache) dizin manifestolarını okur. Aynı ortam
# değişkeni isimleri dagster/assets.py'deki create_minio_client() ile
# uyumludur; docker-compose.yml içinde dashboard servisine eklenir,
# yerelde ise dashboard/.env üzerinden verilebilir.


def get_minio_endpoint() -> str:
    return os.environ.get(
        "MINIO_ENDPOINT",
        "127.0.0.1:9000",
    ).strip()


def get_minio_secure() -> bool:
    return os.environ.get(
        "MINIO_SECURE",
        "false",
    ).strip().lower() in {"1", "true", "yes", "on"}


def get_minio_access_key() -> str:
    return os.environ.get(
        "MINIO_ACCESS_KEY",
        os.environ.get("MINIO_ROOT_USER", "minioadmin"),
    )


def get_minio_secret_key() -> str:
    return os.environ.get(
        "MINIO_SECRET_KEY",
        os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin123"),
    )


def get_pipeline_artifacts_bucket() -> str:
    return os.environ.get(
        "MINIO_ARTIFACT_BUCKET",
        "pipeline-artifacts",
    )


def get_validation_prefix() -> str:
    """
    DVC sekmesinin listeleyeceği önek. Kalite raporu JSON'ları ve onların
    Data Docs bundle'ları bu önekin altındadır;
    _clickhouse-ingest/... altındaki geçici zstd parçaları KAPSAM DIŞI.
    """
    prefix = os.environ.get(
        "MINIO_VALIDATION_PREFIX",
        "validation/",
    ).strip("/")
    return f"{prefix}/" if prefix else ""


def get_dvc_remote_bucket() -> str:
    # dagster tarafında DVC uzak deposu s3://<bucket> biçiminde verilir
    # (docker-compose.yml: DVC_REMOTE_URL=s3://dvc-cache).
    remote_url = os.environ.get("DVC_REMOTE_URL", "s3://dvc-cache").strip()
    if remote_url.startswith("s3://"):
        remote_url = remote_url[len("s3://") :]
    return remote_url.strip("/").split("/", 1)[0] or "dvc-cache"


@st.cache_resource
def get_minio_client():
    """MinIO istemcisini oluşturur (bağlantı testi yapmaz)."""

    if Minio is None:
        raise RuntimeError(
            "`minio` paketi kurulu değil: "
            f"{_MINIO_IMPORT_ERROR}. dashboard/requirements.txt içine "
            "`minio` ekleyip imajı yeniden kurun."
        )

    endpoint = get_minio_endpoint()
    secure = get_minio_secure()

    if "://" in endpoint:
        # http://host:9000 gibi bir değer verilmişse şemayı ayıkla.
        secure = endpoint.split("://", 1)[0].lower() == "https"
        endpoint = endpoint.split("://", 1)[1]

    return Minio(
        endpoint.rstrip("/"),
        access_key=get_minio_access_key(),
        secret_key=get_minio_secret_key(),
        secure=secure,
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
    DESCRIBE TABLE {get_clickhouse_source()}
    """

    try:

        result = client.query(query)

    except Exception as exc:

        # Pipeline henüz hiç çalışmadıysa (clickhouse_auair_batch asset'i
        # tabloyu henüz oluşturmadıysa) ClickHouse "UNKNOWN_TABLE" /
        # "doesn't exist" hatası fırlatır. Bu, gerçek bir bağlantı/yetki
        # hatasından ayırt edilip boş şema olarak ele alınır;
        # render_data_export bu durumda zaten kullanıcı dostu bir uyarı
        # gösteriyor (bkz. "schema.empty" kontrolü).
        exc_text = str(exc)

        if (
            "UNKNOWN_TABLE" in exc_text
            or "doesn't exist" in exc_text
        ):
            return pd.DataFrame()

        raise

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

    internal_columns = {"dagster_run_id"}
    return [
        column
        for column in schema["kolon"].tolist()
        if column not in internal_columns
    ]


# AU-AIR'in kaynak şemasında boylam kolonu "longtitude" olarak yazılıyor.
# Dashboard içinde ise anlaşılır ve standart olan "longitude" adını kullanmaya
# devam ediyoruz. Bu eşleme sayesinde hem mevcut AU-AIR tabloları hem de ileride
# doğru yazımla oluşturulabilecek tablolar aynı dashboard koduyla çalışır.
CLICKHOUSE_COLUMN_ALIASES = {
    "longitude": ("longitude", "longtitude"),
}


def resolve_clickhouse_column(
    logical_name: str,
    available_columns=None,
) -> str | None:
    """Mantıksal dashboard kolonunun ClickHouse'taki gerçek adını döner."""

    available = set(
        get_available_columns()
        if available_columns is None
        else available_columns
    )

    candidates = CLICKHOUSE_COLUMN_ALIASES.get(
        logical_name,
        (logical_name,),
    )

    return next(
        (candidate for candidate in candidates if candidate in available),
        None,
    )


def quote_clickhouse_identifier(identifier: str) -> str:
    """Şemadan gelen bir ClickHouse kolon adını güvenle quote eder."""

    return f"`{identifier.replace('`', '``')}`"


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

    # NOT: file() ile Parquet okurken ClickHouse tüm kolon tiplerini
    # "Nullable(...)" ile sarar (örn. "Nullable(Float64)"); eskiden
    # MergeTree tablosunda tipler sarmalanmadan (örn. "Float64")
    # geliyordu. Bu yüzden prefix kontrolünden önce "Nullable(...)"
    # sarmalayıcısını soyuyoruz.

    def _unwrap_nullable(type_name: str) -> str:

        if type_name.startswith("Nullable(") and type_name.endswith(")"):
            return type_name[len("Nullable("):-1]

        return type_name

    numeric_cols = [
        row["kolon"]
        for _, row in schema.iterrows()
        if _unwrap_nullable(str(row["tip"])).startswith(
            NUMERIC_TYPE_HINTS
        )
    ]

    return numeric_cols


# ============================================================
# CLICKHOUSE FLIGHT / CLASS BİLGİLERİ
# ============================================================

@st.cache_data(ttl=60)
def get_available_classes() -> list:
    """
    "Class" filtresi için seçilebilir değerleri döner.

    - Eski şema (gerçek bir `class` kolonu var): tablodaki DISTINCT
      `class` değerleri.
    - Native frame şeması (`class` kolonu yok ama obj_* sayaç kolonları
      var): tabloda GERÇEKTEN bulunan nesne türleri (human, car, ...);
      filtre bunları `obj_X > 0` olarak uygular (bkz.
      build_clickhouse_where).
    - Hiçbiri yoksa: boş liste (filtre bölümü gizlenir).
    """

    columns = get_available_columns()

    if "class" in columns:

        client = get_clickhouse_client()

        query = f"""
        SELECT DISTINCT class
        FROM {get_clickhouse_source()}
        WHERE class != ''
        ORDER BY class
        """

        result = client.query(query)

        return [
            row[0]
            for row in result.result_rows
        ]

    # class kolonu yok -- obj_* sayaç kolonlarından türet.
    return [
        class_name
        for class_name, obj_column in OBJ_CLASS_COLUMNS.items()
        if obj_column in columns
    ]


@st.cache_data(ttl=60)
def get_available_flights() -> list:
    """
    ClickHouse tablosundaki farklı uçuşları (flight_id) döner.

    Her uçuş bir kaynak dosyaya karşılık gelir (bkz. ingestion.py).
    Tablo bu güncellemeden önce oluşturulmuşsa flight_id kolonu
    olmayabilir; bu durumda boş liste döner ve dashboard uçuş bazlı
    filtre bölümünü otomatik olarak gizler.
    """

    columns = get_available_columns()

    if "flight_id" not in columns:
        return []

    client = get_clickhouse_client()

    query = f"""
    SELECT DISTINCT flight_id
    FROM {get_clickhouse_source()}
    WHERE flight_id != ''
    ORDER BY flight_id
    """

    result = client.query(query)

    return [
        row[0]
        for row in result.result_rows
    ]


# ============================================================
# TIME ARALIĞI
# ============================================================

@st.cache_data(ttl=60)
def get_time_range():
    # Kardeş yardımcılarla (get_available_columns / get_clickhouse_schema
    # / get_lat_lon_bounds ...) aynı şekilde cache'lenir -- eskiden
    # cache'siz olduğu için "Veri Gözat / Dışa Aktar" sekmesinin her
    # rerun'unda (her widget etkileşiminde) ClickHouse'a fazladan bir
    # min/max sorgusu gidiyordu. "🔄 Şimdi yenile" bu cache'i de temizler.

    client = get_clickhouse_client()

    query = f"""
    SELECT
        min(time),
        max(time)
    FROM {get_clickhouse_source()}
    """

    result = client.query(query)

    if not result.result_rows:
        return None, None

    min_time = result.result_rows[0][0]
    max_time = result.result_rows[0][1]

    return min_time, max_time


@st.cache_data(ttl=60)
def get_lat_lon_bounds():
    """
    latitude/longitude kolonlarının min/maks değerlerini döner --
    alan seçim haritasının (render_data_export içindeki "🗺️ Alan
    Bazlı Filtre") başlangıç merkezi/yakınlaştırması, verinin
    kapladığı bölgeye göre ayarlanabilsin diye kullanılır. Kolonlar
    tabloda yoksa (None, None, None, None) döner.
    """

    columns = get_available_columns()

    latitude_column = resolve_clickhouse_column("latitude", columns)
    longitude_column = resolve_clickhouse_column("longitude", columns)

    if latitude_column is None or longitude_column is None:
        return None, None, None, None

    latitude_expr = quote_clickhouse_identifier(latitude_column)
    longitude_expr = quote_clickhouse_identifier(longitude_column)

    client = get_clickhouse_client()

    query = f"""
    SELECT
        min({latitude_expr}), max({latitude_expr}),
        min({longitude_expr}), max({longitude_expr})
    FROM {get_clickhouse_source()}
    WHERE {latitude_expr} IS NOT NULL AND {longitude_expr} IS NOT NULL
    """

    result = client.query(query)

    if not result.result_rows:
        return None, None, None, None

    return result.result_rows[0]


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

# "İki değer arasında" (BETWEEN) filtresi -- yukarıdaki tekli
# operatörlerden farklı olarak iki değer (min/maks) gerektirdiği için
# ayrı ele alınır (bkz. build_clickhouse_where ve render_data_export
# içindeki değer bazlı filtre UI'ı).
RANGE_FILTER_OPERATOR = "between"


# ============================================================
# LLM DOĞAL DİL FİLTRE ENTEGRASYONU (qwen_benchmark.py)
# ============================================================
#
# "🤖 Doğal Dil ile Filtrele" kutusuna yazılan Türkçe sorgu, yerelde
# çalışan Ollama üzerinden qwen3.5:4b modeline gönderilir (bkz.
# qwen_benchmark.py -- SYSTEM_PROMPT, sorgula(), son_islem()). Modelin
# döndürdüğü {"filtreler": [...], "mantik": ..., "zaman_araligi": ...}
# JSON'u burada, panonun TÜM elle doldurulan bölümlerinin karşılık
# geldiği session_state anahtarlarına çevrilir (bkz. render_data_export):
#   - value_filters                                (Değer Bazlı Filtreler)
#   - export_start_time / export_end_time / _mode  (Zaman aralığı)
#   - export_selected_hours / _mode                (gun_ici_saat -> Saat filtresi)
#   - export_duration_*                            (ucus_suresi -> Uçuş süresi)
#   - export_selected_classes                      (Class -- deterministik eşleşme)
#   - export_selected_flights / _mode              (Uçuş -- deterministik eşleşme)
# yani LLM, formu kullanıcının yerine doldurmuş gibi davranır.
#
# ÖNEMLİ SINIRLAMALAR:
#   - qwen_benchmark.py; batarya, sicaklik, motor_devri, basinc gibi
#     alanları da tanıyabiliyor, ancak AU-AIR şemasında bunların karşılığı
#     YOK (bkz. AU_AIR_COLUMNS). Böyle bir alan geçen sorgular sessizce
#     yok sayılmaz, kullanıcıya "desteklenmiyor" olarak bildirilir.
#     "hiz" bunun İSTİSNASIdır -- gerçek bir kolon olmasa da
#     velocity_x/velocity_y'den hesaplanabildiği için desteklenir (bkz.
#     COMPUTED_VALUE_COLUMNS).
#   - Pano, değer bazlı filtrelerde yalnızca "VE" (AND) birleşimini
#     destekliyor (bkz. build_clickhouse_where). Model "mantik": "OR"
#     döndürürse filtreler yine eklenir ama kullanıcıya bunun AND olarak
#     uygulanacağı açıkça belirtilir.
#   - Class, uçuş (flight_id) ve kolon adı eşleştirmesi LLM'DEN GEÇMEZ --
#     bunlar veri setine özgü kapalı kelime listeleri olduğu için sorgu
#     metni, get_available_classes() / get_available_flights() /
#     get_available_columns()'ın döndürdüğü GERÇEK değerlerle
#     deterministik olarak karşılaştırılır (bkz. llm_sinif_filtresini_
#     belirle / llm_ucus_filtresini_belirle / llm_kolon_filtresini_
#     belirle) -- halüsinasyon riski taşımaz ama SYSTEM_PROMPT'ta da yer
#     almaz.
#   - "hariç" / "dışında" tespiti (llm_haric_mi) basit bir yakınlık
#     sezgiselidir, tam bir dilbilgisi çözümleyicisi değildir; birden
#     fazla dışlama içeren karmaşık cümlelerde yanlış eşleşebilir --
#     sonuç her zaman "🔎 Aktif filtre" özetiyle kontrol edilmelidir.
#   - Harita alanı (poligon) filtresi buradan hiç desteklenmiyor --
#     serbest metinden güvenilir bir coğrafi poligon çıkarmak (geocoding
#     gerektirir) kapsam dışı bırakıldı, elle çizilmeye devam ediyor.

try:
    from qwen_benchmark import sorgula as _llm_sorgula
    from qwen_benchmark import son_islem as _llm_son_islem
    _LLM_IMPORT_HATASI = None
except Exception as _llm_import_exc:  # ollama kurulu değil, dosya yok vb.
    _llm_sorgula = None
    _llm_son_islem = None
    _LLM_IMPORT_HATASI = str(_llm_import_exc)


# qwen_benchmark.py'deki "alan" adları -> gerçek ClickHouse kolonu.
# Şemada karşılığı olmayan alanlar (batarya, sicaklik, motor_devri,
# basinc...) BİLEREK burada yok; bkz. yukarıdaki not.
LLM_ALAN_TO_COLUMN = {
    "irtifa": "altitude",
    "yatis_acisi": "angle_phi",
    "yunuslama_acisi": "angle_theta",
    "sapma_acisi": "angle_psi",
    "dikey_hiz": "linear_z",
    "enlem": "latitude",
    "boylam": "longitude",
    "hiz": "hiz",
}

# "hiz" (yer hızı / ground speed), AU-AIR'de tek bir kolon DEĞİLDİR --
# linear_x/linear_y bileşenlerinden hesaplanır (dikey bileşen linear_z
# zaten ayrı olarak dikey_hiz'de karşılanıyor, o yüzden burada yalnızca
# yatay bileşenler kullanılır). "ifade" alanı SABİT,
# kod içinde tanımlı bir SQL ifadesidir (kullanıcı girdisinden gelmez),
# bu yüzden build_clickhouse_where içinde parametre bağlamaya gerek
# duymadan doğrudan sorguya eklenebilir -- injection riski taşımaz.
COMPUTED_VALUE_COLUMNS = {
    "hiz": {
        "ifade": (
            "sqrt(linear_x * linear_x + linear_y * linear_y)"
        ),
        "gereken_kolonlar": ["linear_x", "linear_y"],
    },
}

# qwen_benchmark operatörü -> VALUE_FILTER_OPERATORS / RANGE_FILTER_OPERATOR
LLM_OPERATOR_TO_FILTER_OP = {
    "<": "<",
    "<=": "<=",
    ">": ">",
    ">=": ">=",
    "==": "=",
    "!=": "!=",
    "between": RANGE_FILTER_OPERATOR,
}

# Panonun tanıdığı kanonik operatör kümesi.
_LLM_CANONICAL_OPERATORS = set(VALUE_FILTER_OPERATORS) | {RANGE_FILTER_OPERATOR}


def _llm_normalize_operator(operator):
    """
    qwen'in operatör yazımını (==, =, >, between, ...) panonun kanonik
    operatör kümesine ( <, <=, >, >=, =, !=, between ) indirger.
    Tanınmıyorsa None döner.

    Model bazen "==" bazen "=" döndürebiliyor; ayrıca gun_ici_saat /
    ucus_suresi / deger filtresi yolları eskiden bunu her biri kendi
    içinde farklı ele aldığı için "2 saate eşit" gibi bir ifade değer
    filtresinde çalışırken uçuş süresi filtresinde sessizce
    düşebiliyordu. Tek bir normalize noktası bu tutarsızlığı giderir.
    """

    if operator is None:
        return None

    mapped = LLM_OPERATOR_TO_FILTER_OP.get(operator, operator)

    return mapped if mapped in _LLM_CANONICAL_OPERATORS else None


def llm_gun_ici_saat_filtresini_ayikla(filtreler: list):
    """
    qwen_benchmark'ın "gun_ici_saat" alanıyla döndürdüğü filtreleri
    (ör. "saat 7 ile 9 arası") filtreler listesinden ayırıp, panonun
    "Saat filtresi" (export_selected_hours -> toHour(time) IN (...))
    widget'ının beklediği 0-23 arası tam sayı listesine çevirir.

    "gun_ici_saat" gerçek bir ClickHouse kolonu DEĞİLDİR (time'dan
    türetilir), bu yüzden LLM_ALAN_TO_COLUMN / llm_filtreleri_donustur
    üzerinden değil, ayrı bu fonksiyonla işlenir.

    Döner: (kalan_filtreler, saat_listesi_veya_None, uyari_veya_None)
    """

    kalan = []
    saatler = None
    uyari = None

    for f in (filtreler or []):

        if not isinstance(f, dict) or f.get("alan") != "gun_ici_saat":
            kalan.append(f)
            continue

        operator = f.get("operator")
        deger = f.get("deger")

        if operator is None or deger is None:
            uyari = (
                "Saat filtresi için net bir aralık anlaşılamadı, "
                "atlandı."
            )
            continue

        operator = _llm_normalize_operator(operator)

        if operator is None:
            uyari = (
                f"Saat filtresi için '{f.get('operator')}' operatörü "
                "desteklenmiyor, atlandı."
            )
            continue

        try:

            if operator == RANGE_FILTER_OPERATOR:

                if not isinstance(deger, list) or len(deger) != 2:
                    raise ValueError("aralık iki değer içermiyor")

                bas, bit = int(min(deger)), int(max(deger))
                saatler = list(range(max(bas, 0), min(bit, 23) + 1))

            elif operator == "=":
                saatler = [int(deger)]

            elif operator in (">", ">="):
                alt = int(deger) if operator == ">=" else int(deger) + 1
                saatler = list(range(max(alt, 0), 24))

            elif operator in ("<", "<="):
                ust = int(deger) if operator == "<=" else int(deger) - 1
                saatler = list(range(0, min(ust, 23) + 1))

            else:
                uyari = (
                    f"Saat filtresi için '{operator}' operatörü "
                    "desteklenmiyor, atlandı."
                )
                continue

        except (TypeError, ValueError):
            uyari = (
                f"Saat filtresi için sayısal olmayan bir değer geldi "
                f"({deger!r}), atlandı."
            )
            continue

    return kalan, saatler, uyari


def llm_ucus_suresi_filtresini_ayikla(filtreler: list):
    """
    qwen_benchmark'ın "ucus_suresi" alanıyla döndürdüğü filtreleri
    (ör. "4 saatten kısa süren uçuşlar") filtreler listesinden ayırıp,
    panonun "⏱️ Uçuş süresine göre filtrele" bölümünün kullandığı
    duration_filter sözlüğüne ({"operator": ..., "hours": ...,
    "hours2": ...}) çevirir.

    "ucus_suresi" de gun_ici_saat gibi gerçek bir ClickHouse kolonu
    DEĞİLDİR (her uçuşun min(time)/max(time) farkından hesaplanır),
    bu yüzden LLM_ALAN_TO_COLUMN üzerinden değil, ayrı bu fonksiyonla
    işlenir.

    Döner: (kalan_filtreler, duration_filter_veya_None, uyari_veya_None)
    """

    kalan = []
    duration_filter = None
    uyari = None

    for f in (filtreler or []):

        if not isinstance(f, dict) or f.get("alan") != "ucus_suresi":
            kalan.append(f)
            continue

        ham_operator = f.get("operator")
        deger = f.get("deger")

        if ham_operator is None or deger is None:
            uyari = (
                "Uçuş süresi filtresi için net bir eşik anlaşılamadı, "
                "atlandı."
            )
            continue

        operator = _llm_normalize_operator(ham_operator)

        if operator is None:
            uyari = (
                f"Uçuş süresi filtresi için '{ham_operator}' operatörü "
                "desteklenmiyor, atlandı."
            )
            continue

        try:

            if operator == RANGE_FILTER_OPERATOR:

                if not isinstance(deger, list) or len(deger) != 2:
                    raise ValueError("aralık iki değer içermiyor")

                duration_filter = {
                    "operator": RANGE_FILTER_OPERATOR,
                    "hours": float(min(deger)),
                    "hours2": float(max(deger)),
                }

            else:
                duration_filter = {
                    "operator": operator,
                    "hours": float(deger),
                }

        except (TypeError, ValueError):
            uyari = (
                "Uçuş süresi filtresi için sayısal olmayan bir değer "
                f"geldi ({deger!r}), atlandı."
            )
            continue

    return kalan, duration_filter, uyari


def llm_filtreleri_donustur(
    filtreler: list,
    numeric_columns: list,
    baslangic_id: int = 0,
):
    """
    qwen_benchmark.sorgula() (+ son_islem()) çıktısındaki "filtreler"
    listesini, "🎯 Class ve Değer Bazlı Filtreler" bölümünün kullandığı
    value_filter satırlarına dönüştürür.

    id'ler baslangic_id'den itibaren artan şekilde verilir (0'dan
    başlanmaz) -- aksi halde daha önce elle eklenip silinmiş bir
    filtrenin widget'larına ait eski session_state değerleri (ör.
    value_filter_operator_0), aynı id yeniden kullanıldığında geri
    sızabilir.

    Döner: (value_filter_satirlari, atlanan_aciklamalari)
    """

    satirlar = []
    atlananlar = []
    sonraki_id = baslangic_id

    for f in (filtreler or []):

        if not isinstance(f, dict):
            continue

        alan = f.get("alan")
        operator = f.get("operator")
        deger = f.get("deger")

        column = LLM_ALAN_TO_COLUMN.get(alan)

        if column is None:
            atlananlar.append(
                f"'{alan}' alanının AU-AIR verisinde karşılığı "
                "olmadığı için atlandı."
            )
            continue

        hesaplanan = COMPUTED_VALUE_COLUMNS.get(column)

        if hesaplanan is not None:

            eksik_kolonlar = [
                c
                for c in hesaplanan["gereken_kolonlar"]
                if c not in numeric_columns
            ]

            if eksik_kolonlar:
                atlananlar.append(
                    f"'{column}' hesaplanan alanı için gereken "
                    f"{', '.join(eksik_kolonlar)} kolon(u/ları) "
                    "tabloda bulunamadı, atlandı."
                )
                continue

        elif column not in numeric_columns:
            atlananlar.append(
                f"'{column}' kolonu şu an ClickHouse tablosunda "
                "sayısal olarak bulunamadı, atlandı."
            )
            continue

        if operator is None or deger is None:
            atlananlar.append(
                f"'{column}' için net bir eşik değeri anlaşılamadı "
                "(ör. \"çok yüksek\" gibi belirsiz bir ifade), bu "
                "filtre atlandı."
            )
            continue

        filtre_op = _llm_normalize_operator(operator)

        if filtre_op is None:
            atlananlar.append(
                f"'{column}' için '{operator}' operatörü tanınmadı, "
                "atlandı."
            )
            continue

        satir = {
            "id": sonraki_id,
            "column": column,
            "operator": filtre_op,
            "exclude": False,
        }

        if filtre_op == RANGE_FILTER_OPERATOR:

            if not isinstance(deger, list) or len(deger) != 2:
                atlananlar.append(
                    f"'{column}' için aralık (min-maks) değeri "
                    "anlaşılamadı, atlandı."
                )
                continue

            try:
                satir["value"] = float(min(deger))
                satir["value2"] = float(max(deger))
            except (TypeError, ValueError):
                atlananlar.append(
                    f"'{column}' için sayısal olmayan bir aralık "
                    f"geldi ({deger!r}), atlandı."
                )
                continue

        else:

            try:
                satir["value"] = float(deger)
            except (TypeError, ValueError):
                atlananlar.append(
                    f"'{column}' için sayısal olmayan bir değer "
                    f"geldi ({deger!r}), atlandı."
                )
                continue

        satirlar.append(satir)
        sonraki_id += 1

    return satirlar, atlananlar


_LLM_ZAMAN_SAYI_BIRIM_REGEX = re.compile(r"(\d+)\s*(dakika|saat)", re.I)

# qwen_benchmark'ın SYSTEM_PROMPT'undaki ZAMAN İFADELERİ listesi yalnızca
# GÖRELİ ifadeleri kapsıyor ("son 1 saat", "bugün" vb.) -- kesin takvim
# tarihleri (ör. "20 haziran 2025") o listede YOK. Model yine de -- "aynen
# kopyala" talimatına uyarak -- böyle bir ifadeyi olduğu gibi zaman_araligi
# alanına koyuyor; bu yüzden burada, göreli ifadelerden hiçbiri
# eşleşmediğinde, ayrı bir MUTLAK TARİH ayrıştırıcısına düşülür.

_LLM_TR_AYLAR = {
    "ocak": 1, "şubat": 2, "subat": 2, "mart": 3, "nisan": 4,
    "mayıs": 5, "mayis": 5, "haziran": 6, "temmuz": 7,
    "ağustos": 8, "agustos": 8, "eylül": 9, "eylul": 9,
    "ekim": 10, "kasım": 11, "kasim": 11, "aralık": 12, "aralik": 12,
}

_LLM_TARIH_ISIM_REGEX = re.compile(
    r"(\d{1,2})\s+(" + "|".join(_LLM_TR_AYLAR.keys()) + r")\s+(\d{4})",
    re.I,
)
_LLM_TARIH_SAYISAL_REGEX = re.compile(r"\b(\d{1,2})[./](\d{1,2})[./](\d{4})\b")
_LLM_TARIH_ISO_REGEX = re.compile(r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b")


def _llm_mutlak_tarihleri_bul(ifade: str):
    """
    ifade içinde geçen kesin (mutlak) tarihleri, cümledeki geçiş
    sırasına göre bir datetime.date listesi olarak döner. "20 haziran
    2025", "20.06.2025", "2025-06-20" biçimlerini tanır. Geçersiz bir
    tarih (ör. 32. gün) sessizce atlanır.
    """

    bulunanlar = []

    for eslesme in _LLM_TARIH_ISIM_REGEX.finditer(ifade):
        try:
            bulunanlar.append((
                eslesme.start(),
                date(
                    int(eslesme.group(3)),
                    _LLM_TR_AYLAR[_tr_kucuk(eslesme.group(2))],
                    int(eslesme.group(1)),
                ),
            ))
        except ValueError:
            continue

    for eslesme in _LLM_TARIH_SAYISAL_REGEX.finditer(ifade):
        try:
            gun, ay, yil = (int(g) for g in eslesme.groups())
            bulunanlar.append((eslesme.start(), date(yil, ay, gun)))
        except ValueError:
            continue

    for eslesme in _LLM_TARIH_ISO_REGEX.finditer(ifade):
        try:
            yil, ay, gun = (int(g) for g in eslesme.groups())
            bulunanlar.append((eslesme.start(), date(yil, ay, gun)))
        except ValueError:
            continue

    bulunanlar.sort(key=lambda x: x[0])

    return [tarih for _, tarih in bulunanlar]


def llm_zaman_araligini_coz(ifade: str, referans: datetime):
    """
    qwen_benchmark'ın ürettiği "zaman_araligi" ifadesini (göreli --
    "son 10 dakika", "bugün", "dün"-- ya da mutlak -- "20 haziran
    2025", "20 haziran 2025 ile 19 eylül 2025 arası" --) (baslangic,
    bitis) datetime çiftine çevirir.

    "referans" gerçek saat DEĞİL, verideki EN SON zaman damgası
    (get_time_range()'in döndürdüğü max_time) olmalıdır -- aksi halde
    geçmişe ait bir telemetri kümesinde göreli ifadeler ("son 1 saat")
    her zaman boş sonuç döner.

    Çözümlenemezse None döner (zaman aralığı değiştirilmez).
    """

    if not ifade:
        return None

    ifade_l = ifade.strip().lower()

    if "yarım saat" in ifade_l:
        return referans - timedelta(minutes=30), referans

    if ifade_l == "bugün":
        gun_baslangic = datetime.combine(
            referans.date(), datetime.min.time()
        )
        return gun_baslangic, referans

    if ifade_l == "dün":
        dun = referans.date() - timedelta(days=1)
        return (
            datetime.combine(dun, datetime.min.time()),
            datetime.combine(dun, datetime.max.time()),
        )

    if "bu sabah" in ifade_l:
        gun_baslangic = datetime.combine(
            referans.date(), datetime.min.time()
        )
        return gun_baslangic + timedelta(hours=6), referans

    eslesme = _LLM_ZAMAN_SAYI_BIRIM_REGEX.search(ifade_l)

    if eslesme:
        sayi = int(eslesme.group(1))
        birim = eslesme.group(2)
        delta = (
            timedelta(minutes=sayi)
            if birim == "dakika"
            else timedelta(hours=sayi)
        )
        return referans - delta, referans

    # Hiçbir göreli kalıp eşleşmedi -- mutlak takvim tarihi(leri) dene.

    mutlak_tarihler = _llm_mutlak_tarihleri_bul(ifade)

    if len(mutlak_tarihler) >= 2:

        bas, bit = min(mutlak_tarihler), max(mutlak_tarihler)

        return (
            datetime.combine(bas, datetime.min.time()),
            datetime.combine(bit, datetime.max.time()),
        )

    if len(mutlak_tarihler) == 1:

        tek = mutlak_tarihler[0]

        return (
            datetime.combine(tek, datetime.min.time()),
            datetime.combine(tek, datetime.max.time()),
        )

    return None


# ------------------------------------------------------------
# SINIF (class) VE UÇUŞ (flight_id) EŞLEŞTİRME
# ------------------------------------------------------------
#
# Bu ikisi, qwen3.5:4b'nin önceden BİLEMEYECEĞİ, veri setine özgü
# kapalı kelime listeleridir (class değerleri AU-AIR etiketleridir,
# flight_id'ler ingestion sırasında dosya adından türetilir -- bkz.
# ingestion.py). Modele bunları "tahmin ettirmek" yerine, sorgu
# metninde GEÇEN kelimeler doğrudan get_available_classes() /
# get_available_flights()'ın döndürdüğü GERÇEK değerlerle (ve birkaç
# yaygın Türkçe eş anlamlıyla) karşılaştırılır -- halüsinasyon riski
# taşımayan, deterministik bir eşleştirme; bu yüzden SYSTEM_PROMPT'a
# eklenmedi.

LLM_SINIF_ESANLAMLI = {
    "insan": "human", "yaya": "human", "kişi": "human", "kisi": "human",
    "araba": "car", "otomobil": "car", "araç": "car", "arac": "car",
    "kamyon": "truck",
    "van": "van", "minibüs": "van", "minibus": "van",
    "motosiklet": "motorbike", "motor": "motorbike",
    "bisiklet": "bicycle",
    "otobüs": "bus", "otobus": "bus",
    "römork": "trailer", "romork": "trailer", "treyler": "trailer",
}


def _tr_kucuk(metin: str) -> str:
    """
    Türkçe'ye duyarlı küçük harfe çevirme. Python'un standart
    str.lower() metodu BÜYÜK "İ" harfini "i" değil "i̇" (nokta
    işaretiyle birleşik, iki karakter) yapar -- bu da "İnsan" gibi bir
    kelimenin "insan" eş anlamlısıyla eşleşmesini SESSİZCE bozar. Bu
    yüzden Türkçe metin karşılaştırmalarında (kelime eşleştirme,
    "hariç" sezgiseli) her zaman .lower() yerine bu fonksiyon
    kullanılır.
    """

    return metin.replace("İ", "i").replace("I", "ı").lower()


def _llm_kelime_gecer_mi(kelime: str, metin: str) -> bool:

    if not kelime:
        return False

    return re.search(
        r"(?<![\wğüşıöçİĞÜŞÖÇ])"
        + re.escape(_tr_kucuk(kelime))
        + r"(?![\wğüşıöçİĞÜŞÖÇ])",
        _tr_kucuk(metin),
    ) is not None


def llm_sinif_filtresini_belirle(sorgu: str, mevcut_siniflar: list):
    """
    Sorgu metninde geçen class adlarını (doğrudan ya da Türkçe eş
    anlamlısıyla) mevcut_siniflar (get_available_classes()) ile
    eşleştirir. Eşleşme yoksa boş liste döner (mevcut tüm class'lar
    dahil edilmiş sayılır -- bkz. multiselect'in "boş = hepsi" kuralı).
    """

    if not sorgu or not mevcut_siniflar:
        return []

    eslesenler = []
    mevcut_kucuk = {_tr_kucuk(c): c for c in mevcut_siniflar}

    for sinif_kucuk, gercek in mevcut_kucuk.items():

        if (
            _llm_kelime_gecer_mi(sinif_kucuk, sorgu)
            and gercek not in eslesenler
        ):
            eslesenler.append(gercek)

    for esanlamli, ingilizce in LLM_SINIF_ESANLAMLI.items():

        if not _llm_kelime_gecer_mi(esanlamli, sorgu):
            continue

        gercek = mevcut_kucuk.get(_tr_kucuk(ingilizce))

        if gercek and gercek not in eslesenler:
            eslesenler.append(gercek)

    return eslesenler


def llm_ucus_filtresini_belirle(sorgu: str, mevcut_ucuslar: list):
    """
    Sorgu metninde GEÇEN uçuş kimliklerini (ör. "flight_2") doğrudan
    mevcut_ucuslar (get_available_flights()) ile eşleştirir.
    """

    if not sorgu or not mevcut_ucuslar:
        return []

    return [
        ucus
        for ucus in mevcut_ucuslar
        if _llm_kelime_gecer_mi(ucus, sorgu)
    ]


# ------------------------------------------------------------
# KOLON SEÇİMİ ("sadece X kolonunu göster")
# ------------------------------------------------------------
#
# Bu bir SATIR FİLTRESİ değildir -- hangi satırların geleceğini değil,
# gelen satırlarda hangi KOLONLARIN gösterileceğini/dışa aktarılacağını
# belirler (bkz. "📋 Kolonlar"). LLM'e sorulmaz (class/flight_id gibi
# deterministik eşleştirilir); üstelik yanlışlıkla HER filtre
# sorgusunda tetiklenmemesi için yalnızca cümlede "kolon" kelimesiyle
# birlikte "sadece/yalnızca" ya da "hariç/dışında" geçtiğinde devreye
# girer -- aksi halde "irtifası 300'ün altındaki kayıtlar" gibi sıradan
# bir filtre sorgusu bile yanlışlıkla kolon seçimi sanılabilirdi.

LLM_KOLON_ESANLAMLI = {
    "zaman": "time", "tarih": "time",
    "enlem": "latitude", "lat": "latitude",
    "boylam": "longitude", "lon": "longitude", "lng": "longitude",
    "irtifa": "altitude", "yükseklik": "altitude",
    "yukseklik": "altitude", "rakım": "altitude", "rakim": "altitude",
    "yatış açısı": "angle_phi", "yatis acisi": "angle_phi",
    "yunuslama açısı": "angle_theta", "yunuslama acisi": "angle_theta",
    "sapma açısı": "angle_psi", "sapma acisi": "angle_psi",
    "dikey hız": "linear_z", "dikey hiz": "linear_z",
    "uçuş kimliği": "flight_id", "ucus kimligi": "flight_id",
    "uçuş no": "flight_id", "ucus no": "flight_id",
    "görüntü adı": "image_name", "goruntu adi": "image_name",
    "resim adı": "image_name", "resim adi": "image_name",
    "platform": "platform",
    "nesne sayısı": "num_objects", "nesne sayisi": "num_objects",
    "insan sayısı": "obj_human", "insan sayisi": "obj_human",
    "araba sayısı": "obj_car", "araba sayisi": "obj_car",
    "kamyon sayısı": "obj_truck", "kamyon sayisi": "obj_truck",
    "minibüs sayısı": "obj_van", "minibus sayisi": "obj_van",
    "motosiklet sayısı": "obj_motorbike", "motosiklet sayisi": "obj_motorbike",
    "bisiklet sayısı": "obj_bicycle", "bisiklet sayisi": "obj_bicycle",
    "otobüs sayısı": "obj_bus", "otobus sayisi": "obj_bus",
    "römork sayısı": "obj_trailer", "romork sayisi": "obj_trailer",
}

_LLM_KOLON_KELIMESI = re.compile(r"\bkolon(u|unu|ları|larını|lar)?\b", re.I)
_LLM_SADECE_KELIMELER = re.compile(r"\bsadece\b|\byalnızca\b|\byalniz\b", re.I)


def llm_kolon_secimi_istegi_mi(sorgu: str) -> bool:
    """
    Sorgunun bir kolon seçimi/dışa aktarma isteği olup olmadığını
    belirler -- "kolon" kelimesi TEK BAŞINA yeterli sayılmaz (yanlış
    pozitif riski yüksek), "sadece/yalnızca" ya da "hariç/dışında" ile
    BİRLİKTE geçmesi aranır.
    """

    if not sorgu or not _LLM_KOLON_KELIMESI.search(sorgu):
        return False

    return bool(
        _LLM_SADECE_KELIMELER.search(sorgu)
        or _LLM_HARIC_KELIMELER.search(sorgu)
    )


def llm_kolon_filtresini_belirle(sorgu: str, mevcut_kolonlar: list):
    """
    Sorgu metninde geçen kolon adlarını (doğrudan gerçek kolon adıyla
    ya da Türkçe eş anlamlısıyla) mevcut_kolonlar (get_available_columns())
    ile eşleştirir.
    """

    if not sorgu or not mevcut_kolonlar:
        return []

    eslesenler = []
    mevcut_kucuk = {_tr_kucuk(c): c for c in mevcut_kolonlar}

    for kolon_kucuk, gercek in mevcut_kucuk.items():

        if (
            _llm_kelime_gecer_mi(kolon_kucuk, sorgu)
            and gercek not in eslesenler
        ):
            eslesenler.append(gercek)

    for esanlamli, gercek_ad in LLM_KOLON_ESANLAMLI.items():

        if not _llm_kelime_gecer_mi(esanlamli, sorgu):
            continue

        gercek = mevcut_kucuk.get(_tr_kucuk(gercek_ad))

        if gercek and gercek not in eslesenler:
            eslesenler.append(gercek)

    return eslesenler


_LLM_HARIC_KELIMELER = re.compile(
    r"hariç|haric|dışında|disinda|dışındaki|disindaki", re.I
)


def llm_haric_mi(
    sorgu: str,
    anahtar: str,
    diger_ogeler=None,
    pencere: int = 20,
) -> bool:
    """
    "anahtar" (bir sınıf adı, uçuş kimliği ya da zaman_araligi ifadesi
    -- model bunu cümledeki haliyle AYNEN döndürür) sorgu metninde
    geçtiği yerin HEMEN ARDINDAN (ör. "flight_2 hariç", "İnsan
    dışındaki") bir dışlama kelimesi gelip gelmediğine bakar. Türkçede
    "hariç"/"dışında" neredeyse her zaman değiştirdiği öğeden SONRA
    geldiği için pencere yalnızca İLERİYE doğru açılır -- geriye doğru
    bakmak, "flight_1 ve flight_2 hariç" gibi bir cümlede "hariç"i
    yanlışlıkla flight_1'e de mal ederdi.

    "diger_ogeler" verilirse (aynı sorguda tespit edilen DİĞER
    sınıf/uçuş adları), pencere içinde dışlama kelimesinden ÖNCE başka
    bir öge geçiyorsa dışlama o öğeye ait sayılır, bu "anahtar" için
    False döner -- yukarıdaki "flight_1 ve flight_2 hariç" örneğinde
    flight_1 için False, flight_2 için True döner.

    Kesin bir dilbilgisi çözümleyicisi DEĞİLDİR, yalnızca yaygın
    kalıpları yakalayan bir sezgiseldir; sonuç her zaman panonun
    "🔎 Aktif filtre" özetiyle kontrol edilmelidir.
    """

    if not sorgu or not anahtar:
        return False

    sorgu_kucuk = _tr_kucuk(sorgu)
    anahtar_kucuk = _tr_kucuk(anahtar)
    konum = sorgu_kucuk.find(anahtar_kucuk)

    if konum == -1:
        return False

    ileri_baslangic = konum + len(anahtar_kucuk)
    ileri_bitis = min(len(sorgu_kucuk), ileri_baslangic + pencere)
    parca = sorgu_kucuk[ileri_baslangic:ileri_bitis]

    eslesme = _LLM_HARIC_KELIMELER.search(parca)

    if not eslesme:
        return False

    for oge in (diger_ogeler or []):

        oge_kucuk = _tr_kucuk(oge)

        if oge_kucuk == anahtar_kucuk:
            continue

        oge_konum = parca.find(oge_kucuk)

        if oge_konum != -1 and oge_konum < eslesme.start():
            return False

    return True


def _polygon_has_area(polygon) -> bool:
    """
    Bir poligonun (köşe noktası listesi) ClickHouse'un pointInPolygon
    fonksiyonuna güvenle gönderilebilecek gerçek bir alanı olup
    olmadığını kontrol eder.

    Haritada fare sürüklenmeden (tek tıkla) çizilen bir dikdörtgen gibi
    durumlarda tüm köşeler aynı noktaya ya da tek bir doğru üzerine denk
    gelebilir -- bu "poligon" ClickHouse'a gönderildiğinde "Polygon is
    not valid: Geometry has wrong topological dimension" hatasıyla
    sorgunun tamamen patlamasına yol açar. Bu fonksiyon, kapanış
    noktası (ilk = son) ve ardışık tekrarlar hariç en az 3 FARKLI köşesi
    olan VE bu köşelerin tek bir doğru üzerinde olmadığı (shoelace
    formülüyle alanı sıfırdan farklı) poligonları geçerli sayar.
    """

    if not polygon:
        return False

    distinct_points = []

    for point in polygon:
        if not distinct_points or distinct_points[-1] != tuple(point):
            distinct_points.append(tuple(point))

    if len(distinct_points) > 1 and distinct_points[0] == distinct_points[-1]:
        distinct_points.pop()

    if len(distinct_points) < 3:
        return False

    signed_area_x2 = 0.0
    point_count = len(distinct_points)

    for index in range(point_count):
        x1, y1 = distinct_points[index]
        x2, y2 = distinct_points[(index + 1) % point_count]
        signed_area_x2 += x1 * y2 - x2 * y1

    return abs(signed_area_x2) > 1e-12


class _SeedIntoDrawLayer(folium.MacroElement):
    """
    Paylaşılan bağlantıdan gelen alan(lar)ı, Leaflet.draw'ın KENDİ
    düzenlenebilir katmanına (`drawnItems`) enjekte eder -- böylece
    kullanıcı bu alan(lar)ı haritanın kendi çöp kutusu/düzenleme
    aracıyla, tıpkı kendi çizdiği bir şekil gibi seçip silebilir.

    Önceden bu alan(lar) düz bir `folium.Polygon` katmanı olarak
    haritaya EKLENİYORDU ama `Draw` eklentisinin yönettiği katmanın
    DIŞINDA kalıyordu -- bu yüzden haritanın silme aracı onları hiç
    "görmüyordu" (tıklama "Click on a feature to remove" durumunda
    hiçbir şeye denk gelmiyordu). streamlit-folium, `Draw`'ın oluşturduğu
    `drawnItems_...` değişkenini regex ile global `drawnItems`'a
    yeniden adlandırır (bkz. streamlit_folium._get_map_string); bu
    script o global değişkene enjekte eder, Draw'ın kendi script'inden
    SONRA çalışacak şekilde (render sırası `Draw(...).add_to(map)`
    çağrısından SONRA bu sınıfın eklenmesine bağlıdır).
    """

    _template = Template(
        """
        {% macro script(this, kwargs) %}
        (function() {
            var seedFeatures = {{ this.geojson_features }};
            var seedStyle = {
                color: "#eda100",
                weight: 2,
                dashArray: "6,4",
                fillOpacity: 0.15
            };
            var attemptsLeft = 40;
            function trySeed() {
                if (typeof drawnItems !== "undefined" && drawnItems.addLayer) {
                    seedFeatures.forEach(function(feature) {
                        L.geoJSON(
                            feature,
                            {style: function() { return seedStyle; }}
                        ).eachLayer(function(layer) {
                            drawnItems.addLayer(layer);
                        });
                    });
                } else if (attemptsLeft > 0) {
                    attemptsLeft -= 1;
                    setTimeout(trySeed, 50);
                }
            }
            trySeed();
        })();
        {% endmacro %}
        """
    )

    def __init__(self, polygons):

        super().__init__()

        self._name = "SeedIntoDrawLayer"

        features = [
            {
                "type": "Feature",
                "properties": {},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [[lon, lat] for lon, lat in polygon]
                        + [[polygon[0][0], polygon[0][1]]]
                    ],
                },
            }
            for polygon in polygons
        ]

        self.geojson_features = json.dumps(features)


def build_clickhouse_where(
    start_time=None,
    end_time=None,
    selected_classes=None,
    value_filters=None,
    selected_flights=None,
    area_polygons=None,
    selected_hours=None,
    duration_filter=None,
    area_mode="include",
    time_mode="include",
    hours_mode="include",
    flights_mode="include",
):
    """
    value_filters: [{"column": "altitude", "operator": "<", "value": 23,
    "exclude": False}, ...]. "between" operatörü için ayrıca "value2"
    (maks) de gerekir, örn. {"column": "altitude", "operator": "between",
    "value": 10, "value2": 50} -> "altitude BETWEEN 10 AND 50".
    "exclude": True verilirse koşul NOT (...) ile tersine çevrilir (örn.
    "altitude 10-50 aralığında DEĞİL" -> altitude < 10 OR altitude > 50
    ile aynı anlama gelir, ama tek bir NOT BETWEEN ile ifade edilir).
    selected_flights: ["flight_1", "flight_2", ...] -> flight_id IN (...);
    flights_mode="exclude" ise flight_id NOT IN (...) (örn. "1. uçuş
    dışındaki uçuşlar" için o uçuş seçilip flights_mode="exclude" verilir).
    selected_hours: [7, 8, ...] -> toHour(time) IN (...). Tarihten/uçuştan
    bağımsız olarak, günün belirli saatlerindeki satırları filtreler
    (örn. tüm uçuşlarda saat 07:00-07:59 arasına denk gelen satırlar).
    hours_mode="exclude" ise toHour(time) NOT IN (...) (örn. "18:00-21:00
    aralığı dışındaki satırlar" için 18/19/20 seçilip hours_mode=
    "exclude" verilir).
    time_mode="exclude" ise start_time/end_time aralığı tersine çevrilir
    -- yalnızca bu aralığın DIŞINDaki (start_time'dan önceki ya da
    end_time'dan sonraki) satırlar tutulur.
    duration_filter: {"operator": "<", "hours": 4} -> her uçuşun
    min(time)/max(time) farkına (saniye) göre hesaplanan süresi bu
    koşulu sağlamıyorsa o uçuşun TÜM satırları elenir (örn. "4 saatten
    kısa uçuşları filtrele" -> {"operator": "<", "hours": 4}).
    "between" operatörü için ayrıca "hours2" (maks saat) gerekir.
    area_polygons: [[(lon1, lat1), (lon2, lat2), ...], ...] -- haritada
    çizilen bir ya da birden fazla alanın köşe noktaları (her poligon en
    az 3 nokta). ClickHouse'un pointInPolygon fonksiyonuyla, bu
    poligonlardan HERHANGİ BİRİNİN içinde kalan (longitude, latitude)
    satırları eşleştirilir (poligonlar OR ile birleştirilir; yani "bölge
    A veya bölge B"). Her poligonun nokta listesi, clickhouse_connect'in
    parametre bağlamada (client-side) doğrudan desteklemediği
    Tuple(...) tipinden kaçınmak için iki ayrı Array(Float64) (boylam/
    enlem) parametresi olarak gönderilir ve sorgu içinde arrayZip ile
    Array(Tuple(...))'a dönüştürülür.
    area_mode: "include" (varsayılan) çizilen alan(lar)IN İÇİNDEKİ
    satırları tutar; "exclude" bunun tersini yapar -- çizilen alan(lar)IN
    HİÇBİRİNE hiç GİRMEMİŞ uçuşların TÜM satırlarını tutar (örn. "Erzurum
    dışındaki uçuşlar" için Erzurum'u çevreleyen bir alan çizip "exclude"
    seçilir). Bu, satır bazında bir NOT ile değil, flight_id bazında bir
    alt sorguyla yapılır -- aksi halde bölgeye giren bir uçuşun poligon
    dışındaki satırları sonuçta kalır ve o uçuş yanlışlıkla "bölgeye hiç
    girmemiş" gibi görünür (flight_id kolonu yoksa eski satır bazlı NOT'a
    geri düşülür).

    Her filtre AND ile birleştirilir (örn. "altitude < 23 AND box_w >= 50").
    Kolon adı ve operatör beyaz listeye (whitelist) karşı doğrulanır,
    değer ise ClickHouse parametre binding'i ile geçirilir; bu sayede
    SQL injection riski oluşmaz.
    """

    conditions = []
    parameters = {}

    if start_time is not None and end_time is not None:

        parameters["start_time"] = start_time
        parameters["end_time"] = end_time

        if time_mode == "exclude":
            conditions.append(
                "NOT (time BETWEEN {start_time:DateTime} "
                "AND {end_time:DateTime})"
            )
        else:
            conditions.append("time >= {start_time:DateTime}")
            conditions.append("time <= {end_time:DateTime}")

    elif start_time is not None:

        parameters["start_time"] = start_time

        conditions.append(
            "time < {start_time:DateTime}"
            if time_mode == "exclude"
            else "time >= {start_time:DateTime}"
        )

    elif end_time is not None:

        parameters["end_time"] = end_time

        conditions.append(
            "time > {end_time:DateTime}"
            if time_mode == "exclude"
            else "time <= {end_time:DateTime}"
        )

    if selected_classes:

        available_columns = set(
            get_available_columns()
        )

        if "class" in available_columns:

            # Eski şema: satır bazlı gerçek `class` etiketi.
            conditions.append(
                "class IN {classes:Array(String)}"
            )

            parameters["classes"] = [
                str(x)
                for x in selected_classes
            ]

        else:

            # Native frame şeması: `class` kolonu yok -- seçilen nesne
            # türlerinin obj_* sayaç kolonları üzerinden "frame'de en az
            # 1 adet var" (obj_X > 0) koşulu kurulur; birden fazla tür
            # seçilirse OR ile birleştirilir ("insan VEYA araba içeren
            # frame'ler"). Kolon adları sabit OBJ_CLASS_COLUMNS
            # sözlüğünden gelip tabloya karşı doğrulandığı için (kullanıcı
            # girdisi değil) parametre bağlamaya gerek yoktur.
            obj_conditions = []

            for class_name in selected_classes:

                obj_column = OBJ_CLASS_COLUMNS.get(str(class_name))

                if obj_column and obj_column in available_columns:
                    obj_conditions.append(f"`{obj_column}` > 0")

            if obj_conditions:
                conditions.append(
                    "(" + " OR ".join(obj_conditions) + ")"
                )

    if selected_flights:

        conditions.append(
            "flight_id NOT IN {flight_ids:Array(String)}"
            if flights_mode == "exclude"
            else "flight_id IN {flight_ids:Array(String)}"
        )

        parameters["flight_ids"] = [
            str(x)
            for x in selected_flights
        ]

    if selected_hours:

        conditions.append(
            "toHour(time) NOT IN {hours:Array(UInt8)}"
            if hours_mode == "exclude"
            else "toHour(time) IN {hours:Array(UInt8)}"
        )

        parameters["hours"] = [
            int(hour)
            for hour in selected_hours
        ]

    if duration_filter:

        available_columns = set(
            get_available_columns()
        )

        operator = duration_filter.get("operator")
        hours = duration_filter.get("hours")

        if (
            "flight_id" in available_columns
            and hours is not None
            and (
                operator in VALUE_FILTER_OPERATORS
                or operator == RANGE_FILTER_OPERATOR
            )
        ):

            duration_subquery = (
                "flight_id IN ("
                "SELECT flight_id FROM "
                f"{get_clickhouse_source()} "
                "GROUP BY flight_id "
                "HAVING dateDiff('second', min(time), max(time)) "
            )

            if operator == RANGE_FILTER_OPERATOR:

                hours2 = duration_filter.get("hours2")

                if hours2 is not None:

                    conditions.append(
                        duration_subquery
                        + "BETWEEN {duration_min_sec:UInt64} "
                        "AND {duration_max_sec:UInt64})"
                    )

                    parameters["duration_min_sec"] = int(
                        min(hours, hours2) * 3600
                    )
                    parameters["duration_max_sec"] = int(
                        max(hours, hours2) * 3600
                    )

            else:

                conditions.append(
                    duration_subquery
                    + f"{VALUE_FILTER_OPERATORS[operator]} "
                    "{duration_sec:UInt64})"
                )

                parameters["duration_sec"] = int(hours * 3600)

    valid_area_polygons = [
        polygon
        for polygon in (area_polygons or [])
        if polygon and _polygon_has_area(polygon)
    ]

    if valid_area_polygons:

        available_columns = set(
            get_available_columns()
        )

        latitude_column = resolve_clickhouse_column(
            "latitude",
            available_columns,
        )
        longitude_column = resolve_clickhouse_column(
            "longitude",
            available_columns,
        )

        if latitude_column is not None and longitude_column is not None:

            latitude_expr = quote_clickhouse_identifier(latitude_column)
            longitude_expr = quote_clickhouse_identifier(longitude_column)

            area_conditions = []

            for index, polygon in enumerate(valid_area_polygons):

                lon_param = f"area_lons_{index}"
                lat_param = f"area_lats_{index}"

                area_conditions.append(
                    f"pointInPolygon(({longitude_expr}, {latitude_expr}), "
                    f"arrayZip({{{lon_param}:Array(Float64)}}, "
                    f"{{{lat_param}:Array(Float64)}}))"
                )

                parameters[lon_param] = [
                    float(lon) for lon, lat in polygon
                ]
                parameters[lat_param] = [
                    float(lat) for lon, lat in polygon
                ]

            # Birden fazla poligon OR ile birleştirilir (bölge A veya
            # bölge B içinde kalan satırlar eşleşir); tüm poligon
            # koşulları da tek bir AND'lenebilir grup olması için
            # parantez içine alınır.
            area_group = "(" + " OR ".join(area_conditions) + ")"

            if area_mode == "exclude":

                if "flight_id" in available_columns:

                    # Satır bazında "NOT (...)" burada YANLIŞ olur: bir
                    # uçuş poligona girmiş olsa bile, o uçuşun poligon
                    # DIŞINDAKİ satırları hâlâ NOT koşulunu sağlar ve
                    # sonuçta kalır -- yani bölgeye giren bir uçuş
                    # yanlışlıkla "bölgeye hiç girmemiş" gibi görünür.
                    # Bunun yerine, poligon(lar)a hiç girmemiş
                    # flight_id'ler flight_id bazında bulunup TÜM
                    # satırları o uçuşlar üzerinden tutulur.
                    area_group = (
                        "flight_id NOT IN (SELECT DISTINCT flight_id "
                        f"FROM {get_clickhouse_source()} WHERE "
                        f"{area_group})"
                    )

                else:

                    area_group = "NOT " + area_group

            conditions.append(area_group)

    if value_filters:

        available_columns = set(
            get_available_columns()
        )

        for index, value_filter in enumerate(value_filters):

            column = value_filter.get("column")
            operator = value_filter.get("operator")
            value = value_filter.get("value")
            exclude = value_filter.get("exclude", False)

            hesaplanan = COMPUTED_VALUE_COLUMNS.get(column)

            if hesaplanan is not None:

                # Gerçek bir kolon DEĞİL -- sabit, kod içinde tanımlı
                # bir SQL ifadesi (kullanıcı girdisinden gelmez, bu
                # yüzden parametre bağlamaya gerek yok / injection
                # riski taşımaz). Yalnızca ifadenin dayandığı ham
                # kolonlar (ör. velocity_x, velocity_y) tabloda
                # gerçekten varsa uygulanır.

                if not all(
                    c in available_columns
                    for c in hesaplanan["gereken_kolonlar"]
                ):
                    continue

                kolon_ifadesi = f"({hesaplanan['ifade']})"

            else:

                physical_column = resolve_clickhouse_column(
                    column,
                    available_columns,
                )

                if physical_column is None:
                    continue

                kolon_ifadesi = quote_clickhouse_identifier(
                    physical_column
                )

            if operator == RANGE_FILTER_OPERATOR:

                value2 = value_filter.get("value2")

                if value is None or value2 is None:
                    continue

                param_min = f"value_filter_{index}_min"
                param_max = f"value_filter_{index}_max"

                between_expr = (
                    f"{kolon_ifadesi} BETWEEN "
                    f"{{{param_min}:Float64}} AND {{{param_max}:Float64}}"
                )

                conditions.append(
                    f"NOT ({between_expr})" if exclude else between_expr
                )

                # Kullanıcı min/maks'ı ters girmiş olsa bile (örn. min=50,
                # maks=10) BETWEEN'in boş sonuç dönmemesi için sıralanır.
                parameters[param_min] = float(min(value, value2))
                parameters[param_max] = float(max(value, value2))

                continue

            if operator not in VALUE_FILTER_OPERATORS:
                continue

            if value is None:
                continue

            param_name = f"value_filter_{index}"

            comparison_expr = (
                f"{kolon_ifadesi} "
                f"{VALUE_FILTER_OPERATORS[operator]} "
                f"{{{param_name}:Float64}}"
            )

            conditions.append(
                f"NOT ({comparison_expr})" if exclude else comparison_expr
            )

            parameters[param_name] = float(value)

    if not conditions:
        return "1 = 1", parameters

    return (
        " AND ".join(conditions),
        parameters,
    )


# ============================================================
# SELECT PROJEKSİYONU (kolon listesi -> SQL)
# ============================================================

def _build_select_projection(columns=None) -> str:
    """
    Verilen kolon listesini `SELECT DISTINCT ...` için bir SQL
    projeksiyonuna çevirir. `columns` boşsa ya da hiçbiri tabloda yoksa
    "*" döner; aksi halde yalnızca tabloda GERÇEKTEN var olan kolonlar
    backtick'lenerek döner.

    Hem count_filtered_rows hem fetch_filtered_telemetry AYNI
    projeksiyonu kullanır -- aksi halde satır sayısı `DISTINCT *`
    üzerinden, getirilen veri ise `DISTINCT <seçili kolonlar>` üzerinden
    hesaplanıp ikisi birbirini tutmuyordu (kolon seçimi daraltıldığında
    "Filtreye uyan satır sayısı", sonra getirilen/indirilen satır
    sayısından çok daha büyük çıkabiliyordu).
    """

    if not columns:
        return "*"

    available = set(get_available_columns())

    # Mantıksal kolon adları resolve_clickhouse_column ile fiziksel
    # ClickHouse adlarına çözülür (örn. "longitude" -> "longtitude");
    # ad değişiyorsa `AS` ile mantıksal ada geri takma ad verilir, böylece
    # fetch_filtered_telemetry ile birebir aynı projeksiyon üretilir.
    projection = []

    for logical_column in columns:

        physical_column = resolve_clickhouse_column(
            logical_column,
            available,
        )

        if physical_column is None:
            continue

        physical_expr = quote_clickhouse_identifier(physical_column)

        if physical_column != logical_column:
            physical_expr += (
                " AS " + quote_clickhouse_identifier(logical_column)
            )

        projection.append(physical_expr)

    if not projection:
        return "*"

    return ", ".join(projection)


# ============================================================
# SATIR SAYISI
# ============================================================

def count_filtered_rows(
    start_time=None,
    end_time=None,
    selected_classes=None,
    columns=None,
    value_filters=None,
    selected_flights=None,
    area_polygons=None,
    selected_hours=None,
    duration_filter=None,
    area_mode="include",
    time_mode="include",
    hours_mode="include",
    flights_mode="include",
):

    client = get_clickhouse_client()

    where, parameters = build_clickhouse_where(
        start_time,
        end_time,
        selected_classes,
        value_filters,
        selected_flights,
        area_polygons,
        selected_hours,
        duration_filter,
        area_mode,
        time_mode,
        hours_mode,
        flights_mode,
    )

    # NOT: projeksiyon fetch_filtered_telemetry ile AYNI olmalı --
    # yoksa gösterilen satır sayısı getirilen veriyle uyuşmaz.
    projection = _build_select_projection(columns)

    query = f"""
    SELECT count() FROM (
        SELECT DISTINCT {projection}
        FROM {get_clickhouse_source()}
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
    selected_flights=None,
    area_polygons=None,
    selected_hours=None,
    duration_filter=None,
    area_mode="include",
    time_mode="include",
    hours_mode="include",
    flights_mode="include",
):

    client = get_clickhouse_client()

    where, parameters = build_clickhouse_where(
        start_time,
        end_time,
        selected_classes,
        value_filters,
        selected_flights,
        area_polygons,
        selected_hours,
        duration_filter,
        area_mode,
        time_mode,
        hours_mode,
        flights_mode,
    )

    # Güvenlik açısından sadece tabloda gerçekten var olan kolonlar
    # kullanılır; count_filtered_rows ile birebir aynı projeksiyon
    # (bkz. _build_select_projection -- mantıksal->fiziksel kolon
    # eşlemesi de burada yapılır).
    col_expr = _build_select_projection(columns)

    query = f"""
    SELECT DISTINCT {col_expr}
    FROM {get_clickhouse_source()}
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
# POSTGRES (ASSET METADATA GEÇMİŞİ)
# ============================================================
#
# Güncel katalogdaki pipeline_asset_materializations her asset materialize
# olduğunda public.asset_metadata_history tablosuna bir trigger ile yansıtılır
# (bkz. docs/postgres_pipeline_catalog_schema.sql). Dashboard bu tabloyu
# sadece OKUR. Bu, yukarıdaki
# ASSET_CATALOG_QUERY'nin (Dagster GraphQL, sadece SON materialization)
# aksine, asset / uçuş / tarih bazlı filtrelenebilir bir GEÇMİŞ sağlar.

def get_postgres_conn_params() -> dict:
    return dict(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
        dbname=os.environ.get(
            "POSTGRES_DATABASE",
            os.environ.get("POSTGRES_DB", "postgres"),
        ),
    )


def get_postgres_conn():
    return psycopg2.connect(**get_postgres_conn_params())


@st.cache_data(ttl=30)
def get_metadata_history_filters() -> dict:
    """
    Filtre widget'ları (Asset / Uçuş multiselect) için mevcut değerleri
    döner. Tablo henüz yoksa (pipeline hiç çalışmadıysa ya da Postgres'e
    erişilemiyorsa) boş listeler döner; katalog sekmesi bu durumda
    "henüz veri yok" mesajı gösterir, hata fırlatmaz.
    """

    try:
        conn = get_postgres_conn()
    except Exception:
        return {"assets": [], "flights": []}

    try:
        with conn.cursor() as cur:

            cur.execute(
                "SELECT to_regclass('public.asset_metadata_history')"
            )

            if cur.fetchone()[0] is None:
                return {"assets": [], "flights": []}

            cur.execute(
                "SELECT DISTINCT asset_key FROM public.asset_metadata_history "
                "ORDER BY asset_key"
            )
            assets = [row[0] for row in cur.fetchall()]

            # Uçuş seçenekleri iki kaynaktan birleştirilir:
            #   1) tekil flight_id kolonu (eski, uçuş başına ayrı
            #      materialization üreten satırlar),
            #   2) metadata['flight_ids'] JSON dizisi (yeni birleşik
            #      AU-AIR pipeline'ında TEK materialization tüm uçuşları
            #      kapsar; sync trigger flight_id kolonuna yalnızca
            #      dizinin İLK elemanını yazar -- bkz.
            #      docs/postgres_pipeline_catalog_schema.sql
            #      sync_asset_metadata_history()). (2) olmadan "Uçuş"
            #      filtresi tek bir uçuş kimliğine sıkışıp kalıyordu.
            cur.execute(
                "SELECT DISTINCT fid FROM ("
                "  SELECT flight_id AS fid "
                "  FROM public.asset_metadata_history "
                "  WHERE flight_id IS NOT NULL AND flight_id <> '' "
                "  UNION "
                "  SELECT jsonb_array_elements_text(metadata -> 'flight_ids') "
                "  FROM public.asset_metadata_history "
                "  WHERE jsonb_typeof(metadata -> 'flight_ids') = 'array'"
                ") AS all_flights "
                "WHERE fid IS NOT NULL AND fid <> '' "
                "ORDER BY fid"
            )
            flights = [row[0] for row in cur.fetchall()]

        return {"assets": assets, "flights": flights}

    except Exception:
        return {"assets": [], "flights": []}

    finally:
        conn.close()


@st.cache_data(ttl=15)
def fetch_metadata_history(
    assets: tuple = (),
    flights: tuple = (),
    start_date=None,
    end_date=None,
    limit: int = 500,
) -> pd.DataFrame:
    """
    asset_metadata_history tablosundan, verilen filtrelere uyan
    kayıtları en yeni materialization'dan başlayarak döner (en fazla
    `limit` satır).
    """

    conn = get_postgres_conn()

    try:

        clauses = []
        params = []

        if assets:
            clauses.append("asset_key = ANY(%s)")
            params.append(list(assets))

        if flights:
            # Uçuş filtresi üç koşulu OR'lar:
            #   1) tekil flight_id kolonu (eski satırlar),
            #   2) metadata['flight_ids'] JSON dizisiyle kesişim -- yeni
            #      birleşik AU-AIR pipeline'ında tek materialization
            #      birden çok uçuşu kapsar ve flight_id kolonu diziye
            #      DEĞİL yalnızca ilk elemana eşittir; bu koşul olmadan
            #      birden çok uçuş seçilse bile sonuç tek uçuşa sıkışıyordu
            #      (bkz. get_metadata_history_filters ve schema'daki
            #      sync_asset_metadata_history()).
            #   3) flight_id IS NULL -- dvc_published_telemetry gibi belirli
            #      bir uçuşa değil tüm partition'a ait asset'ler flight_id=
            #      NULL ile kaydediliyor; ANY(...) ile filtrelenseydi bir
            #      uçuş seçildiğinde bunlar sonuçlardan tamamen kaybolurdu.
            # NOT: ?| operatörü yerine eşdeğer jsonb_exists_any(...) fonksiyonu
            # kullanılır -- '?' içeren operatörler bazı sürücü/paramstyle
            # kombinasyonlarında placeholder ile karışabiliyor.
            clauses.append(
                "("
                "flight_id = ANY(%s) "
                "OR (jsonb_typeof(metadata -> 'flight_ids') = 'array' "
                "AND jsonb_exists_any(metadata -> 'flight_ids', %s)) "
                "OR flight_id IS NULL"
                ")"
            )
            params.append(list(flights))
            params.append(list(flights))

        if start_date:
            clauses.append("partition_date >= %s")
            params.append(start_date)

        if end_date:
            clauses.append("partition_date <= %s")
            params.append(end_date)

        where_sql = (
            "WHERE " + " AND ".join(clauses)
            if clauses
            else ""
        )

        query = f"""
            SELECT
                asset_key, group_name, partition_date, flight_id,
                run_id, row_count, metadata, materialized_at
            FROM public.asset_metadata_history
            {where_sql}
            ORDER BY materialized_at DESC
            LIMIT %s
        """

        params.append(limit)

        return pd.read_sql(query, conn, params=params)

    finally:
        conn.close()


# ============================================================
# METADATA GEÇMİŞİ (RENDER)
# ============================================================
#
# NOT: Filtreler değiştirildiğinde eski sonucun ekranda kafa karıştırıcı
# şekilde kalmaya devam etmesini önlemek için, "Veri Gözat / Dışa Aktar"
# sekmesindeki (render_data_export) aynı desen kullanılıyor: sonuç ve o
# sonucu üreten filtre "imzası" session_state'te birlikte tutulur; imza
# güncel filtrelerle uyuşmuyorsa eski sonuç hemen temizlenir ve kullanıcı
# yeniden sorgulamaya yönlendirilir. Sorgu, her widget değişiminde değil,
# yalnızca "🔍 Sorgula" butonuna basıldığında (veya ilk açılışta,
# filtresiz varsayılan olarak) çalışır.

def _parse_metadata_date_range(date_range):

    start_date = None
    end_date = None

    if isinstance(date_range, (tuple, list)):
        if len(date_range) == 2:
            start_date, end_date = date_range
        elif len(date_range) == 1:
            start_date = date_range[0]
    elif date_range:
        start_date = date_range

    return start_date, end_date


def _row_flight_ids(row) -> list:
    """
    Bir metadata-geçmişi satırının temsil ettiği TÜM uçuş kimliklerini
    döner. Yeni birleşik AU-AIR pipeline'ında tek bir materialization
    birden çok uçuşu kapsar ve kimlikler metadata['flight_ids'] dizisinde
    tutulur; flight_id kolonu bu dizinin yalnızca ilk elemanıdır (bkz.
    schema'daki sync_asset_metadata_history()). Eski satırlarda ise
    yalnızca tekil flight_id doludur.
    """

    metadata = row.get("metadata") or {}
    ids = metadata.get("flight_ids")

    if isinstance(ids, list) and ids:
        return [str(x) for x in ids if str(x).strip()]

    if row.get("flight_id"):
        return [str(row["flight_id"])]

    return []


def render_metadata_history() -> None:

    st.subheader(
        "🗄️ Metadata Geçmişi"
    )

    st.caption(
        "Yukarıdaki tablo her asset'in yalnızca SON materialization'ını "
        "gösterir. Burada, her materialization'da Postgres'e kaydedilen "
        "geçmiş; asset / uçuş / tarih bazlı filtrelenebilir "
        "(public.asset_metadata_history, bkz. "
        "docs/postgres_pipeline_catalog_schema.sql)."
    )

    try:
        filters = get_metadata_history_filters()
    except Exception as exc:
        st.warning(
            f"Postgres'e bağlanılamadı: {exc}"
        )
        return

    if not filters["assets"]:
        st.info(
            "Henüz metadata geçmişi yok. İlk başarılı asset materialization'ı "
            "pipeline kataloğuna yazıldığında bu bölüm dolacaktır."
        )
        return

    # --------------------------------------------------------------
    # Filtreler
    # --------------------------------------------------------------
    #
    # st.form içindeki widget'lar, dışarıdaki gibi HER değişiklikte
    # (ya da otomatik yenileme sekmesindeki periyodik rerun'larda) anlık
    # olarak sorguyu tetiklemez -- yalnızca "Sorgula" ile submit edilince
    # okunur. Önceki tasarımda filtre widget'ları formun dışındaydı ve
    # her rerun'da (örn. otomatik yenileme, ya da listedeki asset/uçuş
    # seçenekleri arka planda değiştiğinde) o anki widget değerleri son
    # sorgulanan değerlerle karşılaştırılıyordu; bu karşılaştırma
    # otomatik yenilemeyle çakışıp "Filtreler değişti" mesajının
    # durduk yere görünüp kaybolmasına (flicker) yol açıyordu. Form,
    # bu karşılaştırmayı tamamen gereksiz kılar.

    with st.form("metadata_history_filter_form", border=True):

        filter_col1, filter_col2, filter_col3 = st.columns(
            [1, 1, 1.4]
        )

        with filter_col1:

            selected_assets = st.multiselect(
                "Asset",
                options=filters["assets"],
                key="metadata_history_assets",
            )

        with filter_col2:

            selected_flights = st.multiselect(
                "Uçuş",
                options=filters["flights"],
                key="metadata_history_flights",
            )

        with filter_col3:

            date_range = st.date_input(
                "Tarih aralığı (partition)",
                value=(),
                key="metadata_history_date_range",
                help=(
                    "Boş bırakılırsa tüm tarihler dahil edilir. Aralık "
                    "seçmek için takvimde iki tarihe art arda tıklayın."
                ),
            )

        submitted = st.form_submit_button(
            "🔍 Sorgula",
            type="primary",
        )

    if st.button(
        "🧹 Filtreleri temizle",
        key="metadata_history_clear_btn",
    ):

        for widget_key in (
            "metadata_history_assets",
            "metadata_history_flights",
            "metadata_history_date_range",
        ):
            st.session_state.pop(widget_key, None)

        st.session_state.pop("metadata_history_result", None)

        st.rerun()

    # --------------------------------------------------------------
    # Sorgu -- yalnızca "Sorgula" submit edildiğinde (ya da ilk
    # açılışta, filtresiz varsayılan olarak) Postgres'e gidilir.
    # --------------------------------------------------------------

    if submitted:

        start_date, end_date = _parse_metadata_date_range(
            date_range
        )

        try:

            # Cache'i temizle -- aksi halde aynı filtre kombinasyonuyla
            # tekrar "Sorgula"ya basıldığında (örn. yeni bir materialization
            # sonrası) ttl dolana kadar eski sonuç dönebilir.
            fetch_metadata_history.clear()

            with st.spinner("Sorgulanıyor..."):

                history_df = fetch_metadata_history(
                    assets=tuple(selected_assets),
                    flights=tuple(selected_flights),
                    start_date=start_date,
                    end_date=end_date,
                )

        except Exception as exc:

            st.error(
                f"Metadata geçmişi okunamadı: {exc}"
            )
            return

        st.session_state["metadata_history_result"] = history_df

    elif "metadata_history_result" not in st.session_state:

        # İlk açılış: henüz hiç sorgu yapılmadıysa filtresiz (tüm
        # kayıtlar, en yeni önce) varsayılan sonuç otomatik gösterilir.
        try:

            history_df = fetch_metadata_history()

        except Exception as exc:

            st.error(
                f"Metadata geçmişi okunamadı: {exc}"
            )
            return

        st.session_state["metadata_history_result"] = history_df

    history_df = st.session_state.get("metadata_history_result")

    if history_df is None or history_df.empty:
        st.info(
            "Filtrelere uyan kayıt bulunamadı."
        )
        return

    # --------------------------------------------------------------
    # Özet + sonuç tablosu
    # --------------------------------------------------------------

    # Her satırın kapsadığı uçuşlar tekil flight_id kolonundan DEĞİL,
    # metadata['flight_ids'] dizisinden çözülür (bkz. _row_flight_ids) --
    # birleşik AU-AIR materialization'ında flight_id kolonu dizinin
    # yalnızca ilk elemanıdır.
    history_df = history_df.copy()
    history_df["_flight_ids"] = history_df.apply(_row_flight_ids, axis=1)
    history_df["Uçuş(lar)"] = history_df["_flight_ids"].apply(
        lambda ids: ", ".join(ids) if ids else "-"
    )

    all_flight_ids = set()
    for ids in history_df["_flight_ids"]:
        all_flight_ids.update(ids)

    metric_col1, metric_col2, metric_col3 = st.columns(3)

    metric_col1.metric("Kayıt", len(history_df))
    metric_col2.metric("Asset", history_df["asset_key"].nunique())
    metric_col3.metric(
        "Uçuş",
        len(all_flight_ids),
    )

    st.caption(
        "En fazla 500 kayıt gösterilir, en yeni materialization önce."
    )

    display_df = history_df[
        [
            "asset_key",
            "group_name",
            "partition_date",
            "Uçuş(lar)",
            "row_count",
            "materialized_at",
            "run_id",
        ]
    ].rename(
        columns={
            "asset_key": "Asset",
            "group_name": "Grup",
            "partition_date": "Tarih",
            "row_count": "Satır Sayısı",
            "materialized_at": "Materialize Zamanı",
            "run_id": "Run ID",
        }
    )

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
    )

    st.caption(
        "Detay için bir satırı aşağıdan genişletin:"
    )

    for _, row in history_df.iterrows():

        # Uçuş bilgisi (ve diğer temel alanlar) her zaman görünür olsun
        # diye, ham JSON metadata'nın önüne eklenir -- eski kayıtlarda
        # JSON içine "flight_id" yazılmamış olsa bile burada gösterilir.
        flight_label = row["Uçuş(lar)"]

        display_metadata = {
            "flight_ids": flight_label,
            "asset_key": row["asset_key"],
            "partition_date": row["partition_date"],
            "materialized_at": row["materialized_at"],
        }

        for key, value in (row["metadata"] or {}).items():
            if key not in display_metadata:
                display_metadata[key] = value

        with st.expander(
            f"✈️ {flight_label} — {row['asset_key']} — "
            f"{row['partition_date']} — {row['materialized_at']}"
        ):

            metadata_rows = []

            for key, value in display_metadata.items():

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
# ALERT KAYITLARI (MinIO)
# ===========================================================================
#
# Alertler, Dagster'ın (dagster/alerting.py) MinIO'da tek bir JSON
# nesnesinde tuttuğu bir listedir; dashboard bu nesneyi salt-okunur
# çeker. Eski sürümde dagster ve dashboard bind-mount edilmiş bir dosyayı
# paylaşıyordu -- artık ortak dosya yok, kaynak MinIO.
#
# Bucket / nesne: MINIO_ALERTS_BUCKET / MINIO_ALERTS_OBJECT.
# Bağlantı: get_minio_client() (MINIO_ENDPOINT / MINIO_ACCESS_KEY / ...).

_ALERT_COLUMNS = [
    "timestamp",
    "job_name",
    "step_name",
    "error",
    "status",
]


def get_alerts_bucket() -> str:
    return os.environ.get("MINIO_ALERTS_BUCKET", "pipeline-alerts")


def get_alerts_object() -> str:
    return os.environ.get("MINIO_ALERTS_OBJECT", "alerts.json")


def get_alerts_uri() -> str:
    return f"s3://{get_alerts_bucket()}/{get_alerts_object()}"


@st.cache_data(ttl=5)
def load_alerts() -> pd.DataFrame:

    empty = pd.DataFrame(columns=_ALERT_COLUMNS)

    if Minio is None:
        return empty

    try:

        client = get_minio_client()
        bucket = get_alerts_bucket()

        if not client.bucket_exists(bucket):
            return empty

        response = client.get_object(bucket, get_alerts_object())

        try:
            raw = response.read()
        finally:
            response.close()
            response.release_conn()

        alerts = json.loads(raw)

    except Exception:
        # MinIO erişilemez / nesne yok / bozuk JSON -- alert bölümü
        # "kayıt yok" gösterir, dashboard çökmemeli.
        return empty

    if not isinstance(alerts, list) or not alerts:
        return empty

    df = pd.DataFrame(alerts)

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
# ALERT TOAST BİLDİRİMİ
# ===========================================================================

def notify_new_alerts(alerts_df: pd.DataFrame) -> None:
    """
    Yeni bir hata oluştuğunda (veya daha önce hatalı olan bir adım
    başarıyla çözüldüğünde) ekranın köşesinde kısa süreli, küçük bir
    bildirim (st.toast) gösterir.

    Hangi tab açık olursa olsun her rerun'da çalışır. İlk sayfa
    yüklemesinde geçmişte birikmiş tüm alertler için toast spam'i
    yapmamak amacıyla, yalnızca bu oturumda daha önce görülmemiş
    (session_state'te izi olmayan) durum değişiklikleri bildirilir.
    """

    if alerts_df.empty:
        return

    seen_status = st.session_state.setdefault(
        "_seen_alert_status", {}
    )

    is_first_load = "_alerts_initialized" not in st.session_state

    for _, row in alerts_df.iterrows():

        key = (
            f"{row.get('job_name')}|"
            f"{row.get('step_name')}|"
            f"{row.get('timestamp')}"
        )

        status = row.get("status", "")
        previous_status = seen_status.get(key)

        if is_first_load:

            seen_status[key] = status
            continue

        if previous_status is None and status == "FAILURE":

            st.toast(
                f"Yeni hata: {row.get('job_name')} → {row.get('step_name')}",
                icon="🚨",
            )

        elif previous_status == "FAILURE" and status == "RESOLVED":

            st.toast(
                f"Çözüldü: {row.get('job_name')} → {row.get('step_name')}",
                icon="✅",
            )

        seen_status[key] = status

    st.session_state["_alerts_initialized"] = True


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
    # Filtreler (aktif hata + zaman aralığı)
    # -----------------------------------------------------------------------
    #
    # Test/backfill sırasında biriken çok sayıda eski alert, yeni gelen
    # gerçek alertleri gözden kaçırmayı kolaylaştırıyor. Zaman filtresi hem
    # alert listesine hem de aşağıdaki "Başarısız Run'lar" tablosuna
    # uygulanır; KPI'lar da seçilen aralığa göre güncellenir.

    TIME_FILTER_OPTIONS = [
        "🕐 Son 1 saat",
        "📅 Bugün",
        "🗓️ Tümü",
    ]

    with st.container(border=True):

        filter_col1, filter_col2 = st.columns(
            [1, 2]
        )

        with filter_col1:

            show_only_active = st.toggle(
                "🔴 Sadece aktif hatalar",
                value=True,
                help=(
                    "Açıkken, geçmişte yaşanıp sonradan başarılı (RESOLVED) "
                    "olan adımlar listeden gizlenir. Kapatırsan çözülmüş "
                    "geçmiş alertler de görünür."
                ),
            )

        with filter_col2:

            time_filter = st.radio(
                "Zaman aralığı",
                options=TIME_FILTER_OPTIONS,
                index=2,
                horizontal=True,
                key="alert_time_filter",
            )

        if show_only_active and not alerts_df.empty and "status" in alerts_df.columns:
            alerts_df = alerts_df[alerts_df["status"] == "FAILURE"]

        now_utc = datetime.now(timezone.utc)

        def _filter_by_time(
            df: pd.DataFrame,
            time_column: str,
        ) -> pd.DataFrame:

            if df.empty or time_column not in df.columns:
                return df

            if time_filter == "🗓️ Tümü":
                return df

            series = pd.to_datetime(
                df[time_column],
                errors="coerce",
                utc=True,
            )

            if time_filter == "🕐 Son 1 saat":

                cutoff = now_utc - timedelta(hours=1)

                return df[series >= cutoff]

            if time_filter == "📅 Bugün":

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
            f"🔎 **{time_filter}**"
            + (" · sadece aktif hatalar" if show_only_active else "")
            + f" — **{len(alerts_df_filtered):,}** / {len(alerts_df):,} alert gösteriliyor."
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
    # Alert kaynağı bilgisi
    # -----------------------------------------------------------------------

    st.caption(
        f"Alert kaynağı: `{get_alerts_uri()}` (MinIO)"
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

            run_id = row.get(
                "run_id"
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

                # -------------------------------------------------------
                # Tekrar çalıştırma bağlantısı
                # -------------------------------------------------------
                #
                # Eski alertlerde run_id yok (bu alan sonradan eklendi),
                # bu yüzden yalnızca mevcutsa gösteriliyor. Dagster UI'daki
                # run sayfasından "Re-execute from failure" ile aynı adım
                # tekrar çalıştırılabilir.

                if run_id and not pd.isna(run_id):

                    run_url = f"{get_ui_url()}/runs/{run_id}"

                    alert_status = row.get(
                        "status",
                        "FAILURE",
                    )

                    button_col, status_col = st.columns(
                        [1, 2]
                    )

                    with button_col:

                        st.link_button(
                            "🔁 Dagster'da Aç ve Tekrar Çalıştır",
                            run_url,
                        )

                    # -----------------------------------------------
                    # Otomatik düzeltme durumu
                    # -----------------------------------------------
                    #
                    # Yeniden çalıştırma denemeleri artık burada
                    # (kullanıcı bir butona basınca) DEĞİL, hata
                    # oluştuğu anda Dagster'ın kendisinde --
                    # dagster/alerting.py::alert_on_failure hook'u
                    # içinde -- otomatik olarak yapılıyor (en fazla 3
                    # deneme, "re-execute from failure"). Burada
                    # sadece o otomatik sürecin MinIO'daki alert
                    # kaydına yazdığı sonuç okunup gösteriliyor;
                    # kullanıcının hiçbir işlem yapmasına gerek yok.

                    auto_fix_resolved_attempt = row.get(
                        "auto_fix_resolved_attempt"
                    )
                    auto_fix_exhausted = row.get(
                        "auto_fix_exhausted"
                    )

                    with status_col:

                        if alert_status == "RESOLVED":

                            resolved_at = row.get(
                                "resolved_at"
                            )

                            resolved_at_suffix = (
                                f" (`{resolved_at}`)"
                                if resolved_at and not pd.isna(resolved_at)
                                else ""
                            )

                            if (
                                auto_fix_resolved_attempt is not None
                                and not pd.isna(auto_fix_resolved_attempt)
                            ):

                                # Sistem, hatayı KENDİ otomatik
                                # denemeleriyle çözdü (bkz. dagster/
                                # alerting.py::_auto_fix_failure).

                                st.success(
                                    "✅ Çözüldü: **otomatik denemeler "
                                    "ile çözüldü** — sistem "
                                    f"{int(auto_fix_resolved_attempt)}. "
                                    "denemede sorunu kendisi giderdi, "
                                    "herhangi bir işlem yapmanıza "
                                    "gerek yok."
                                    + resolved_at_suffix
                                )

                            elif (
                                auto_fix_exhausted is not None
                                and not pd.isna(auto_fix_exhausted)
                                and bool(auto_fix_exhausted)
                            ):

                                # Sistemin 3 otomatik denemesi de
                                # başarısız olmuştu, ama alert artık
                                # RESOLVED -- yani sorun otomatik
                                # denemelerle DEĞİL, daha sonra başka
                                # bir şekilde (ör. Dagster'da elle
                                # "Re-execute from failure") çözülmüş.

                                st.success(
                                    "✅ Çözüldü: **elle (manuel) "
                                    "çözüldü** — sistemin 3 otomatik "
                                    "denemesi başarısız olmuştu, sorun "
                                    "daha sonra Dagster üzerinden elle "
                                    "yeniden çalıştırılarak giderildi."
                                    + resolved_at_suffix
                                )

                            else:

                                # Alert RESOLVED ama otomatik düzeltme
                                # kaydı (ne resolved_attempt ne
                                # exhausted) yok. Sebep: otomatik
                                # düzeltme kapalı/kaldırılmış, ya da
                                # devreye girmeden elle çözülmüş, ya da
                                # işaret alerts.json'a yazılırken
                                # yarış (race) nedeniyle kaybolmuş
                                # olabilir. Kesin "elle" demiyoruz.

                                st.success(
                                    "✅ Çözüldü — otomatik düzeltme "
                                    "kaydı yok (otomatik düzeltme kapalı "
                                    "olabilir ya da adım elle yeniden "
                                    "çalıştırılmış olabilir). Tekrar "
                                    "çalıştırmaya gerek yok."
                                    + resolved_at_suffix
                                )

                        elif (
                            auto_fix_exhausted is not None
                            and not pd.isna(auto_fix_exhausted)
                            and bool(auto_fix_exhausted)
                        ):

                            attempts = row.get(
                                "auto_fix_attempts",
                                3,
                            )

                            attempts = (
                                int(attempts)
                                if attempts and not pd.isna(attempts)
                                else 3
                            )

                            st.error(
                                f"❌ Sistem hatayı otomatik olarak "
                                f"{attempts} kere art arda yeniden "
                                "çalıştırmayı denedi, hepsi başarısız "
                                "oldu. Yukarıdaki linkten Dagster'ı "
                                "açıp elle inceleyebilirsiniz."
                            )

                        else:

                            current_attempt = row.get(
                                "auto_fix_current_attempt"
                            )

                            if (
                                current_attempt is not None
                                and not pd.isna(current_attempt)
                            ):

                                st.info(
                                    "🤖 Sistem bu hatayı otomatik "
                                    f"olarak düzeltmeye çalışıyor — "
                                    f"deneme {int(current_attempt)}/3. "
                                    "Sonuç burada görünecek."
                                )

                            else:

                                st.info(
                                    "🤖 Sistem bu hatayı otomatik "
                                    "olarak düzeltmeye çalışıyor (en "
                                    "fazla 3 deneme). Sonuç burada "
                                    "görünecek."
                                )

    elif not alerts_df.empty:

        st.info(
            f"Seçilen zaman aralığında (**{time_filter}**) alert yok. "
            f"Filtreye uyan toplam {len(alerts_df):,} kayıtlı alert var — "
            f"görmek için filtreyi **Tümü** olarak değiştirin."
        )

    else:

        st.success(
            "🎉 Henüz kayıtlı veya aktif bir pipeline alert'i yok."
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

    failures = runs_df_filtered[
        runs_df_filtered["status"]
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

DOWNLOAD_FORMAT_LABELS = {
    "all": "📄 Tüm Veri (Tek Dosya)",
    "zip": "📦 Uçuş Bazlı ZIP",
    "each": "✈️ Uçuşları Tek Tek İndir",
    "merge": "🧩 Seçili Uçuşları Birleştir",
}

# Dosya TİPİ (CSV / Parquet / TAB / MAT) -- yukarıdaki
# DOWNLOAD_FORMAT_LABELS'tan bağımsız bir eksen: format YAPIYI (tek
# dosya mı, uçuş başına ayrı dosya mı) belirlerken, bu seçim üretilen
# her dosyanın biçimini belirler. Parquet; sütun bazlı, sıkıştırılmış
# ve pandas/pyarrow ile CSV'den daha hızlı okunan bir format -- büyük
# veri setlerini başka bir analiz aracına (örn. pandas, Spark)
# aktarmak için tercih edilir. TAB, CSV ile aynı ama virgül yerine
# TAB karakteriyle ayrılmış düz metin (bazı analiz araçları/eski
# yazılımlar bunu bekler). MAT, scipy.io.savemat ile üretilen ve
# MATLAB/Octave'da doğrudan `load()` ile açılabilen ikili bir format
# (bkz. _dataframe_to_mat_bytes).
DOWNLOAD_FILE_TYPE_LABELS = {
    "csv": "📄 CSV",
    "parquet": "🗄️ Parquet (.parquet)",
    "tab": "📋 Tab Ayraçlı (.tab)",
    "mat": "🔬 MATLAB (.mat)",
}

DOWNLOAD_FILE_EXTENSIONS = {
    "parquet": "parquet",
    "tab": "tab",
    "mat": "mat",
}

DOWNLOAD_MIME_TYPES = {
    "parquet": "application/octet-stream",
    "tab": "text/tab-separated-values",
    "mat": "application/octet-stream",
}


def _download_file_extension(file_type: str) -> str:
    return DOWNLOAD_FILE_EXTENSIONS.get(file_type, "csv")


def _download_mime_type(file_type: str) -> str:
    return DOWNLOAD_MIME_TYPES.get(file_type, "text/csv")


def _sanitize_mat_field_name(name: str, taken: set) -> str:
    """
    MATLAB değişken/struct alan adları yalnızca harf, rakam ve alt
    tireden oluşabilir ve bir harfle başlamalıdır. Kolon adları
    (özellikle "Kolon Seçimi" ile serbestçe seçilebildiği için) bu
    kurala uymayabilir -- scipy.io.savemat bunu kendiliğinden
    doğrulamaz ve geçersiz bir anahtarla üretilen .mat dosyası
    MATLAB/Octave'da açılamayabilir. Bu fonksiyon adı güvenli hale
    getirir ve çakışmaları ("box_x!" ile "box_x?" gibi ikisi de
    "box_x"a sadeleşen adları) sırayla numaralandırarak (_2, _3, ...)
    çözer.
    """

    safe = "".join(
        ch if ch.isalnum() or ch == "_" else "_"
        for ch in str(name)
    )

    if not safe or not (safe[0].isalpha() or safe[0] == "_"):
        safe = f"col_{safe}"

    candidate = safe
    suffix = 2

    while candidate in taken:
        candidate = f"{safe}_{suffix}"
        suffix += 1

    taken.add(candidate)

    return candidate


def _dataframe_to_mat_bytes(dataframe: pd.DataFrame) -> bytes:
    """
    dataframe'i MATLAB/Octave'un scipy.io.loadmat ile okuyabileceği bir
    .mat dosyasına çevirir. Her kolon, .mat içinde aynı isimde ayrı bir
    değişken/struct alanı olur. MATLAB tarih/saat ya da pandas'a özgü
    tipleri tanımadığı için datetime kolonları ISO 8601 metne, diğer
    metin/object kolonları da düz string dizisine çevrilir; sayısal
    kolonlar olduğu gibi (numpy array) aktarılır.
    """

    mat_dict = {}
    taken_names = set()

    for column in dataframe.columns:

        series = dataframe[column]
        field_name = _sanitize_mat_field_name(column, taken_names)

        if pd.api.types.is_datetime64_any_dtype(series):
            values = series.astype(str).to_numpy()
        elif pd.api.types.is_object_dtype(series) or isinstance(
            series.dtype, pd.StringDtype
        ):
            values = series.astype(str).to_numpy()
        else:
            values = series.to_numpy()

        mat_dict[field_name] = values

    buffer = io.BytesIO()

    scipy.io.savemat(
        buffer,
        mat_dict,
        do_compression=True,
    )

    return buffer.getvalue()


def _dataframe_to_download_bytes(
    dataframe: pd.DataFrame,
    file_type: str,
) -> bytes:
    """
    dataframe'i seçilen dosya tipine göre byte'a çevirir. CSV/TAB için
    Excel'in Türkçe karakterleri doğru okuyabilmesi adına "utf-8-sig"
    (BOM'lu UTF-8) kullanılır; parquet ve mat zaten ikili/kendi
    kendini tanımlayan formatlar olduğu için kodlama sorunu yaşanmaz.
    """

    if file_type == "parquet":
        return dataframe.to_parquet(index=False)

    if file_type == "mat":
        return _dataframe_to_mat_bytes(dataframe)

    if file_type == "tab":
        return dataframe.to_csv(
            index=False,
            sep="\t",
        ).encode("utf-8-sig")

    return dataframe.to_csv(index=False).encode("utf-8-sig")


def render_download_section(
    dataframe: pd.DataFrame,
    start_time: datetime,
    end_time: datetime,
    share_params: dict,
) -> None:
    """
    "3️⃣ İndir" adımının içeriği.

    Kullanıcı önce bir indirme formatı seçer; CSV/ZIP dönüşümü SADECE
    seçilen format için yapılır. Önceden üç format da (tek CSV, uçuş
    bazlı ZIP, uçuş bazlı tek tek CSV) her render'da aynı anda
    hesaplanıyordu — büyük veri setlerinde bu gereksiz yere uzun
    sürüyordu.

    ÖNEMLİ: Format seçildiğinde (hatta "Tüm Veri" varsayılan seçili
    geldiğinde, kullanıcı hiçbir şeye tıklamadan) dönüşümün OTOMATİK
    başlamaması için, dönüşüm doğrudan indirme butonuna gömülüdür:
    st.download_button'a `data` olarak argümansız bir callable verilir
    (Streamlit >= 1.61) ve bu callable YALNIZCA kullanıcı butona
    bastığında, ayrı bir thread'de çalışır (bkz.
    _render_all_data_csv_download vb.). Böylece ayrı bir "Oluştur"
    butonuna gerek kalmaz; tek tıkla veri hazırlanıp indirilir.

    share_params: mevcut filtre durumunun URL query parametresi olarak
    kodlanmış hâli (bkz. _encode_export_state_to_query_params). Burada,
    format seçiminin hemen yanında "bu filtreleri bağlantı olarak
    paylaş" seçeneği için kullanılır.
    """

    if start_time is None and end_time is None:
        time_suffix = "tum_zaman"
    else:
        time_suffix = (
            f"{start_time.strftime('%Y%m%d_%H%M%S') if start_time else 'basi'}_"
            f"{end_time.strftime('%Y%m%d_%H%M%S') if end_time else 'sonu'}"
        )

    has_flight_id = "flight_id" in dataframe.columns

    format_keys = ["all"]

    if has_flight_id:
        format_keys += ["zip", "each", "merge"]

    if "download_format_choice" not in st.session_state:

        pending_format = _decode_export_state_from_query_params(
            st.query_params.to_dict()
        ).get("download_format")

        if pending_format in format_keys:
            st.session_state["download_format_choice"] = pending_format

    selected_format = st.radio(
        "İndirme formatı seçin",
        options=format_keys,
        format_func=lambda key: DOWNLOAD_FORMAT_LABELS[key],
        index=0,
        horizontal=True,
        key="download_format_choice",
    )

    if "download_file_type_choice" not in st.session_state:

        pending_file_type = _decode_export_state_from_query_params(
            st.query_params.to_dict()
        ).get("download_file_type")

        if pending_file_type in DOWNLOAD_FILE_TYPE_LABELS:
            st.session_state["download_file_type_choice"] = pending_file_type

    selected_file_type = st.radio(
        "Dosya tipi",
        options=list(DOWNLOAD_FILE_TYPE_LABELS.keys()),
        format_func=lambda key: DOWNLOAD_FILE_TYPE_LABELS[key],
        index=0,
        horizontal=True,
        key="download_file_type_choice",
    )

    # --------------------------------------------------------
    # URL İLE PAYLAŞMA
    # --------------------------------------------------------
    #
    # Butona basıldığında hem tarayıcı adres çubuğundaki URL
    # (st.query_params ile) hem de tam, doğrudan kopyalanabilir bir
    # bağlantı (aşağıdaki metin kutusu) güncellenir. Parametrelere
    # "tab=export" eklenir; bu sayede bağlantı açıldığında uygulama
    # otomatik olarak "Veri Gözat / Dışa Aktar" sekmesinde açılır
    # (bkz. main() içindeki sekme seçim mantığı) ve filtreler
    # render_data_export'taki "PAYLAŞILAN BAĞLANTIDAN GELEN FİLTRE
    # DURUMU" bölümü sayesinde otomatik doldurulur -- alıcı hiçbir
    # sekmeye tıklamak zorunda kalmadan filtrelenmiş veriyi görür.

    with st.expander(
        "🔗 Bu Filtreleri Bağlantı Olarak Paylaş",
        expanded=False,
    ):

        st.caption(
            "Aşağıdaki butona basınca, şu an seçili zaman aralığı, "
            "uçuş/class/değer filtreleri, kolon seçimi ve indirme "
            "formatını içeren tam bir bağlantı oluşturulur. Bu "
            "bağlantıyı kopyalayıp paylaştığınızda, açan kişi doğrudan "
            "\"Veri Gözat / Dışa Aktar\" sekmesinde, aynı filtrelerle "
            "karşılaşır."
        )

        if st.button(
            "🔗 Paylaşılabilir Bağlantı Oluştur",
            key="build_export_share_link_btn",
        ):

            full_share_params = dict(share_params)
            full_share_params["export_fmt"] = selected_format
            full_share_params["export_ft"] = selected_file_type
            full_share_params["tab"] = "export"

            for existing_key in list(st.query_params.keys()):
                if existing_key.startswith("export_") or existing_key == "tab":
                    del st.query_params[existing_key]

            st.query_params.update(full_share_params)

            # Kopyalanabilir bağlantı alıcıyı DAİMA Kullanıcı panelindeki
            # "Veri Gözat / Dışa Aktar" sekmesine götürür -- "panel=user"
            # yalnızca paylaşılan URL metnine eklenir; paylaşan kişinin
            # adres çubuğundaki kendi "panel" seçimi (Admin olabilir)
            # değiştirilmez.
            shareable_params = dict(full_share_params)
            shareable_params["panel"] = "user"

            st.session_state["export_share_query_string"] = urlencode(
                shareable_params
            )

        query_string = st.session_state.get("export_share_query_string")

        if query_string:

            st.success(
                "Bağlantı hazır — aşağıdaki kutudaki tam URL'yi "
                "kopyalayıp paylaşabilirsiniz."
            )

            # window.parent kullanılır çünkü components.html içeriği
            # kendi (srcdoc) iframe'inde çalışır; window.location orada
            # "about:srcdoc" döner, asıl sayfanın URL'i window.parent
            # üzerinden okunur. Kopyalama, HTTPS olmayan ortamlarda
            # (navigator.clipboard güvenli bağlam ister) çalışmayabilir
            # diye document.execCommand("copy") ile yedeklenir.
            components.html(
                f"""
                <div style="display:flex; gap:6px; font-family: inherit;">
                  <input id="export_share_link_input" type="text" readonly
                         style="flex:1; padding:8px; font-size:14px;
                                border:1px solid #999; border-radius:4px;" />
                  <button id="export_share_copy_btn"
                          style="padding:8px 14px; font-size:14px;
                                 border-radius:4px; border:1px solid #999;
                                 cursor:pointer; white-space:nowrap;">
                    📋 Kopyala
                  </button>
                </div>
                <script>
                  const input = document.getElementById(
                    "export_share_link_input"
                  );
                  const btn = document.getElementById(
                    "export_share_copy_btn"
                  );
                  const fullUrl = window.parent.location.origin
                    + window.parent.location.pathname
                    + "?{query_string}";
                  input.value = fullUrl;

                  btn.addEventListener("click", function () {{
                    input.select();
                    input.setSelectionRange(0, 999999);

                    function fallbackCopy() {{
                      try {{ document.execCommand("copy"); }} catch (e) {{}}
                    }}

                    if (navigator.clipboard && window.isSecureContext) {{
                      navigator.clipboard.writeText(input.value)
                        .catch(fallbackCopy);
                    }} else {{
                      fallbackCopy();
                    }}

                    const original = btn.innerText;
                    btn.innerText = "✅ Kopyalandı";
                    setTimeout(function () {{
                      btn.innerText = original;
                    }}, 1500);
                  }});
                </script>
                """,
                height=55,
            )

    st.divider()

    if selected_format == "all":

        _render_all_data_csv_download(
            dataframe,
            time_suffix,
            selected_file_type,
        )

        return

    # "zip" ve "each" — ikisi de flight_id bazlı gruplamaya ihtiyaç duyar.
    # pandas.groupby varsayılan olarak NaN/None grup anahtarlarını zaten
    # otomatik eler; burada ek olarak sadece boş string ("") ve gerçekten
    # boş/whitespace-only değerler dışlanır (eski, flight_id kolonu
    # eklenmeden önce yazılmış satırlar boş string içerebilir).

    flight_groups = {
        str(flight): group_df
        for flight, group_df in dataframe.groupby("flight_id")
        if str(flight).strip()
    }

    if not flight_groups:

        st.info(
            "Seçilen veride uçuş bazlı gruplama için kullanılabilir "
            "flight_id bulunamadı."
        )

        return

    st.markdown(
        "**✈️ Uçuş Bazlı İndirme**"
    )

    summary_rows = [
        {
            "uçuş": flight,
            "satır_sayısı": len(group_df),
        }
        for flight, group_df in sorted(flight_groups.items())
    ]

    st.dataframe(
        pd.DataFrame(summary_rows),
        use_container_width=True,
        hide_index=True,
    )

    if selected_format == "zip":

        _render_flight_zip_download(
            flight_groups,
            time_suffix,
            selected_file_type,
        )

    elif selected_format == "each":

        _render_flight_individual_downloads(
            flight_groups,
            time_suffix,
            selected_file_type,
        )

    else:

        _render_flight_merge_download(
            flight_groups,
            time_suffix,
            selected_file_type,
        )


def _render_all_data_csv_download(
    dataframe: pd.DataFrame,
    time_suffix: str,
    file_type: str,
) -> None:

    extension = _download_file_extension(file_type)

    st.caption(
        f"{DOWNLOAD_FILE_TYPE_LABELS[file_type]} dosyası: "
        f"{len(dataframe):,} satır, {len(dataframe.columns)} kolon"
    )

    # Dönüşüm, download_button'ın `data` callable'ı içinde yapılır --
    # yani YALNIZCA kullanıcı indir butonuna basınca (ayrı bir thread'de)
    # çalışır. Böylece "Tüm Veri" formatı varsayılan seçili geldiği için
    # sekme açılır açılmaz büyük veri setinde saniyeler süren bir dönüşüm
    # otomatik başlamaz.

    st.download_button(
        label=f"⬇️ Tüm Veriyi {extension.upper()} Olarak İndir",
        data=lambda: _dataframe_to_download_bytes(dataframe, file_type),
        file_name=f"au_air_telemetry_{time_suffix}.{extension}",
        mime=_download_mime_type(file_type),
        type="primary",
        key="download_all_data_csv",
    )

    st.caption(
        "Butona bastığınızda dosya hazırlanıp indirilir; veri miktarına "
        "bağlı olarak birkaç saniye sürebilir."
    )


def _render_flight_zip_download(
    flight_groups: dict,
    time_suffix: str,
    file_type: str,
) -> None:

    extension = _download_file_extension(file_type)

    flight_options = sorted(flight_groups.keys())

    selected_flights_to_zip = st.multiselect(
        "ZIP'e dahil edilecek uçuşlar",
        options=flight_options,
        help=(
            "ZIP'e dahil etmek istediğiniz uçuşları seçin (hepsini "
            "istiyorsanız listeden tümünü seçin)."
        ),
        key="download_zip_selected_flights",
    )

    if not selected_flights_to_zip:

        st.caption(
            "ZIP indirmek için yukarıdan bir ya da daha fazla uçuş seçin."
        )

        return

    st.caption(
        f"{len(selected_flights_to_zip)} uçuş, toplam "
        f"{sum(len(flight_groups[f]) for f in selected_flights_to_zip):,} "
        "satır."
    )

    def _build_flight_zip() -> bytes:

        # download_button'ın `data` callable'ı -- yalnızca kullanıcı
        # butona basınca çalışır.

        zip_buffer = io.BytesIO()

        with zipfile.ZipFile(
            zip_buffer,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
        ) as zip_file:

            for flight in selected_flights_to_zip:

                file_bytes = _dataframe_to_download_bytes(
                    flight_groups[flight],
                    file_type,
                )

                zip_file.writestr(
                    f"ucus_{flight}_{time_suffix}.{extension}",
                    file_bytes,
                )

        return zip_buffer.getvalue()

    st.download_button(
        label=(
            f"⬇️ Seçilen Uçuşları ZIP Olarak İndir "
            f"({len(selected_flights_to_zip)} dosya)"
        ),
        data=_build_flight_zip,
        file_name=f"ucuslar_{time_suffix}.zip",
        mime="application/zip",
        type="primary",
        key="download_all_flights_zip",
    )

    st.caption(
        f"Her uçuş için ayrı bir {extension.upper()} dosyası içerir."
    )


def _render_flight_individual_downloads(
    flight_groups: dict,
    time_suffix: str,
    file_type: str,
) -> None:

    extension = _download_file_extension(file_type)

    flight_options = sorted(flight_groups.keys())

    selected_flights_to_prepare = st.multiselect(
        "İndirilecek uçuşlar",
        options=flight_options,
        help=(
            "Yalnızca burada seçtiğiniz uçuşlar için indirme butonu "
            "gösterilir; dosya, ilgili butona basıldığında hazırlanır."
        ),
        key="download_each_selected_flights",
    )

    if not selected_flights_to_prepare:

        st.caption(
            "İndirmek için yukarıdan bir ya da daha fazla uçuş seçin."
        )

        return

    for flight in sorted(selected_flights_to_prepare):

        group_df = flight_groups[flight]

        col_a, col_b = st.columns(
            [3, 1]
        )

        with col_a:

            st.write(
                f"**{flight}** — {len(group_df):,} satır"
            )

        with col_b:

            # data callable'ı her uçuş için ayrı ayrı bağlanır (df=group_df)
            # ve yalnızca o uçuşun butonuna basılınca çalışır.
            st.download_button(
                label=f"{extension.upper()} indir",
                data=lambda df=group_df: _dataframe_to_download_bytes(
                    df,
                    file_type,
                ),
                file_name=f"ucus_{flight}_{time_suffix}.{extension}",
                mime=_download_mime_type(file_type),
                key=f"download_flight_{flight}",
            )


def _render_flight_merge_download(
    flight_groups: dict,
    time_suffix: str,
    file_type: str,
) -> None:
    """
    Filtreye uyan uçuşlar arasından kullanıcının seçtiği BİRKAÇININ
    (hepsinin değil) satırlarını tek bir dosyada birleştirir -- örn.
    filtreye uyan 5 uçuş varken yalnızca uçuş1 ve uçuş2'yi birleştirip
    tek dosya olarak indirmek için. "Tüm Veri" formatından farkı budur:
    o format filtreye uyan TÜM uçuşları birleştirir, burası ise
    kullanıcının seçtiği bir alt kümeyi.
    """

    extension = _download_file_extension(file_type)

    flight_options = sorted(flight_groups.keys())

    selected_flights_to_merge = st.multiselect(
        "Birleştirilecek uçuşlar",
        options=flight_options,
        help=(
            "Filtreye uyan uçuşlar arasından, tek bir dosyada "
            "birleştirmek istediklerinizi seçin (örn. 5 uçuş içinden "
            "yalnızca 2'sini birleştirip indirebilirsiniz)."
        ),
        key="download_merge_selected_flights",
    )

    if not selected_flights_to_merge:

        st.caption(
            "Birleştirmek için yukarıdan bir ya da daha fazla uçuş seçin."
        )

        return

    merge_row_count = sum(
        len(flight_groups[flight])
        for flight in selected_flights_to_merge
    )

    st.caption(
        f"{len(selected_flights_to_merge)} uçuş, toplam "
        f"{merge_row_count:,} satır birleştirilecek."
    )

    # Dosya adına en fazla 3 uçuşun adı eklenir; daha fazlasında
    # dosya adı okunaksız uzayacağı için sadece sayı belirtilir.
    if len(selected_flights_to_merge) <= 3:
        flights_label = "_".join(sorted(selected_flights_to_merge))
    else:
        flights_label = f"{len(selected_flights_to_merge)}_ucus"

    def _build_merged_file() -> bytes:

        # download_button'ın `data` callable'ı -- yalnızca kullanıcı
        # butona basınca çalışır.

        merged_df = pd.concat(
            [
                flight_groups[flight]
                for flight in selected_flights_to_merge
            ],
            ignore_index=True,
        )

        return _dataframe_to_download_bytes(
            merged_df,
            file_type,
        )

    st.download_button(
        label=(
            f"⬇️ Birleştirilmiş {extension.upper()}'yi İndir "
            f"({len(selected_flights_to_merge)} uçuş)"
        ),
        data=_build_merged_file,
        file_name=(
            f"ucuslar_birlesik_{flights_label}_{time_suffix}."
            f"{extension}"
        ),
        mime=_download_mime_type(file_type),
        type="primary",
        key="download_flight_merge_csv",
    )


# ============================================================
# VERİ GÖZAT / DIŞA AKTAR — URL İLE PAYLAŞMA
# ============================================================
#
# "Veri Gözat / Dışa Aktar" sekmesindeki filtre durumu (zaman aralığı,
# uçuş/class/değer filtreleri, kolon seçimi) URL query parametrelerine
# yazılıp geri okunabilir. Kullanıcı "🔗 Paylaşılabilir Bağlantı
# Oluştur"a bastığında bu parametreler tarayıcı adres çubuğundaki
# URL'ye yazılır (st.query_params ile); o URL kopyalanıp paylaşıldığında
# ve açıldığında aynı filtreler otomatik olarak uygulanır.
#
# Operatörler URL'de "<", ">=" gibi özel karakterler yerine kısa kod
# adlarıyla (lt, gte, ...) tutulur -- sorgu string'inde bu karakterler
# çalışsa da (tarayıcı/streamlit yüzde-kodlaması yapar) okunabilirlik
# ve olası ayrıştırma sorunlarını önlemek için kod adları tercih edildi.

EXPORT_VALUE_FILTER_OP_CODES = {
    "<": "lt",
    "<=": "lte",
    ">": "gt",
    ">=": "gte",
    "=": "eq",
    "!=": "neq",
    RANGE_FILTER_OPERATOR: "bt",
}

EXPORT_VALUE_FILTER_OP_CODES_REVERSE = {
    code: op
    for op, code in EXPORT_VALUE_FILTER_OP_CODES.items()
}


def _encode_export_state_to_query_params(
    start_time,
    end_time,
    selected_flights,
    selected_classes,
    selected_columns,
    value_filters,
    area_polygons=None,
    selected_hours=None,
    duration_filter=None,
    area_mode="include",
    time_mode="include",
    hours_mode="include",
    flights_mode="include",
    columns_mode="include",
) -> dict:

    params = {}
    if start_time is not None:
        params["export_st"] = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    if end_time is not None:
        params["export_et"] = end_time.strftime("%Y-%m-%dT%H:%M:%S")

    if time_mode == "exclude":
        params["export_tm"] = "exclude"

    if selected_flights:
        params["export_fl"] = ",".join(
            str(flight) for flight in selected_flights
        )

        if flights_mode == "exclude":
            params["export_flm"] = "exclude"

    if selected_hours:
        params["export_hr"] = ",".join(
            str(int(hour)) for hour in selected_hours
        )

        if hours_mode == "exclude":
            params["export_hrm"] = "exclude"

    if selected_classes:
        params["export_cl"] = ",".join(
            str(cls) for cls in selected_classes
        )

    if selected_columns:
        params["export_co"] = ",".join(
            str(col) for col in selected_columns
        )

        if columns_mode == "exclude":
            params["export_com"] = "exclude"

    if value_filters:

        def _encode_value_filter(vf: dict) -> str:

            op_code = EXPORT_VALUE_FILTER_OP_CODES.get(
                vf["operator"], "lt"
            )

            # "x" öneki, bu filtrenin "hariç tut" (NOT) modunda olduğunu
            # işaretler (bkz. value_filter["exclude"] / build_clickhouse_
            # where) -- örn. "xbt" = "aralıkta DEĞİL".
            if vf.get("exclude"):
                op_code = f"x{op_code}"

            chunk = (
                f"{vf['column']}:"
                f"{op_code}:"
                f"{vf['value']}"
            )

            if vf["operator"] == RANGE_FILTER_OPERATOR:
                chunk += f":{vf.get('value2', 0.0)}"

            return chunk

        params["export_vf"] = "|".join(
            _encode_value_filter(vf) for vf in value_filters
        )

    if duration_filter and duration_filter.get("hours") is not None:

        op_code = EXPORT_VALUE_FILTER_OP_CODES.get(
            duration_filter.get("operator"), "lt"
        )

        chunk = f"{op_code}:{duration_filter['hours']}"

        if duration_filter.get("operator") == RANGE_FILTER_OPERATOR:
            chunk += f":{duration_filter.get('hours2', 0.0)}"

        params["export_dur"] = chunk

    if area_polygons:

        # Poligonlar "|" ile, her poligonun köşeleri ";" ile, her
        # köşenin boylam/enlemi "," ile ayrılır -- 6 ondalık basamak
        # (~11 cm hassasiyet) URL'yi gereksiz uzatmadan yeterli.
        params["export_ap"] = "|".join(
            ";".join(
                f"{lon:.6f},{lat:.6f}"
                for lon, lat in polygon
            )
            for polygon in area_polygons
        )

        if area_mode == "exclude":
            params["export_am"] = "exclude"

    return params


def _decode_export_state_from_query_params(query_params: dict) -> dict:

    state = {}

    if "export_st" in query_params:

        try:
            state["start_time"] = datetime.strptime(
                query_params["export_st"],
                "%Y-%m-%dT%H:%M:%S",
            )
        except ValueError:
            pass

    if "export_et" in query_params:

        try:
            state["end_time"] = datetime.strptime(
                query_params["export_et"],
                "%Y-%m-%dT%H:%M:%S",
            )
        except ValueError:
            pass

    if "export_tm" in query_params:
        state["time_mode"] = (
            "exclude"
            if query_params["export_tm"] == "exclude"
            else "include"
        )

    if "export_fl" in query_params:
        state["selected_flights"] = [
            flight
            for flight in query_params["export_fl"].split(",")
            if flight
        ]

        if "export_flm" in query_params:
            state["flights_mode"] = (
                "exclude"
                if query_params["export_flm"] == "exclude"
                else "include"
            )

    if "export_cl" in query_params:
        state["selected_classes"] = [
            cls
            for cls in query_params["export_cl"].split(",")
            if cls
        ]

    if "export_hr" in query_params:
        state["selected_hours"] = [
            int(hour)
            for hour in query_params["export_hr"].split(",")
            if hour.strip().isdigit()
            and 0 <= int(hour) <= 23
        ]

        if "export_hrm" in query_params:
            state["hours_mode"] = (
                "exclude"
                if query_params["export_hrm"] == "exclude"
                else "include"
            )

    if "export_co" in query_params:
        state["selected_columns"] = [
            col
            for col in query_params["export_co"].split(",")
            if col
        ]

        if "export_com" in query_params:
            state["columns_mode"] = (
                "exclude"
                if query_params["export_com"] == "exclude"
                else "include"
            )

    if "export_fmt" in query_params:
        state["download_format"] = query_params["export_fmt"]

    if "export_ft" in query_params:
        state["download_file_type"] = query_params["export_ft"]

    if "export_vf" in query_params:

        value_filters = []

        for chunk in query_params["export_vf"].split("|"):

            if not chunk:
                continue

            parts = chunk.split(":")

            if len(parts) == 3:
                column, op_code, raw_value = parts
                raw_value2 = None
            elif len(parts) == 4:
                column, op_code, raw_value, raw_value2 = parts
            else:
                continue

            # "x" öneki bu filtrenin "hariç tut" modunda kaydedildiğini
            # işaretler (bkz. _encode_value_filter).
            exclude = op_code.startswith("x")

            operator = EXPORT_VALUE_FILTER_OP_CODES_REVERSE.get(
                op_code[1:] if exclude else op_code
            )

            if operator is None:
                continue

            if operator == RANGE_FILTER_OPERATOR and raw_value2 is None:
                continue

            try:
                value = float(raw_value)
                value2 = (
                    float(raw_value2)
                    if raw_value2 is not None
                    else None
                )
            except ValueError:
                continue

            decoded_filter = {
                "column": column,
                "operator": operator,
                "value": value,
                "exclude": exclude,
            }

            if value2 is not None:
                decoded_filter["value2"] = value2

            value_filters.append(decoded_filter)

        state["value_filters"] = value_filters

    if "export_dur" in query_params:

        parts = query_params["export_dur"].split(":")

        if len(parts) in (2, 3):

            op_code = parts[0]
            operator = EXPORT_VALUE_FILTER_OP_CODES_REVERSE.get(op_code)

            if operator is not None and not (
                operator == RANGE_FILTER_OPERATOR and len(parts) != 3
            ):

                try:
                    hours = float(parts[1])
                    hours2 = float(parts[2]) if len(parts) == 3 else None
                except ValueError:
                    hours = None
                    hours2 = None

                if hours is not None:

                    decoded_duration_filter = {
                        "operator": operator,
                        "hours": hours,
                    }

                    if hours2 is not None:
                        decoded_duration_filter["hours2"] = hours2

                    state["duration_filter"] = decoded_duration_filter

    if "export_ap" in query_params:

        area_polygons = []

        for polygon_chunk in query_params["export_ap"].split("|"):

            if not polygon_chunk:
                continue

            polygon = []

            for point_chunk in polygon_chunk.split(";"):

                point_parts = point_chunk.split(",")

                if len(point_parts) != 2:
                    continue

                try:
                    lon = float(point_parts[0])
                    lat = float(point_parts[1])
                except ValueError:
                    continue

                polygon.append((lon, lat))

            if len(polygon) >= 3:
                area_polygons.append(polygon)

        if area_polygons:
            state["area_polygons"] = area_polygons

    if "export_am" in query_params:
        state["area_mode"] = (
            "exclude"
            if query_params["export_am"] == "exclude"
            else "include"
        )

    return state


def render_data_export():

    st.subheader(
        "Geniş Şemalı AU-AIR Telemetri Verisi"
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
            f"Şema okunamadı: {exc}"
        )

        return

    if schema.empty:

        st.warning(
            f"`{get_clickhouse_database()}.{get_clickhouse_table()}` "
            "tablosu henüz oluşturulmamış. Dagster pipeline'ı en az bir "
            "kez çalışıp `clickhouse_auair_batch` asset'ini materialize "
            "ettikten sonra burada veri görünecektir."
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
            "ClickHouse AU-AIR tablosunda henüz veri bulunamadı."
        )

        return

    st.write(
        f"**Veri aralığı:** "
        f"{min_time} → {max_time}"
    )

    st.divider()

    # ==========================================================
    # PAYLAŞILAN BAĞLANTIDAN GELEN FİLTRE DURUMU
    # ==========================================================
    #
    # URL'de export_* query parametreleri varsa (bkz.
    # _encode_export_state_to_query_params / "🔗 Bu Filtreleri Bağlantı
    # Olarak Paylaş" bölümü) burada çözülür. Her widget kendi
    # session_state key'ini oluşturmadan ÖNCE bu değerleri okuyup
    # (ve mevcut seçeneklere göre süzüp) session_state'e yazar --
    # aksi halde Streamlit widget'ları kendi varsayılanını kullanır.
    #
    # Bir widget key'i session_state'te zaten varsa (kullanıcı elle
    # değiştirmiş ya da bu sekme bu oturumda daha önce render
    # edilmişse) buradaki değerler UYGULANMAZ -- yoksa kullanıcı
    # filtreyi değiştirdikten sonra her rerun'da URL'deki eski
    # değere geri dönerdi.

    pending_url_state = _decode_export_state_from_query_params(
        st.query_params.to_dict()
    )

    # ==========================================================
    # ADIM 1 — FİLTRELE
    # ==========================================================
    #
    # Tüm filtreler burada, kapanabilir bölümler (expander) hâlinde
    # gruplanır. En sık kullanılan "Zaman ve Uçuş" varsayılan olarak
    # açık gelir; daha az kullanılan filtreler kapalı başlar ki sayfa
    # ilk bakışta sade görünsün.

    st.subheader(
        "1️⃣ Filtrele"
    )

    with st.expander(
        "🤖 Doğal Dil ile Filtrele (Qwen3)",
        expanded=False,
    ):

        st.caption(
            "Filtreleri elle kurmak yerine aradığınızı Türkçe yazın; "
            "yerelde çalışan Qwen3.5:4B modeli sorguyu ayrıştırıp "
            "aşağıdaki bölümleri sizin yerinize doldurur: zaman "
            "aralığı, günün saati, uçuş süresi, class ve uçuş seçimi, "
            "değer bazlı filtreler. Uygulandığında, önceki değer "
            "bazlı filtrelerin YERİNE geçer, diğerlerini günceller. "
            "Örn: *\"Saat 7-9 arasında, irtifası 300 metrenin altına "
            "düşen İnsan class'lı kayıtlar\"*. Harita alanı (poligon) "
            "filtresi buradan desteklenmiyor -- o hâlâ elle çizilir."
        )

        if _llm_sorgula is None:

            st.warning(
                "LLM entegrasyonu kullanılamıyor "
                f"(`{_LLM_IMPORT_HATASI}`). `qwen_benchmark.py` "
                "dosyasının `app.py` ile aynı klasörde olduğundan ve "
                "`ollama` paketinin kurulu olduğundan emin olun "
                "(`pip install ollama`). Ayrıca `OLLAMA_HOST` ile "
                "erişilen Ollama servisinin ve `OLLAMA_MODEL` ile "
                "seçilen modelin çalıştığını kontrol edin."
            )

        else:

            llm_query_text = st.text_area(
                "Sorgunuz",
                key="llm_query_input",
                placeholder=(
                    "Örn: Saat 7-9 arasında irtifası 300 metrenin "
                    "altında olan person class'lı kayıtlar"
                ),
                height=80,
            )

            llm_submit = st.button(
                "🔍 Sorguyu Yorumla ve Uygula",
                key="llm_submit_btn",
            )

            if llm_submit:

                if not llm_query_text or not llm_query_text.strip():

                    st.session_state["llm_last_result"] = {
                        "hata": "Önce bir sorgu yazın.",
                    }

                else:

                    with st.spinner("Qwen3 sorguyu ayrıştırıyor..."):

                        try:

                            ham_parsed, _durum, ham = _llm_sorgula(
                                llm_query_text
                            )

                            if ham_parsed is None:

                                st.session_state["llm_last_result"] = {
                                    "hata": (
                                        "Model geçerli bir JSON "
                                        f"döndürmedi: {ham[:200]!r}"
                                    ),
                                }

                            else:

                                parsed, _duzeltmeler = _llm_son_islem(
                                    ham_parsed, llm_query_text
                                )

                                (
                                    kalan_filtreler,
                                    saat_listesi,
                                    saat_uyari,
                                ) = llm_gun_ici_saat_filtresini_ayikla(
                                    parsed.get("filtreler", [])
                                )

                                (
                                    kalan_filtreler,
                                    duration_filter,
                                    sure_uyari,
                                ) = llm_ucus_suresi_filtresini_ayikla(
                                    kalan_filtreler
                                )

                                baslangic_id = st.session_state.get(
                                    "value_filter_next_id", 0
                                )

                                satirlar, uyarilar = (
                                    llm_filtreleri_donustur(
                                        kalan_filtreler,
                                        get_numeric_columns(),
                                        baslangic_id,
                                    )
                                )

                                if saat_uyari:
                                    uyarilar.append(saat_uyari)

                                if sure_uyari:
                                    uyarilar.append(sure_uyari)

                                # --- Günün saati (gun_ici_saat) ---

                                saat_uygulandi = False
                                saat_haric = False

                                if saat_listesi is not None:

                                    st.session_state[
                                        "export_selected_hours"
                                    ] = saat_listesi

                                    saat_haric = llm_haric_mi(
                                        llm_query_text, "saat"
                                    )
                                    st.session_state[
                                        "export_hours_mode_exclude"
                                    ] = saat_haric

                                    saat_uygulandi = True

                                # --- Uçuş süresi (ucus_suresi) ---

                                sure_uygulandi = False

                                if duration_filter:

                                    def _llm_saat_to_hm(saat_ondalik):
                                        toplam_dk = round(
                                            float(saat_ondalik) * 60
                                        )
                                        return (
                                            toplam_dk // 60,
                                            toplam_dk % 60,
                                        )

                                    st.session_state[
                                        "export_duration_enabled"
                                    ] = True
                                    st.session_state[
                                        "export_duration_operator"
                                    ] = duration_filter["operator"]

                                    _h, _m = _llm_saat_to_hm(
                                        duration_filter["hours"]
                                    )
                                    st.session_state[
                                        "export_duration_hours_h"
                                    ] = _h
                                    st.session_state[
                                        "export_duration_hours_m"
                                    ] = _m

                                    if "hours2" in duration_filter:

                                        _h2, _m2 = _llm_saat_to_hm(
                                            duration_filter["hours2"]
                                        )
                                        st.session_state[
                                            "export_duration_hours2_h"
                                        ] = _h2
                                        st.session_state[
                                            "export_duration_hours2_m"
                                        ] = _m2

                                    sure_uygulandi = True

                                # --- Class (sınıf) -- deterministik eşleştirme ---

                                try:
                                    mevcut_siniflar = (
                                        get_available_classes()
                                    )
                                except Exception:
                                    mevcut_siniflar = []

                                sinif_eslesen = (
                                    llm_sinif_filtresini_belirle(
                                        llm_query_text, mevcut_siniflar
                                    )
                                )

                                sinif_uygulandi = False

                                if sinif_eslesen:
                                    st.session_state[
                                        "export_selected_classes"
                                    ] = sinif_eslesen
                                    sinif_uygulandi = True

                                # --- Uçuş (flight_id) -- deterministik eşleştirme ---

                                try:
                                    mevcut_ucuslar = (
                                        get_available_flights()
                                    )
                                except Exception:
                                    mevcut_ucuslar = []

                                ucus_eslesen = (
                                    llm_ucus_filtresini_belirle(
                                        llm_query_text, mevcut_ucuslar
                                    )
                                )

                                ucus_uygulandi = False
                                ucus_haric = False

                                if ucus_eslesen:

                                    st.session_state[
                                        "export_selected_flights"
                                    ] = ucus_eslesen

                                    ucus_haric = any(
                                        llm_haric_mi(
                                            llm_query_text,
                                            ucus,
                                            diger_ogeler=ucus_eslesen,
                                        )
                                        for ucus in ucus_eslesen
                                    )
                                    st.session_state[
                                        "export_flights_mode_exclude"
                                    ] = ucus_haric

                                    ucus_uygulandi = True

                                # --- Kolon seçimi -- deterministik eşleştirme ---

                                kolon_uygulandi = False
                                kolon_haric = False
                                kolon_eslesen = []

                                if llm_kolon_secimi_istegi_mi(
                                    llm_query_text
                                ):

                                    try:
                                        mevcut_kolonlar = (
                                            get_available_columns()
                                        )
                                    except Exception:
                                        mevcut_kolonlar = []

                                    kolon_eslesen = (
                                        llm_kolon_filtresini_belirle(
                                            llm_query_text,
                                            mevcut_kolonlar,
                                        )
                                    )

                                    if kolon_eslesen:

                                        kolon_haric = any(
                                            llm_haric_mi(
                                                llm_query_text,
                                                kolon,
                                                diger_ogeler=(
                                                    kolon_eslesen
                                                ),
                                            )
                                            for kolon in kolon_eslesen
                                        )

                                        st.session_state[
                                            "export_selected_columns"
                                        ] = kolon_eslesen
                                        st.session_state[
                                            "export_columns_mode_exclude"
                                        ] = kolon_haric

                                        kolon_uygulandi = True

                                    else:

                                        uyarilar.append(
                                            "Kolon seçimi istendi ama "
                                            "sorgudan mevcut "
                                            "kolonlarla eşleşen bir "
                                            "ad çıkarılamadı."
                                        )

                                mantik = str(
                                    parsed.get("mantik", "AND")
                                ).strip().upper()

                                if mantik == "OR" and len(satirlar) > 1:

                                    uyarilar.append(
                                        "Sorgu \"veya\" (OR) mantığı "
                                        "içeriyor; pano değer bazlı "
                                        "filtrelerde şu an yalnızca "
                                        "\"VE\" (AND) birleşimini "
                                        "destekliyor. Filtreler yine "
                                        "de eklendi ama hepsi birden "
                                        "(AND) uygulanacak."
                                    )

                                zaman_araligi = parsed.get(
                                    "zaman_araligi"
                                )

                                zaman_uygulandi = False
                                zaman_haric = False

                                if zaman_araligi:

                                    cozulen = llm_zaman_araligini_coz(
                                        zaman_araligi, max_time
                                    )

                                    if cozulen:

                                        (
                                            st.session_state[
                                                "export_start_time"
                                            ],
                                            st.session_state[
                                                "export_end_time"
                                            ],
                                        ) = cozulen

                                        zaman_haric = llm_haric_mi(
                                            llm_query_text,
                                            zaman_araligi,
                                        )
                                        st.session_state[
                                            "export_time_mode_exclude"
                                        ] = zaman_haric

                                        zaman_uygulandi = True

                                    else:

                                        uyarilar.append(
                                            "Zaman ifadesi "
                                            f"'{zaman_araligi}' "
                                            "çözümlenemedi, tarih "
                                            "aralığı değiştirilmedi."
                                        )

                                st.session_state["value_filters"] = (
                                    satirlar
                                )
                                st.session_state[
                                    "value_filter_next_id"
                                ] = baslangic_id + len(satirlar)

                                st.session_state["llm_last_result"] = {
                                    "parsed": parsed,
                                    "uygulanan": len(satirlar),
                                    "uyarilar": uyarilar,
                                    "zaman_uygulandi": zaman_uygulandi,
                                    "zaman_haric": zaman_haric,
                                    "saat_uygulandi": saat_uygulandi,
                                    "saat_haric": saat_haric,
                                    "sure_uygulandi": sure_uygulandi,
                                    "sinif_uygulandi": sinif_uygulandi,
                                    "sinif_eslesen": sinif_eslesen,
                                    "ucus_uygulandi": ucus_uygulandi,
                                    "ucus_eslesen": ucus_eslesen,
                                    "ucus_haric": ucus_haric,
                                    "kolon_uygulandi": kolon_uygulandi,
                                    "kolon_eslesen": kolon_eslesen,
                                    "kolon_haric": kolon_haric,
                                }

                        except Exception as exc:

                            st.session_state["llm_last_result"] = {
                                "hata": (
                                    "Qwen3'e ulaşılamadı ya da sorgu "
                                    f"işlenemedi: {exc}"
                                ),
                            }

                st.rerun()

            son_sonuc = st.session_state.get("llm_last_result")

            if son_sonuc:

                if son_sonuc.get("hata"):

                    st.error(son_sonuc["hata"])

                else:

                    if (
                        son_sonuc["uygulanan"] > 0
                        or son_sonuc["zaman_uygulandi"]
                        or son_sonuc["saat_uygulandi"]
                        or son_sonuc["sure_uygulandi"]
                        or son_sonuc["sinif_uygulandi"]
                        or son_sonuc["ucus_uygulandi"]
                        or son_sonuc["kolon_uygulandi"]
                    ):

                        ek_parcalar = [
                            f"{son_sonuc['uygulanan']} değer filtresi"
                        ]

                        if son_sonuc["zaman_uygulandi"]:
                            ek_parcalar.append(
                                "zaman aralığı"
                                + (
                                    " (hariç tut)"
                                    if son_sonuc["zaman_haric"]
                                    else ""
                                )
                            )

                        if son_sonuc["saat_uygulandi"]:
                            ek_parcalar.append(
                                "saat filtresi"
                                + (
                                    " (hariç tut)"
                                    if son_sonuc["saat_haric"]
                                    else ""
                                )
                            )

                        if son_sonuc["sure_uygulandi"]:
                            ek_parcalar.append("uçuş süresi")

                        if son_sonuc["sinif_uygulandi"]:
                            ek_parcalar.append(
                                f"{len(son_sonuc['sinif_eslesen'])} "
                                "class ("
                                + ", ".join(son_sonuc["sinif_eslesen"])
                                + ")"
                            )

                        if son_sonuc["ucus_uygulandi"]:
                            ek_parcalar.append(
                                f"{len(son_sonuc['ucus_eslesen'])} "
                                "uçuş ("
                                + ", ".join(son_sonuc["ucus_eslesen"])
                                + ")"
                                + (
                                    " (hariç tut)"
                                    if son_sonuc["ucus_haric"]
                                    else ""
                                )
                            )

                        if son_sonuc["kolon_uygulandi"]:
                            ek_parcalar.append(
                                f"{len(son_sonuc['kolon_eslesen'])} "
                                "kolon ("
                                + ", ".join(son_sonuc["kolon_eslesen"])
                                + ")"
                                + (
                                    " (hariç tut)"
                                    if son_sonuc["kolon_haric"]
                                    else ""
                                )
                            )

                        st.success(
                            "Uygulandı: " + " · ".join(ek_parcalar)
                        )

                    else:

                        st.info(
                            "Sorgudan uygulanabilir bir filtre "
                            "çıkarılamadı."
                        )

                    for uyari in son_sonuc.get("uyarilar", []):
                        st.warning(uyari)

                    with st.expander(
                        "Ayrıştırılan JSON (hata ayıklama)",
                        expanded=False,
                    ):
                        st.json(son_sonuc["parsed"])

    with st.expander(
        "🕒 Zaman ve Uçuş",
        expanded=True,
    ):

        # Uçuş seçimi (flight_id)
        #
        # Her uçuş bir kaynak dosyaya karşılık gelir (bkz. ingestion.py).
        # Burada seçilen uçuşlar, aşağıdaki tüm filtrelerle birlikte
        # kullanılır; en altta ise her uçuş için ayrı ayrı (ya da hepsi
        # birden ZIP olarak) CSV indirme imkanı sunulur.

        try:
            available_flights = get_available_flights()
        except Exception:
            available_flights = []

        selected_flights = []
        flights_mode = "include"

        if available_flights:

            if (
                "export_selected_flights" not in st.session_state
                and "selected_flights" in pending_url_state
            ):
                st.session_state["export_selected_flights"] = [
                    flight
                    for flight in pending_url_state["selected_flights"]
                    if flight in available_flights
                ]

            selected_flights = st.multiselect(
                "Uçuş(lar)",
                options=available_flights,
                help=(
                    "Boş bırakılırsa tüm uçuşlar dahil edilir. Birden fazla "
                    "uçuş seçerseniz, aşağıdaki filtrelere uyan satırlar her "
                    "uçuş için ayrı ayrı CSV olarak indirilebilir."
                ),
                key="export_selected_flights",
            )

            if selected_flights:

                if (
                    "export_flights_mode_exclude" not in st.session_state
                    and pending_url_state.get("flights_mode") == "exclude"
                ):
                    st.session_state["export_flights_mode_exclude"] = True

                flights_mode_exclude = st.checkbox(
                    "🔁 Seçilen uçuşları hariç tut",
                    help=(
                        "İşaretlenirse, örn. \"flight_1\" seçiliyken "
                        "flight_1 HARİÇ tüm uçuşlar gösterilir."
                    ),
                    key="export_flights_mode_exclude",
                )

                flights_mode = (
                    "exclude" if flights_mode_exclude else "include"
                )

            # Uçuş süresi filtresi
            #
            # Her uçuşun süresi min(time)/max(time) farkından hesaplanır
            # (bkz. build_clickhouse_where -> duration_filter); bu koşulu
            # sağlamayan uçuşların TÜM satırları elenir. Örn. "4 saatten
            # kısa uçuşları filtrele" -> operatör "<", saat 4.

            def _hours_to_h_m(hours_value: float) -> tuple:

                total_minutes = round(float(hours_value) * 60)

                return (
                    total_minutes // 60,
                    total_minutes % 60,
                )

            if (
                "export_duration_enabled" not in st.session_state
                and "duration_filter" in pending_url_state
            ):
                st.session_state["export_duration_enabled"] = True
                st.session_state["export_duration_operator"] = (
                    pending_url_state["duration_filter"]["operator"]
                )

                _h, _m = _hours_to_h_m(
                    pending_url_state["duration_filter"]["hours"]
                )
                st.session_state["export_duration_hours_h"] = _h
                st.session_state["export_duration_hours_m"] = _m

                _h2, _m2 = _hours_to_h_m(
                    pending_url_state["duration_filter"].get(
                        "hours2", 0.0
                    )
                )
                st.session_state["export_duration_hours2_h"] = _h2
                st.session_state["export_duration_hours2_m"] = _m2

            duration_enabled = st.checkbox(
                "⏱️ Uçuş süresine göre filtrele",
                help=(
                    "Örn. 4 saat 30 dakikadan kısa süren uçuşları "
                    "filtrelemek için operatörü \"<\", saati 4, dakikayı "
                    "30 seçin. Süre, her uçuşun ilk ve son telemetri "
                    "zaman damgası arasındaki farktan hesaplanır."
                ),
                key="export_duration_enabled",
            )

            duration_filter = None

            if duration_enabled:

                dcol1, dcol2, dcol3 = st.columns([2, 3, 3])

                duration_operator_options = list(
                    VALUE_FILTER_OPERATORS.keys()
                ) + [RANGE_FILTER_OPERATOR]

                with dcol1:

                    duration_operator = st.selectbox(
                        "Operatör",
                        options=duration_operator_options,
                        format_func=lambda op: (
                            "aralıkta (min–maks)"
                            if op == RANGE_FILTER_OPERATOR
                            else op
                        ),
                        key="export_duration_operator",
                    )

                is_duration_range = (
                    duration_operator == RANGE_FILTER_OPERATOR
                )

                with dcol2:

                    st.caption(
                        "Min Süre" if is_duration_range else "Süre"
                    )

                    dh1, dm1 = st.columns(2)

                    with dh1:

                        duration_hours_h = st.number_input(
                            "Saat",
                            min_value=0,
                            value=int(
                                st.session_state.get(
                                    "export_duration_hours_h", 4
                                )
                            ),
                            step=1,
                            key="export_duration_hours_h",
                        )

                    with dm1:

                        duration_hours_m = st.number_input(
                            "Dakika",
                            min_value=0,
                            max_value=59,
                            value=int(
                                st.session_state.get(
                                    "export_duration_hours_m", 0
                                )
                            ),
                            step=1,
                            key="export_duration_hours_m",
                        )

                duration_hours = duration_hours_h + duration_hours_m / 60.0

                duration_hours2 = None

                if is_duration_range:

                    with dcol3:

                        st.caption("Maks Süre")

                        dh2, dm2 = st.columns(2)

                        with dh2:

                            duration_hours2_h = st.number_input(
                                "Saat",
                                min_value=0,
                                value=int(
                                    st.session_state.get(
                                        "export_duration_hours2_h", 8
                                    )
                                ),
                                step=1,
                                key="export_duration_hours2_h",
                            )

                        with dm2:

                            duration_hours2_m = st.number_input(
                                "Dakika",
                                min_value=0,
                                max_value=59,
                                value=int(
                                    st.session_state.get(
                                        "export_duration_hours2_m", 0
                                    )
                                ),
                                step=1,
                                key="export_duration_hours2_m",
                            )

                        duration_hours2 = (
                            duration_hours2_h + duration_hours2_m / 60.0
                        )

                duration_filter = {
                    "operator": duration_operator,
                    "hours": duration_hours,
                }

                if duration_hours2 is not None:
                    duration_filter["hours2"] = duration_hours2

        else:

            st.info(
                "Tabloda 'flight_id' kolonu bulunamadı, uçuş bazlı filtre "
                "kullanılamıyor. Pipeline'ı bu güncellemeyle tekrar "
                "çalıştırdığınızda bu alan otomatik olarak dolacaktır."
            )

            duration_filter = None

        st.divider()

        if (
            "export_start_time" not in st.session_state
            and "start_time" in pending_url_state
        ):
            st.session_state["export_start_time"] = pending_url_state[
                "start_time"
            ]

        if (
            "export_end_time" not in st.session_state
            and "end_time" in pending_url_state
        ):
            st.session_state["export_end_time"] = pending_url_state[
                "end_time"
            ]

        col1, col2 = st.columns(2)

        # Varsayılan olarak tarih aralığı UYGULANMAZ: her iki alan da boş
        # gelir, sorguya zaman koşulu eklenmez (tüm veri dahil). Kullanıcı
        # bir alt/üst sınır isterse ilgili alanı doldurur; yalnızca biri
        # doldurulursa tek yönlü sınır uygulanır (bkz. build_clickhouse_where).
        with col1:

            start_time = st.datetime_input(
                "Başlangıç zamanı",
                value=None,
                key="export_start_time",
                help=(
                    f"Boş bırakılırsa alt sınır uygulanmaz. Verinin en "
                    f"erken zamanı: {min_time}"
                ),
            )

        with col2:

            end_time = st.datetime_input(
                "Bitiş zamanı",
                value=None,
                key="export_end_time",
                help=(
                    f"Boş bırakılırsa üst sınır uygulanmaz. Verinin en "
                    f"geç zamanı: {max_time}"
                ),
            )

        if (
            "export_time_mode_exclude" not in st.session_state
            and pending_url_state.get("time_mode") == "exclude"
        ):
            st.session_state["export_time_mode_exclude"] = True

        time_mode_exclude = st.checkbox(
            "🔁 Bu tarih aralığını hariç tut (yalnızca DIŞINDAKİ satırlar)",
            help=(
                "İşaretlenirse, başlangıç/bitiş zamanı arasındaki satırlar "
                "DEĞİL, bu aralığın dışında kalan (öncesi ve sonrası) "
                "satırlar filtreye dahil edilir."
            ),
            key="export_time_mode_exclude",
        )

        time_mode = "exclude" if time_mode_exclude else "include"

        # Saat bazlı filtre (günün saati, tarihten/uçuştan bağımsız)
        #
        # Yukarıdaki başlangıç/bitiş zamanı belirli bir TARİH aralığını
        # sınırlar; bu filtre ise günün belirli SAATLERİNİ (0-23) seçer --
        # örn. "saat 7" seçilirse, aralıktaki tüm uçuşlarda saat 07:00-
        # 07:59 arasına denk gelen satırlar (hangi güne/uçuşa ait olursa
        # olsun) filtreye dahil edilir. toHour(time) ile eşleştirilir.

        if (
            "export_selected_hours" not in st.session_state
            and "selected_hours" in pending_url_state
        ):
            st.session_state["export_selected_hours"] = pending_url_state[
                "selected_hours"
            ]

        selected_hours = st.multiselect(
            "Saat (0-23)",
            options=list(range(24)),
            help=(
                "Boş bırakılırsa tüm saatler dahil edilir. Seçilen "
                "saat(ler), tarihten ve uçuştan bağımsız olarak günün o "
                "saatine denk gelen satırları filtreler (örn. 7 seçilirse "
                "tüm uçuşlarda saat 07:00-07:59 arası satırlar gösterilir)."
            ),
            format_func=lambda hour: f"{hour:02d}:00",
            key="export_selected_hours",
        )

        hours_mode = "include"

        if selected_hours:

            if (
                "export_hours_mode_exclude" not in st.session_state
                and pending_url_state.get("hours_mode") == "exclude"
            ):
                st.session_state["export_hours_mode_exclude"] = True

            hours_mode_exclude = st.checkbox(
                "🔁 Seçilen saatleri hariç tut",
                help=(
                    "İşaretlenirse, örn. saat 18/19/20 seçiliyken "
                    "yalnızca 18:00-21:00 aralığının DIŞINDAKİ satırlar "
                    "gösterilir."
                ),
                key="export_hours_mode_exclude",
            )

            hours_mode = "exclude" if hours_mode_exclude else "include"

    # NOT: Bu bölüm kasıtlı olarak st.expander() İÇİNDE DEĞİL. Folium/
    # Leaflet haritası mount olurken kapsayıcısının genişliğini/
    # yüksekliğini bir kez ölçüyor; st.expander kapalıyken (varsayılan
    # expanded=False) bu ölçüm 0 çıkıyor ve harita kullanıcı expander'ı
    # açtıktan sonra bile boş/görünmez kalıyor (bilinen bir streamlit-
    # folium/streamlit sorunu). st.container(border=True) aynı görsel
    # kutuyu verir ama içeriği gizlemediği için bu sorun oluşmuyor.

    st.markdown(
        "##### 🗺️ Alan Bazlı Filtre (Haritadan Seç)"
    )

    # Ek güvenlik önlemi -- iki ayrı streamlit-folium sorununa karşı:
    #  1) İlk render'da harita iframe'i yüksekliği 0 ile çiziliyor
    #     (sayfa yenilenince düzeliyor).
    #  2) streamlit-folium'un JS tarafı, Leaflet "draw:*"/"overlayadd"
    #     olaylarında Streamlit.setFrameHeight()'ı argümansız çağırıyor;
    #     bu da varsayılan olarak iframe İÇİNDEKİ document.body.
    #     scrollHeight'ı kullanıyor. Bu ölçüm kararsız (aynı sayfa
    #     birkaç kez yeniden yüklendiğinde bazen 420px yerine ~1900px
    #     ölçülüyor) ve iframe'i haritanın gerçek boyutundan çok daha
    #     uzun çizip altında koca bir boş alan bırakıyor, sayfanın
    #     geri kalanını aşağı itiyordu ("haritanın konumu bozuk"
    #     şikayeti buradan geliyordu). min-height tek başına bunu
    #     engellemiyor çünkü sorun MAKSİMUM yüksekliğin kaçması;
    #     height'ı !important ile sabitlemek, JS'in inline stilde
    #     (!important OLMADAN) ayarladığı değeri geçersiz kılıyor.
    st.markdown(
        """
        <style>
        iframe[title="streamlit_folium.st_folium"] {
            height: 420px !important;
            min-height: 420px;
            max-height: 420px !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    with st.container(border=True):

        try:
            available_columns = get_available_columns()
            has_lat_lon = (
                resolve_clickhouse_column(
                    "latitude",
                    available_columns,
                ) is not None
                and resolve_clickhouse_column(
                    "longitude",
                    available_columns,
                ) is not None
            )
        except Exception:
            has_lat_lon = False

        area_polygons = []
        area_mode = "include"

        if not has_lat_lon:

            st.info(
                "Tabloda 'latitude'/'longitude' kolonu bulunamadı, "
                "alan bazlı filtre kullanılamıyor."
            )

        else:

            st.caption(
                "Haritada dikdörtgen ya da poligon çizerek alan(lar) "
                "belirleyin (sol üstteki çizim araçlarını kullanın). "
                "Birden fazla şekil çizebilirsiniz; birden fazla alan "
                "çizildiğinde OR ile birleştirilir (\"bölge A veya bölge "
                "B\")."
            )

            if (
                "export_area_mode" not in st.session_state
                and "area_mode" in pending_url_state
            ):
                st.session_state["export_area_mode"] = pending_url_state[
                    "area_mode"
                ]

            area_mode = st.radio(
                "Alan modu",
                options=["include", "exclude"],
                format_func=lambda mode: (
                    "🎯 Dahil et (yalnızca alan(lar)ın İÇİNDEKİ satırlar)"
                    if mode == "include"
                    else "🚫 Hariç tut (alan(lar)ın DIŞINDAKİ satırlar)"
                ),
                help=(
                    "\"Hariç tut\" seçilirse, çizdiğiniz alan(lar)ın "
                    "içinde kalan satırlar filtreden ÇIKARILIR — örn. "
                    "Erzurum'u çevreleyen bir alan çizip \"Hariç tut\" "
                    "seçerek Erzurum DIŞINDAKİ uçuşları görebilirsiniz."
                ),
                horizontal=True,
                key="export_area_mode",
            )

            (
                min_lat,
                max_lat,
                min_lon,
                max_lon,
            ) = get_lat_lon_bounds()

            if min_lat is None:
                map_center = [0.0, 0.0]
                map_zoom = 2
            else:
                map_center = [
                    (min_lat + max_lat) / 2,
                    (min_lon + max_lon) / 2,
                ]
                map_zoom = 11

            # Haritanın kendisi (folium.Map) her rerun'da yeniden
            # kurulur, ama st_folium bileşeni aynı "key" ile aynı
            # widget kabul edildiği için önceki çizimini (all_drawings)
            # döndürmeye devam eder -- "🗑️ Alanı Temizle" butonu bu
            # key'i değiştirerek çizimi gerçekten sıfırlar.

            area_map_version = st.session_state.get(
                "area_map_version",
                0,
            )

            # Paylaşılan bir bağlantı ("🔗 Bu Filtreleri Bağlantı Olarak
            # Paylaş") harita üzerinde çizilmiş alan(lar)ı da URL'ye
            # kodluyor (bkz. export_ap / _decode_export_state_from_query_
            # params). O bağlantı açıldığında -- "Alanı Temizle" ile
            # reddedilmediği sürece -- URL'deki poligon(lar) hem AKTİF
            # filtre olarak kullanılır hem de haritada kesikli çizgiyle
            # gösterilir.
            #
            # ÖNEMLİ: bağlantıyı açan kişi haritaya KENDİ ek alan(lar)ını
            # çizerse, bunlar URL'den gelen alan(lar)ı SİLMEZ -- ikisi
            # birlikte (OR ile) uygulanır. Eskiden herhangi bir çizim
            # yapılması URL'deki alanı kalıcı olarak devre dışı
            # bırakıyordu (bkz. git geçmişi); bu, bağlantıyı paylaşan
            # kişinin filtresinin, alıcı haritaya ekstra bir bölge
            # eklediği anda sessizce kaybolmasına yol açıyordu.

            url_seed_polygons = pending_url_state.get("area_polygons")
            url_seed_dismissed = st.session_state.get(
                "area_filter_url_seed_dismissed",
                False,
            )

            seed_active = bool(url_seed_polygons and not url_seed_dismissed)

            area_map = folium.Map(
                location=map_center,
                zoom_start=map_zoom,
                tiles="CartoDB dark_matter",
            )

            Draw(
                export=False,
                draw_options={
                    "polyline": False,
                    "circle": False,
                    "circlemarker": False,
                    "marker": False,
                    "polygon": {"allowIntersection": False},
                    "rectangle": True,
                },
                edit_options={"edit": True},
            ).add_to(area_map)

            if seed_active:

                # Paylaşılan alan(lar), Draw eklentisinin KENDİ
                # düzenlenebilir katmanına (_SeedIntoDrawLayer) enjekte
                # edilir -- böylece kullanıcı bu alan(lar)ı haritanın
                # kendi düzenleme/silme aracıyla, tıpkı kendi çizdiği bir
                # şekil gibi seçip silebilir (eskiden ayrı, düzenlenemez
                # bir katman olarak eklendiği için harita üzerindeki
                # silme aracı bunları hiç "görmüyordu"). `Draw(...)
                # .add_to(...)`dan SONRA eklenmesi ŞART -- enjeksiyon
                # script'i, Draw'ın `drawnItems` katmanını henüz
                # tanımlamadığı bir ana denk gelirse kısa bir süre
                # (polling ile) bekler.
                _SeedIntoDrawLayer(url_seed_polygons).add_to(area_map)

            map_output = st_folium(
                area_map,
                key=f"area_filter_map_{area_map_version}",
                height=420,
                use_container_width=True,
                returned_objects=["all_drawings"],
            )

            raw_drawings = (map_output or {}).get("all_drawings")

            skipped_degenerate_shapes = 0
            drawn_polygons = []

            for drawing in (raw_drawings or []):

                geometry = drawing.get("geometry", {})

                if geometry.get("type") == "Polygon":

                    ring = geometry.get("coordinates", [[]])[0]

                    ring_points = [
                        (point[0], point[1])
                        for point in ring
                    ]

                    if _polygon_has_area(ring_points):
                        drawn_polygons.append(ring_points)
                    else:
                        skipped_degenerate_shapes += 1

            # `returned_objects=["all_drawings"]` verildiği için
            # streamlit-folium, yalnızca GERÇEK bir çizim olayı (şekil
            # çizme/düzenleme/SİLME) tetiklendiğinde bileşen değerini
            # günceller (bkz. streamlit-folium frontend kaynağı) --
            # tarayıcıda henüz böyle bir olay olmadıysa `raw_drawings`
            # None kalır. Bu durumda haritanın `drawnItems` katmanına
            # enjekte edilen paylaşılan alan(lar) GÖRSEL OLARAK oradadır
            # ama tarayıcı henüz bunu Python'a BİLDİRMEMİŞTİR -- bu
            # yüzden filtre için doğrudan URL'deki değer kullanılır.
            # `raw_drawings` bir kez GERÇEK bir liste hâline geldiğinde
            # (boş liste dahil, örn. paylaşılan alan haritadan
            # silindiğinde) artık TEK doğru kaynak odur -- haritanın
            # `drawnItems` katmanının O ANKİ gerçek içeriğini (silinen
            # paylaşılan alan(lar) dahil) yansıtır.
            if raw_drawings is None:
                area_polygons = list(url_seed_polygons) if seed_active else []
            else:
                area_polygons = drawn_polygons

            # Fare sürüklenmeden (tek tıkla) çizilen sıfır boyutlu bir
            # dikdörtgen gibi durumlarda ring geçerli bir alan oluşturmaz
            # (bkz. _polygon_has_area) -- bu ClickHouse'a gönderilirse
            # "Geometry has wrong topological dimension" hatasıyla sorgu
            # tamamen patlardı; bu yüzden burada sessizce elenir ve
            # kullanıcı bilgilendirilir.
            if skipped_degenerate_shapes:
                st.warning(
                    f"⚠️ {skipped_degenerate_shapes} şekil çok küçük/"
                    "geçersiz olduğu için yok sayıldı (haritada "
                    "sürükleyerek gerçek boyutlu bir alan çizin)."
                )

            area_summary_suffix = (
                "içindeki satırlar"
                if area_mode == "include"
                else "dışındaki satırlar"
            )

            if raw_drawings is None and seed_active:

                st.info(
                    f"🔗 {len(url_seed_polygons)} alan paylaşılan "
                    "bağlantıdan yüklendi (haritada kesikli çizgiyle "
                    f"gösteriliyor) — {area_summary_suffix} aşağıdaki "
                    "filtrelerle birlikte uygulanacak. Kaldırmak için "
                    "haritanın sol üstündeki düzenleme/silme (✏️/🗑️) "
                    "aracını kullanabilir ya da yeni bir alan daha "
                    "çizebilirsiniz."
                )

            elif area_polygons:

                st.success(
                    f"✅ {len(area_polygons)} alan aktif — "
                    f"{area_summary_suffix} aşağıdaki filtrelerle "
                    "birlikte (bölgeler OR ile birleştirilerek) "
                    "uygulanacak. Kaldırmak için haritada seçip "
                    "düzenleme/silme (✏️/🗑️) aracını kullanın."
                )

            else:

                st.caption(
                    "Henüz bir alan seçilmedi — tüm konumlar dahil "
                    "edilecek."
                )

            if area_polygons:

                if st.button(
                    "🗑️ Alanı Temizle",
                    key="clear_area_filter_btn",
                ):
                    st.session_state["area_filter_url_seed_dismissed"] = True
                    st.session_state["area_map_version"] = (
                        area_map_version + 1
                    )

                    # Sadece session_state'te reddetmek yetmez -- URL'de
                    # export_ap/export_am hâlâ duruyorsa, sayfa yenilenince
                    # (F5) ya da link tekrar açılınca (session_state
                    # sıfırlandığı için) alan filtresi geri gelir. Bu
                    # yüzden URL'den de kalıcı olarak silinir.
                    for stale_key in ("export_ap", "export_am"):
                        if stale_key in st.query_params:
                            del st.query_params[stale_key]

                    st.rerun()

    with st.expander(
        "🎯 Class ve Değer Bazlı Filtreler",
        expanded=False,
    ):

        # Class filtresi

        try:
            available_classes = get_available_classes()
        except Exception:
            available_classes = []

        try:
            class_is_object_mode = (
                "class" not in get_available_columns()
            )
        except Exception:
            class_is_object_mode = False

        selected_classes = []

        if available_classes:

            if (
                "export_selected_classes" not in st.session_state
                and "selected_classes" in pending_url_state
            ):
                st.session_state["export_selected_classes"] = [
                    cls
                    for cls in pending_url_state["selected_classes"]
                    if cls in available_classes
                ]

            selected_classes = st.multiselect(
                "Nesne türü (frame'de en az 1 adet)"
                if class_is_object_mode
                else "Class",
                options=available_classes,
                help=(
                    "Seçilen nesne türlerinden en az birini içeren "
                    "frame'ler gösterilir (obj_X > 0). Boş bırakılırsa "
                    "tümü dahil edilir."
                    if class_is_object_mode
                    else "Boş bırakılırsa tüm class değerleri seçilir."
                ),
                key="export_selected_classes",
            )

        # Değer bazlı satır filtresi (örn. altitude < 23)

        st.caption(
            "Sayısal bir kolon için koşul tanımlayarak satır bazlı arama "
            "yapabilirsiniz. Örn: `altitude < 23` gibi. \"aralıkta "
            "(min–maks)\" operatörüyle iki değer arasındaki satırları da "
            "filtreleyebilirsiniz (örn. `10 <= altitude <= 50`). Birden "
            "fazla filtre eklenirse hepsi AND ile birleştirilir."
        )

        try:
            numeric_columns = get_numeric_columns()
        except Exception:
            numeric_columns = []

        if "value_filters" not in st.session_state:

            restored_value_filters = []

            for index, vf in enumerate(
                pending_url_state.get("value_filters", [])
            ):

                valid_operator = (
                    vf["operator"] in VALUE_FILTER_OPERATORS
                    or vf["operator"] == RANGE_FILTER_OPERATOR
                )

                if (
                    vf["column"] in numeric_columns
                    or vf["column"] in COMPUTED_VALUE_COLUMNS
                ) and valid_operator:

                    restored_filter_row = {
                        "id": index,
                        "column": vf["column"],
                        "operator": vf["operator"],
                        "value": vf["value"],
                        "exclude": bool(vf.get("exclude", False)),
                    }

                    if "value2" in vf:
                        restored_filter_row["value2"] = vf["value2"]

                    restored_value_filters.append(restored_filter_row)

            st.session_state["value_filters"] = restored_value_filters
            st.session_state["value_filter_next_id"] = len(
                restored_value_filters
            )

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
                        "exclude": False,
                    }
                )

                st.session_state["value_filter_next_id"] += 1

            rows_to_remove = []

            for filter_row in st.session_state["value_filters"]:

                row_id = filter_row["id"]

                fcol1, fcol2, fcol3, fcol4, fcol5, fcol6 = st.columns(
                    [2.2, 1.8, 1.8, 1.8, 1.2, 0.8]
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
                    ) + [RANGE_FILTER_OPERATOR]

                    filter_row["operator"] = st.selectbox(
                        "Operatör",
                        options=operator_options,
                        index=operator_options.index(
                            filter_row["operator"]
                        )
                        if filter_row["operator"] in operator_options
                        else 0,
                        format_func=lambda op: (
                            "aralıkta (min–maks)"
                            if op == RANGE_FILTER_OPERATOR
                            else op
                        ),
                        key=f"value_filter_operator_{row_id}",
                    )

                is_range_filter = (
                    filter_row["operator"] == RANGE_FILTER_OPERATOR
                )

                with fcol3:

                    filter_row["value"] = st.number_input(
                        "Min" if is_range_filter else "Değer",
                        value=float(filter_row.get("value", 0.0)),
                        key=f"value_filter_value_{row_id}",
                        format="%.4f",
                    )

                with fcol4:

                    if is_range_filter:

                        filter_row["value2"] = st.number_input(
                            "Maks",
                            value=float(
                                filter_row.get("value2", 0.0)
                            ),
                            key=f"value_filter_value2_{row_id}",
                            format="%.4f",
                        )

                    else:

                        filter_row.pop("value2", None)

                with fcol5:

                    st.write("")
                    st.write("")

                    filter_row["exclude"] = st.checkbox(
                        "Hariç tut",
                        value=bool(filter_row.get("exclude", False)),
                        help=(
                            "İşaretlenirse bu koşulu SAĞLAYAN satırlar "
                            "değil, sağlamayan satırlar gösterilir (örn. "
                            "\"altitude aralıkta 10-50\" + hariç tut = "
                            "\"altitude 10-50 aralığında DEĞİL\")."
                        ),
                        key=f"value_filter_exclude_{row_id}",
                    )

                with fcol6:

                    st.write("")
                    st.write("")

                    if st.button(
                        "🗑️",
                        key=f"value_filter_remove_{row_id}",
                    ):
                        rows_to_remove.append(row_id)

                # Maks < min olsa da sorgu hâlâ çalışır (build_clickhouse_where
                # değerleri otomatik sıralar) ama kullanıcı muhtemelen yanlışlıkla
                # ters girmiştir -- bu yüzden burada sadece bilgilendirici bir
                # uyarı gösterilir, filtre engellenmez.
                if (
                    is_range_filter
                    and filter_row.get("value2", 0.0) < filter_row.get("value", 0.0)
                ):
                    st.caption(
                        "⚠️ Maks değeri min değerinden küçük — filtre yine "
                        "de çalışır (değerler otomatik sıralanır), ancak "
                        "girdiğiniz aralığı kontrol etmek isteyebilirsiniz."
                    )

            if rows_to_remove:

                st.session_state["value_filters"] = [
                    filter_row
                    for filter_row in st.session_state["value_filters"]
                    if filter_row["id"] not in rows_to_remove
                ]

                st.rerun()

            if st.session_state["value_filters"]:

                def _format_value_filter(filter_row: dict) -> str:

                    if filter_row["operator"] == RANGE_FILTER_OPERATOR:

                        text = (
                            f"{filter_row['column']} aralıkta "
                            f"[{filter_row['value']} , "
                            f"{filter_row.get('value2', 0.0)}]"
                        )

                    else:

                        text = (
                            f"{filter_row['column']} "
                            f"{filter_row['operator']} "
                            f"{filter_row['value']}"
                        )

                    if filter_row.get("exclude"):
                        return f"DEĞİL({text})"

                    return text

                filter_summary = " AND ".join(
                    _format_value_filter(filter_row)
                    for filter_row in st.session_state["value_filters"]
                )

                st.caption(
                    f"Aktif filtre: `{filter_summary}`"
                )

    value_filters = st.session_state.get(
        "value_filters",
        [],
    )

    with st.expander(
        "📋 Kolonlar",
        expanded=False,
    ):

        available_columns = (
            get_available_columns()
        )

        default_columns = [
            column
            for column in AU_AIR_COLUMNS
            if column in available_columns
        ]

        if (
            "export_selected_columns" not in st.session_state
            and "selected_columns" in pending_url_state
        ):
            st.session_state["export_selected_columns"] = [
                col
                for col in pending_url_state["selected_columns"]
                if col in available_columns
            ]

        if (
            "export_columns_mode_exclude" not in st.session_state
            and pending_url_state.get("columns_mode") == "exclude"
        ):
            st.session_state["export_columns_mode_exclude"] = True

        columns_mode_exclude = st.checkbox(
            "🔁 Seçilen kolonları hariç tut (geri kalanların tümünü göster)",
            help=(
                "İşaretlenmezse aşağıda seçtiğiniz kolonlar gösterilir. "
                "İşaretlenirse seçtiğiniz kolonlar ÇIKARILIR, geri kalan "
                "tüm kolonlar gösterilir -- çoğu kolonu isteyip yalnızca "
                "birkaçını istemediğinizde tek tek seçmekten daha "
                "hızlıdır."
            ),
            key="export_columns_mode_exclude",
        )

        columns_mode = "exclude" if columns_mode_exclude else "include"

        # "Dahil et" ve "hariç tut" listeleri anlamca zıt (birinde
        # gösterilecek, diğerinde gizlenecek kolonlar). Aynı widget
        # key'i kullanıldığı için, mod değiştirildiğinde Streamlit eski
        # seçimi olduğu gibi taşıyıp yanlış listeyi doldurmuş oluyordu
        # (ör. "sadece time+altitude göster" seçiliyken hariç-tut'a
        # geçince "time+altitude'u gizle"ye dönüşüyordu). Mod her
        # değiştiğinde seçim sıfırlanır ki widget moduna uygun
        # default'la (boş / tüm AU-AIR kolonları) yeniden başlasın.
        _prev_columns_mode_exclude = st.session_state.get(
            "_export_columns_mode_exclude_prev"
        )

        if (
            _prev_columns_mode_exclude is not None
            and _prev_columns_mode_exclude != columns_mode_exclude
        ):
            st.session_state.pop("export_selected_columns", None)

        st.session_state["_export_columns_mode_exclude_prev"] = (
            columns_mode_exclude
        )

        selected_columns = st.multiselect(
            "Hariç tutulacak kolonlar"
            if columns_mode_exclude
            else "Gösterilecek / dışa aktarılacak kolonlar",
            options=available_columns,
            default=[] if columns_mode_exclude else default_columns,
            key="export_selected_columns",
        )

        if columns_mode_exclude:
            columns = (
                [
                    col
                    for col in available_columns
                    if col not in selected_columns
                ]
                if selected_columns
                else None
            )
        else:
            columns = (
                selected_columns
                if selected_columns
                else None
            )

        # flight_id kolonu, aşağıdaki "uçuş bazlı ayrı dosyalar" bölümünün
        # doğru çalışabilmesi için (satırları uçuşa göre gruplamak amacıyla)
        # her zaman sorguya dahil edilir — kullanıcı Kolon Seçimi'nden onu
        # çıkarmış olsa bile.
        if available_flights and columns is not None and "flight_id" not in columns:

            columns = columns + ["flight_id"]

            st.caption(
                "ℹ️ `flight_id` kolonu, uçuş bazlı dışa aktarma için "
                "otomatik olarak dahil edildi."
            )

    if start_time is not None and end_time is not None and start_time > end_time:

        st.error(
            "Başlangıç zamanı bitiş zamanından sonra olamaz."
        )

        return

    # Seçilen filtrelerin kısa özeti — expander'ları açmadan da hangi
    # filtrelerin aktif olduğu tek bakışta görülsün diye.

    active_filters = []

    if time_mode == "exclude":
        active_filters.append("tarih aralığı (hariç tut)")

    if selected_flights:
        flight_label = f"{len(selected_flights)} uçuş"
        if flights_mode == "exclude":
            flight_label += " (hariç tut)"
        active_filters.append(flight_label)

    if selected_hours:
        hour_label = f"{len(selected_hours)} saat"
        if hours_mode == "exclude":
            hour_label += " (hariç tut)"
        active_filters.append(hour_label)

    if selected_classes:
        active_filters.append(
            f"{len(selected_classes)} nesne türü"
            if class_is_object_mode
            else f"{len(selected_classes)} class"
        )

    if area_polygons:
        area_label = (
            f"harita alanı ({len(area_polygons)})"
            if len(area_polygons) > 1
            else "harita alanı"
        )
        if area_mode == "exclude":
            area_label += " (hariç tut)"
        active_filters.append(area_label)

    if value_filters:
        active_filters.append(f"{len(value_filters)} değer filtresi")

    if duration_filter:
        active_filters.append("uçuş süresi")

    if columns_mode_exclude and selected_columns:
        active_filters.append(f"{len(selected_columns)} kolon (hariç tut)")

    if active_filters:
        st.caption("🔎 Aktif filtre: " + " · ".join(active_filters))
    else:
        st.caption("🔎 Aktif filtre yok — tüm veri dahil edilecek.")

    # Şu anki filtre durumu, URL query parametresi olarak kodlanır --
    # kendisi burada bir UI göstermez, sadece hazırlanır. "🔗 Bu
    # Filtreleri Bağlantı Olarak Paylaş" seçeneği "3️⃣ İndir" adımında,
    # indirme formatı seçiminin hemen yanında gösterilir (bkz.
    # render_download_section) -- bu sayede paylaşılan bağlantı, seçilen
    # indirme formatını da (export_fmt) içerebilir.

    share_params = _encode_export_state_to_query_params(
        start_time=start_time,
        end_time=end_time,
        selected_flights=selected_flights,
        selected_classes=selected_classes,
        selected_columns=selected_columns,
        value_filters=value_filters,
        area_polygons=area_polygons,
        selected_hours=selected_hours,
        duration_filter=duration_filter,
        area_mode=area_mode,
        time_mode=time_mode,
        hours_mode=hours_mode,
        flights_mode=flights_mode,
        columns_mode=columns_mode,
    )

    # Filtreler, en son "Satır Sayısını Hesapla" / "Veriyi Getir ve
    # Önizle" ile hesaplanan sonuçtan farklıysa (kullanıcı bir filtreyi
    # tamamlayıp yeni bir filtrelemeye geçtiyse), aşağıda eski filtreye
    # ait satır sayısı / önizleme / indirme hâlâ açık kalmasın diye
    # session_state'teki eski sonuçlar temizlenir.

    filter_signature = (
        start_time,
        end_time,
        time_mode,
        tuple(sorted(selected_classes)),
        tuple(sorted(selected_flights)),
        flights_mode if selected_flights else None,
        tuple(sorted(selected_hours)) if selected_hours else None,
        hours_mode if selected_hours else None,
        tuple(columns) if columns else None,
        tuple(
            (
                value_filter["column"],
                value_filter["operator"],
                value_filter["value"],
                value_filter.get("value2"),
                bool(value_filter.get("exclude", False)),
            )
            for value_filter in value_filters
        ),
        tuple(
            tuple(polygon) for polygon in area_polygons
        ) if area_polygons else None,
        area_mode if area_polygons else None,
        (
            duration_filter["operator"],
            duration_filter["hours"],
            duration_filter.get("hours2"),
        )
        if duration_filter
        else None,
    )

    if st.session_state.get("export_filters_signature") != filter_signature:

        had_stale_result = (
            st.session_state.get("export_row_count") is not None
            or st.session_state.get("export_df") is not None
        )

        st.session_state.pop("export_row_count", None)
        st.session_state.pop("export_df", None)

        if had_stale_result:
            st.info(
                "Filtreler değişti. Güncel sonucu görmek için satır "
                "sayısını yeniden hesaplayın."
            )

    st.divider()

    # ==========================================================
    # ADIM 2 — VERİYİ GETİR
    # ==========================================================
    #
    # İki adımlı: önce satır sayısı hesaplanır (export limitini aşan
    # bir sorguyu ClickHouse'a göndermemek için), ardından veri
    # gerçekten getirilip önizlenir.

    st.subheader(
        "2️⃣ Veriyi Getir"
    )

    st.caption(
        "Önce satır sayısını hesaplayın, ardından veriyi getirip önizleyin."
    )

    if st.button(
        "🔢 Satır Sayısını Hesapla",
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
                    columns=columns,
                    value_filters=value_filters,
                    selected_flights=selected_flights,
                    area_polygons=area_polygons,
                    selected_hours=selected_hours,
                    duration_filter=duration_filter,
                    area_mode=area_mode,
                    time_mode=time_mode,
                    hours_mode=hours_mode,
                    flights_mode=flights_mode,
                )

            st.session_state[
                "export_row_count"
            ] = row_count

            st.session_state[
                "export_filters_signature"
            ] = filter_signature

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

    if row_count == 0:

        st.warning(
            "Seçilen filtrelerle eşleşen veri yok."
        )

        return

    if st.button(
        "📥 Veriyi Getir ve Önizle",
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
                    selected_flights=selected_flights,
                    area_polygons=area_polygons,
                    selected_hours=selected_hours,
                    duration_filter=duration_filter,
                    area_mode=area_mode,
                    time_mode=time_mode,
                    hours_mode=hours_mode,
                    flights_mode=flights_mode,
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

    st.info(
        f"Toplam {len(dataframe):,} satır getirildi."
    )

    if area_polygons and "flight_id" in dataframe.columns:

        flights_in_area = sorted(
            dataframe["flight_id"].dropna().unique().tolist()
        )

        # area_mode="exclude" iken dataframe, seçilen alan(lar)a hiç
        # girmemiş uçuşların satırlarını içerir (bkz. build_clickhouse_
        # where) -- bu yüzden buradaki flight_id listesi "alanda uçmuş"
        # değil, tam tersine "alana hiç girmemiş" uçuşları temsil eder.
        if area_mode == "exclude":

            if flights_in_area:

                st.success(
                    f"🗺️ Seçilen alan(lar)a hiç girmemiş "
                    f"**{len(flights_in_area)}** uçuş bulundu: "
                    f"{', '.join(flights_in_area)}"
                )

            else:

                st.warning(
                    "Seçilen alan(lar)a girmemiş bir uçuş bulunamadı "
                    "(tüm uçuşlar en az bir kez bu alan(lar)a girmiş)."
                )

        else:

            if flights_in_area:

                st.success(
                    f"🗺️ Seçilen alanda uçmuş **{len(flights_in_area)}** "
                    f"uçuş bulundu: {', '.join(flights_in_area)}"
                )

            else:

                st.warning(
                    "Seçilen alanda uçmuş bir uçuş bulunamadı."
                )

    with st.expander(
        "👁️ Önizleme (ilk 200 satır)",
        expanded=True,
    ):

        st.dataframe(
            dataframe.head(200),
            use_container_width=True,
            hide_index=True,
        )

        st.caption(
            "Yukarıdaki tablo ilk 200 satırı gösterir. "
            "CSV dosyasında seçilen tüm satırlar bulunur."
        )

    st.divider()

    # ==========================================================
    # ADIM 3 — İNDİR
    # ==========================================================

    st.subheader(
        "3️⃣ İndir"
    )

    try:

        render_download_section(
            dataframe,
            start_time,
            end_time,
            share_params,
        )

    except Exception as exc:

        # Bu bölüm daha önce (belirli filtre kombinasyonlarında) hiçbir
        # hata göstermeden sessizce boş kalıyordu. Nedeni tam olarak
        # tekrar üretilemedi; bu yüzden burada olası bir istisna artık
        # yutulmuyor, doğrudan ekranda gösteriliyor ki bir daha "hiçbir
        # şey görünmüyor, hata da yok" durumuna düşülmesin.

        st.error(
            "İndirme seçenekleri hazırlanırken beklenmeyen bir hata oluştu."
        )

        st.exception(exc)


# ============================================================
# UÇUŞ ROTASI HARİTASI (Folium)
# ============================================================
#
# Seçilen bir ya da birden fazla uçuşun (flight_id) zaman sıralı
# lat/lon/altitude noktalarını folium/Leaflet ile haritada gösterir:
# her uçuş kendi kategorik renginde bir PolyLine ile çizilir, ortak
# (uçuştan bağımsız) yeşil/kırmızı CircleMarker'lar başlangıç/bitişi
# işaretler -- kimlik tooltip'te (flight_id) taşınır. Alan Bazlı
# Filtre haritasıyla aynı token gerektirmeyen "CartoDB dark_matter"
# zemini kullanılır.

# Sabit sırayla atanan, karanlık harita zemininde (carto-darkmatter)
# okunaklılık için doğrulanmış kategorik renk paleti -- her uçuşa,
# seçim sırasına göre DEĞİL, ClickHouse'daki tüm uçuş listesindeki
# sabit konumuna göre bir renk atanır (bkz. _flight_color) ki bir
# uçuşun seçimi kaldırılıp eklendiğinde diğer uçuşların rengi
# değişmesin ("color follows the entity, never its rank").
FLIGHT_COLOR_PALETTE = [
    "#3987e5",  # 1 mavi
    "#d95926",  # 2 turuncu
    "#199e70",  # 3 turkuaz
    "#c98500",  # 4 sarı
    "#d55181",  # 5 magenta
    "#008300",  # 6 yeşil
    "#9085e9",  # 7 mor
    "#e66767",  # 8 kırmızı
]

# İlk 3 renk, harita/scatter gibi "herhangi iki nokta yan yana
# olabilir" (all-pairs) bağlamında renk körlüğüne karşı tam
# doğrulanmıştır; 8'e kadar olan renkler de kullanılabilir ama
# birebir ayırt edilmeleri zorlaşabileceği için her uçuşun kimliği
# ayrıca rota tooltip'inde ve haritanın altındaki lejantta adıyla
# taşınır (renk asla tek başına kimlik taşımaz).
#
# MAX_COMPARABLE_FLIGHTS, kasıtlı olarak paletin boyutundan (8) BÜYÜK
# tutulabilir -- kategorik bir palette güvenle ayırt edilebilir yeni
# ton eklemenin pratik bir sınırı var (~8), bunun ötesi renk körlüğüne
# karşı doğrulanamaz hale gelir. Bu yüzden 9. ve sonraki uçuşlar,
# _flight_color'daki modulo ile paletin BAŞINDAN itibaren tekrar renk
# alır (örn. 9. uçuş 1. uçuşla aynı rengi paylaşır) -- kimlik karışıklığı,
# zaten her uçuşun rota tooltip'inde ve lejantta adıyla taşınmasıyla
# sınırlı tutulur.
MAX_COMPARABLE_FLIGHTS = 15

START_POINT_COLOR = "rgb(26, 152, 80)"   # başlangıç -- yeşil (durum rengi, uçuş kimliğinden bağımsız)
END_POINT_COLOR = "rgb(215, 48, 39)"     # bitiş -- kırmızı (durum rengi, uçuş kimliğinden bağımsız)


def _flight_color(flight_id: str, available_flights: list) -> str:
    """
    Bir uçuşa, tüm uçuş listesindeki (sabit, ORDER BY flight_id)
    konumuna göre kararlı bir kategorik renk atar.
    """

    index = available_flights.index(flight_id) % len(FLIGHT_COLOR_PALETTE)

    return FLIGHT_COLOR_PALETTE[index]


def _format_duration(delta: pd.Timedelta) -> str:
    """
    pd.Timedelta'yı "1s 04dk 12sn" gibi okunabilir bir Türkçe metne
    çevirir; saat 0 ise saat kısmı gösterilmez.
    """

    total_seconds = int(delta.total_seconds())

    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    if hours:
        return f"{hours}s {minutes:02d}dk {seconds:02d}sn"

    if minutes:
        return f"{minutes}dk {seconds:02d}sn"

    return f"{seconds}sn"


@st.cache_data(ttl=60)
def fetch_flight_route(flight_id: str) -> pd.DataFrame:
    """
    Seçilen uçuşun zaman sıralı rota noktalarını (time, latitude,
    longitude, altitude) döner. Sıralama fetch_filtered_telemetry
    içinde "ORDER BY time" ile yapılır -- PathLayer'ın rotayı uçuşun
    gerçek izlediği sırayla çizebilmesi için bu sıralama gereklidir.
    """

    return fetch_filtered_telemetry(
        columns=["time", "latitude", "longitude", "altitude"],
        selected_flights=[flight_id],
    )


def render_flight_map():

    st.subheader(
        "Uçuş Rotası"
    )

    st.caption(
        "Seçilen uçuş(lar)ın güzergahını, başlangıç/bitiş noktalarını "
        "haritada gösterir. Birden fazla uçuş seçerek rotalarını "
        "karşılaştırabilirsiniz -- her uçuş kendi rengiyle çizilir, "
        "hangi rengin hangi uçuşa ait olduğu haritanın altındaki "
        "lejantta ve rotaya tıklayınca çıkan tooltip'te görünür."
    )

    try:

        check_clickhouse_connection()

    except Exception as exc:

        st.error(
            f"ClickHouse'a bağlanılamadı: {exc}"
        )

        return

    try:

        available_flights = get_available_flights()

    except Exception as exc:

        st.error(
            f"Uçuş listesi okunamadı: {exc}"
        )

        return

    if not available_flights:

        st.info(
            "Tabloda 'flight_id' kolonu bulunamadı ya da hiç uçuş yok. "
            "Pipeline en az bir kez çalıştıktan sonra burada uçuşlar "
            "görünecektir."
        )

        return

    selected_flights = st.multiselect(
        "Uçuş ara / seç (birden fazla seçilebilir)",
        options=available_flights,
        default=[],
        key="flight_map_selected_flights",
        max_selections=MAX_COMPARABLE_FLIGHTS,
        help=(
            "Yazarak arayabilir, listeden bir ya da birden fazla uçuş "
            f"(flight_id) seçebilirsiniz (aynı anda en fazla "
            f"{MAX_COMPARABLE_FLIGHTS} uçuş). İlk {len(FLIGHT_COLOR_PALETTE)} "
            "uçuş birbirinden farklı renk alır; sonrasında renkler baştan "
            "tekrar eder -- hangi rotanın hangi uçuşa ait olduğunu rota "
            "tooltip'inden ve haritanın altındaki lejanttan da "
            "görebilirsiniz."
        ),
    )

    if not selected_flights:

        st.info(
            "Rotasını görmek istediğiniz uçuş(lar)ı yukarıdan seçin."
        )

        return

    # Her uçuşun rota verisi ayrı ayrı (flight_id başına, cache'lenmiş
    # fetch_flight_route ile) çekilir -- tek bir sorguda tüm uçuşları
    # birlikte çekmek, aralarındaki "ORDER BY time" sıralamasını uçuş
    # bazında ayıramayacağı için PathLayer'ın her rotayı kendi zaman
    # sırasıyla çizmesini zorlaştırırdı.

    flight_routes = []
    flights_without_location = []

    for flight_id in selected_flights:

        try:

            route_df = fetch_flight_route(
                flight_id
            )

        except Exception as exc:

            st.error(
                f"'{flight_id}' için rota verisi okunamadı: {exc}"
            )

            return

        required_route_columns = {
            "latitude",
            "longitude",
            "altitude",
        }

        if not required_route_columns.issubset(route_df.columns):
            flights_without_location.append(flight_id)
            continue

        route_df = route_df.dropna(
            subset=list(required_route_columns)
        )

        if route_df.empty:
            flights_without_location.append(flight_id)
            continue

        flight_routes.append(
            (
                flight_id,
                route_df,
                _flight_color(flight_id, available_flights),
            )
        )

    if flights_without_location:

        st.warning(
            "Konum verisi bulunamadığı için şu uçuşlar haritaya "
            "eklenemedi: " + ", ".join(flights_without_location)
        )

    if not flight_routes:
        return

    # ---- Uçuş başına özet tablo (nokta sayısı, irtifa, süre) ----
    #
    # Önceki tek-uçuş görünümündeki 4 metrik kutusu, birden fazla uçuş
    # aynı anda gösterilebildiği için bir tabloya dönüştürüldü.

    summary_rows = []

    for flight_id, route_df, color in flight_routes:

        min_altitude = float(route_df["altitude"].min())
        max_altitude = float(route_df["altitude"].max())

        # Uçuş süresi: irtifanın 0 olduğu (yerde/kalkış-iniş anı) zaman
        # noktalarının en erken ve en geç olanı arasındaki fark alınarak
        # hesaplanır. İrtifası hiç 0 olmayan uçuşlarda (örn. yalnızca
        # havada kayıt yapılmış veri) bu hesap yapılamaz.

        zero_altitude_times = route_df.loc[
            route_df["altitude"] == 0,
            "time",
        ]

        if len(zero_altitude_times) >= 2:
            flight_duration = _format_duration(
                zero_altitude_times.max() - zero_altitude_times.min()
            )
        else:
            flight_duration = "—"

        summary_rows.append(
            {
                "Uçuş": flight_id,
                "Nokta sayısı": len(route_df),
                "Min irtifa": round(min_altitude, 1),
                "Maks irtifa": round(max_altitude, 1),
                "Uçuş süresi": flight_duration,
            }
        )

    st.dataframe(
        pd.DataFrame(summary_rows),
        use_container_width=True,
        hide_index=True,
    )

    # Harita, seçilen tüm uçuşların rotalarını kapsayacak şekilde
    # fit_bounds ile otomatik ortalanıp yakınlaştırılır.

    all_lat = pd.concat(
        [route_df["latitude"] for _, route_df, _ in flight_routes]
    )
    all_lon = pd.concat(
        [route_df["longitude"] for _, route_df, _ in flight_routes]
    )

    route_map = folium.Map(
        location=[all_lat.mean(), all_lon.mean()],
        tiles="CartoDB dark_matter",
    )

    for flight_id, route_df, color in flight_routes:

        start_row = route_df.iloc[0]
        end_row = route_df.iloc[-1]

        folium.PolyLine(
            locations=list(
                zip(route_df["latitude"], route_df["longitude"])
            ),
            color=color,
            weight=3,
            opacity=0.85,
            tooltip=flight_id,
        ).add_to(route_map)

        # Başlangıç/bitiş işaretçileri kasıtlı olarak uçuş renginden
        # BAĞIMSIZ, sabit yeşil/kırmızı (bir "durum" rengi) kullanır;
        # kimlik tooltip'teki flight_id ile taşınır.
        folium.CircleMarker(
            location=[start_row["latitude"], start_row["longitude"]],
            radius=7,
            color="white",
            weight=1,
            fill=True,
            fill_color=START_POINT_COLOR,
            fill_opacity=1.0,
            tooltip=f"{flight_id} — Başlangıç (irtifa: {start_row['altitude']:.1f})",
        ).add_to(route_map)

        folium.CircleMarker(
            location=[end_row["latitude"], end_row["longitude"]],
            radius=7,
            color="white",
            weight=1,
            fill=True,
            fill_color=END_POINT_COLOR,
            fill_opacity=1.0,
            tooltip=f"{flight_id} — Bitiş (irtifa: {end_row['altitude']:.1f})",
        ).add_to(route_map)

    route_map.fit_bounds(
        [
            [all_lat.min(), all_lon.min()],
            [all_lat.max(), all_lon.max()],
        ]
    )

    # streamlit-folium'un iframe yüksekliğini kararsız ölçmesini
    # yalnızca uçuş rotası haritası için geçersiz kıl.
    st.markdown(
        """
        <style>
        .st-key-flight_route_map iframe[title="streamlit_folium.st_folium"] {
            height: 600px !important;
            min-height: 600px;
            max-height: 600px !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    legend_items = " &nbsp;&nbsp; ".join(
        f'<span style="color:{color};">●</span> {flight_id}'
        for flight_id, _, color in flight_routes
    )

    st.markdown(
        "🟢 Başlangıç &nbsp;&nbsp; 🔴 Bitiş &nbsp;&nbsp; " + legend_items,
        unsafe_allow_html=True,
    )

    with st.container(key="flight_route_map"):

        st_folium(
            route_map,
            height=600,
            use_container_width=True,
            returned_objects=[],
        )


# ============================================================
# DVC / VERİ SÜRÜMLERİ (MinIO artefaktları)
# ============================================================
#
# Pipeline (dagster/assets.py) her doğrulanmış batch için Great
# Expectations kalite raporunu MinIO'daki "pipeline-artifacts"
# bucket'ına JSON ve tam Data Docs bundle'ı olarak yazar:
#
#     validation/<dataset_id>/<batch_id>/<source_etag[:12]>.json
#     validation/<dataset_id>/<batch_id>/<source_etag[:12]>/...
#
# batch_id (ör. "1000_10000_flight_1_2019-08-29") aynı zamanda DVC'ye
# publish edilen veri sürümünün kimliğidir. Bu sekme JSON dosyalarını
# listeler; seçilen JSON'daki Data Docs anahtarını izleyip tam GX raporunu
# MinIO üzerinden yeni sekmede gösterir. Ayrıca DVC uzak deposundaki
# (dvc-cache) dizin manifestolarını (.dir) okuyup her sürümün hangi
# dosyalardan oluştuğunu md5'leriyle birlikte gösterir.


_VALIDATION_KEY_RE = re.compile(
    r"^validation/(?P<dataset>[^/]+)/(?P<batch>[^/]+)/(?P<etag>[^/]+)\.json$"
)


def _human_size(num_bytes) -> str:
    try:
        value = float(num_bytes)
    except (TypeError, ValueError):
        return "-"

    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{value:,.0f} {unit}" if unit == "B" else f"{value:,.1f} {unit}"
        value /= 1024

    return f"{value:,.1f} TiB"


@st.cache_data(ttl=30)
def list_artifact_objects() -> pd.DataFrame:
    """
    pipeline-artifacts bucket'ında SADECE validation/ öneki altındaki
    kalite raporu JSON'larını listeler. validation/<dataset>/<batch>/
    <etag>.json şemasına uymayan hiçbir nesne (ör. _clickhouse-ingest/...
    altındaki geçici zstd parçaları) DAHİL EDİLMEZ.

    Bucket yoksa boş DataFrame döner (hata fırlatmaz); bağlantı hatası
    çağırana yükselir.
    """

    client = get_minio_client()
    bucket = get_pipeline_artifacts_bucket()
    prefix = get_validation_prefix()

    if not client.bucket_exists(bucket):
        return pd.DataFrame()

    rows = []

    for obj in client.list_objects(bucket, prefix=prefix, recursive=True):

        if obj.is_dir:
            continue

        key = obj.object_name
        match = _VALIDATION_KEY_RE.match(key)

        # Şemaya uymayan / JSON olmayan nesneleri ele (güvenlik ağı --
        # prefix zaten _clickhouse-ingest'i dışarıda bırakıyor).
        if match is None or not key.endswith(".json"):
            continue

        rows.append(
            {
                "nesne": key,
                "dataset": match.group("dataset"),
                "batch": match.group("batch"),
                "etag_kisa": match.group("etag"),
                "boyut": _human_size(obj.size),
                "boyut_bytes": obj.size,
                "degisiklik": obj.last_modified,
                "etag": (obj.etag or "").strip('"'),
            }
        )

    df = pd.DataFrame(rows)

    if not df.empty:
        df = df.sort_values("degisiklik", ascending=False, ignore_index=True)

    return df


@st.cache_data(ttl=30)
def fetch_artifact_bytes(key: str, etag: str) -> bytes:
    """
    Tek bir artefakt nesnesinin ham içeriğini indirir. `etag` yalnızca
    cache anahtarını nesne değişince tazelemek için parametre edilir.
    """

    client = get_minio_client()
    bucket = get_pipeline_artifacts_bucket()

    response = client.get_object(bucket, key)

    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def render_ge_data_docs_viewer(object_key: str, cache_buster: str) -> None:
    """Fetch a private Data Docs page and its bundle assets from MinIO."""

    st.markdown(
        '<a href="?panel=admin" target="_self">← Dashboarda dön</a>',
        unsafe_allow_html=True,
    )
    st.subheader("Great Expectations — Data Docs")

    try:
        html_key, bundle_prefix = validate_data_docs_html_key(
            object_key,
            validation_prefix=get_validation_prefix(),
        )
        html = fetch_artifact_bytes(html_key, cache_buster)
        rendered_html = inline_data_docs_bundle(
            html,
            html_key=html_key,
            bundle_prefix=bundle_prefix,
            cache_buster=cache_buster,
            fetch_object=lambda key: fetch_artifact_bytes(key, cache_buster),
        )
    except DataDocsReferenceError as exc:
        st.error(f"Geçersiz Data Docs bağlantısı: {exc}")
        return
    except Exception as exc:
        st.error(f"Data Docs raporu MinIO'dan okunamadı: {exc}")
        return

    components.html(rendered_html, height=1200, scrolling=True)


@st.cache_data(ttl=60)
def fetch_dvc_remote_manifests() -> dict:
    """
    DVC uzak deposundaki (dvc-cache) .dir manifestolarını okur. Her
    manifest, DVC ile takip edilen bir dizinin içeriğini
    [{"md5": ..., "relpath": ...}, ...] biçiminde JSON olarak tutar.

    Döner: {"object_count", "total_bytes", "manifests": [...]}
    """

    client = get_minio_client()
    bucket = get_dvc_remote_bucket()

    if not client.bucket_exists(bucket):
        return {"object_count": 0, "total_bytes": 0, "manifests": []}

    object_count = 0
    total_bytes = 0
    manifest_keys = []

    for obj in client.list_objects(bucket, recursive=True):

        if obj.is_dir:
            continue

        object_count += 1
        total_bytes += obj.size or 0

        if obj.object_name.endswith(".dir"):
            manifest_keys.append(obj.object_name)

    manifests = []

    for key in sorted(manifest_keys):

        response = client.get_object(bucket, key)

        try:
            payload = json.loads(response.read())
        except Exception:
            payload = []
        finally:
            response.close()
            response.release_conn()

        # DVC md5 dizin hash'i: <2 harf>/<geri kalan>.dir
        parts = key.split("/")
        dir_md5 = "".join(parts[-2:]).replace(".dir", "") if len(parts) >= 2 else key

        manifests.append(
            {
                "manifest_key": key,
                "dir_md5": dir_md5,
                "nfiles": len(payload) if isinstance(payload, list) else 0,
                "files": payload if isinstance(payload, list) else [],
            }
        )

    return {
        "object_count": object_count,
        "total_bytes": total_bytes,
        "manifests": manifests,
    }


def _looks_like_ge_report(data) -> bool:
    return isinstance(data, dict) and "statistics" in data and "result" in data


def _render_ge_report(data: dict) -> None:

    success = bool(data.get("success"))

    if success:
        st.success("✅ Doğrulama başarılı — tüm beklentiler karşılandı.")
    else:
        st.error("❌ Doğrulama başarısız — en az bir beklenti karşılanmadı.")

    stats = data.get("statistics", {}) or {}

    cols = st.columns(4)
    cols[0].metric(
        "Değerlendirilen beklenti",
        f"{stats.get('evaluated_expectations', 0):,}",
    )
    cols[1].metric(
        "Başarılı",
        f"{stats.get('successful_expectations', 0):,}",
    )
    cols[2].metric(
        "Başarısız",
        f"{stats.get('unsuccessful_expectations', 0):,}",
    )
    cols[3].metric(
        "Başarı %",
        f"{stats.get('success_percent', 0.0):.1f}",
    )

    meta_bits = []

    if data.get("rules_profile"):
        meta_bits.append(f"**Kural profili:** `{data['rules_profile']}`")

    validated_columns = data.get("validated_columns")
    if isinstance(validated_columns, list):
        meta_bits.append(f"**Doğrulanan kolon sayısı:** {len(validated_columns):,}")

    if meta_bits:
        st.markdown(" &nbsp;&nbsp;|&nbsp;&nbsp; ".join(meta_bits))

    results = (data.get("result") or {}).get("results") or []

    if results:

        expectation_rows = []

        for item in results:

            config = item.get("expectation_config", {}) or {}
            kwargs = config.get("kwargs", {}) or {}

            column = kwargs.get("column")
            if not column and isinstance(kwargs.get("column_list"), list):
                column = f"<{len(kwargs['column_list']):,} kolon>"

            expectation_rows.append(
                {
                    "beklenti": config.get("type")
                    or config.get("expectation_type")
                    or "-",
                    "kolon": column or "-",
                    "sonuç": "✅" if item.get("success") else "❌",
                }
            )

        st.dataframe(
            pd.DataFrame(expectation_rows),
            use_container_width=True,
            hide_index=True,
        )


def render_dvc_artifacts() -> None:

    st.subheader("DVC / Veri Sürümleri")

    st.caption(
        "Pipeline'ın her doğrulanmış batch için MinIO'ya yazdığı kalite "
        "raporu JSON'larını ve tam GE Data Docs bundle'larını "
        "(`pipeline-artifacts/validation/<dataset>/<batch>/<etag>...`) ile "
        "DVC uzak deposundaki (`dvc-cache`) dizin "
        "manifestolarını gösterir. Her `batch` kimliği, DVC'ye publish "
        "edilen bir veri sürümüne karşılık gelir."
    )

    if Minio is None:
        st.error(
            "`minio` paketi kurulu değil, MinIO'ya bağlanılamıyor: "
            f"{_MINIO_IMPORT_ERROR}\n\n"
            "`dashboard/requirements.txt` içine `minio` ekleyip dashboard "
            "imajını yeniden kurun."
        )
        return

    st.caption(
        f"MinIO: `{get_minio_endpoint()}` &nbsp;·&nbsp; "
        f"önek: `{get_pipeline_artifacts_bucket()}/{get_validation_prefix()}` "
        f"&nbsp;·&nbsp; DVC remote bucket: `{get_dvc_remote_bucket()}` "
        "&nbsp;·&nbsp; yeni artefaktlar için sol paneldeki **🔄 Şimdi "
        "yenile** düğmesini kullanın."
    )

    try:
        artifacts_df = list_artifact_objects()
    except Exception as exc:
        st.error(
            f"MinIO'ya bağlanılamadı ({get_minio_endpoint()}): {exc}"
        )
        return

    if artifacts_df.empty:
        st.info(
            f"`{get_pipeline_artifacts_bucket()}` bucket'ında henüz artefakt "
            "yok. Pipeline en az bir batch'i doğrulayıp publish ettikten "
            "sonra kalite raporları burada görünür."
        )
    else:

        cols = st.columns(4)
        cols[0].metric("Artefakt dosyası", f"{len(artifacts_df):,}")
        cols[1].metric(
            "Toplam boyut",
            _human_size(artifacts_df["boyut_bytes"].sum()),
        )
        cols[2].metric(
            "Dataset",
            f"{artifacts_df['dataset'].dropna().nunique():,}",
        )
        cols[3].metric(
            "Veri sürümü (batch)",
            f"{artifacts_df['batch'].dropna().nunique():,}",
        )

        st.dataframe(
            artifacts_df[
                [
                    "nesne",
                    "dataset",
                    "batch",
                    "boyut",
                    "degisiklik",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

        st.divider()
        st.markdown("### Rapor Detayı")

        object_keys = artifacts_df["nesne"].tolist()

        def _label_for(key: str) -> str:
            row = artifacts_df.loc[artifacts_df["nesne"] == key].iloc[0]
            if row["batch"]:
                return f"{row['dataset']} / {row['batch']}  ({row['etag_kisa']})"
            return key

        selected_key = st.selectbox(
            "Görüntülenecek artefakt",
            options=object_keys,
            format_func=_label_for,
            key="dvc_selected_artifact",
        )

        if selected_key:

            selected_row = artifacts_df.loc[
                artifacts_df["nesne"] == selected_key
            ].iloc[0]

            try:
                raw_bytes = fetch_artifact_bytes(
                    selected_key,
                    str(selected_row["etag"]),
                )
            except Exception as exc:
                st.error(f"Artefakt indirilemedi: {exc}")
            else:

                try:
                    data = json.loads(raw_bytes)
                except Exception as exc:
                    st.error(f"JSON çözümlenemedi: {exc}")
                    st.code(
                        raw_bytes[:5000].decode("utf-8", errors="replace")
                    )
                    data = None

                if data is not None:

                    if _looks_like_ge_report(data):
                        _render_ge_report(data)

                    data_docs = data.get("data_docs") if isinstance(data, dict) else None
                    if isinstance(data_docs, dict) and data_docs.get(
                        "validation_key"
                    ):
                        data_docs_query = urlencode(
                            {
                                "ge_data_docs": data_docs["validation_key"],
                                "ge_docs_version": str(selected_row["etag"]),
                            }
                        )
                        st.link_button(
                            "📊 GE Data Docs raporunu yeni sekmede aç",
                            f"?{data_docs_query}",
                            type="primary",
                            use_container_width=True,
                        )

                    st.download_button(
                        "📥 JSON indir",
                        data=raw_bytes,
                        file_name=selected_key.split("/")[-1],
                        mime="application/json",
                        key="dvc_download_artifact",
                    )

                    # validated_columns 10.000+ öğe içerebildiği için ham
                    # görünümde bu alan özetlenir.
                    preview = data
                    if (
                        isinstance(data, dict)
                        and isinstance(data.get("validated_columns"), list)
                        and len(data["validated_columns"]) > 50
                    ):
                        preview = dict(data)
                        preview["validated_columns"] = (
                            f"<{len(data['validated_columns'])} kolon — "
                            "ham dosyada listelenmiştir>"
                        )

                    with st.expander("Ham JSON"):
                        st.json(preview, expanded=False)

    st.divider()
    st.markdown("### DVC Uzak Deposu (`dvc-cache`)")

    try:
        remote = fetch_dvc_remote_manifests()
    except Exception as exc:
        st.warning(f"DVC uzak deposu okunamadı: {exc}")
        return

    cols = st.columns(3)
    cols[0].metric("Nesne sayısı", f"{remote['object_count']:,}")
    cols[1].metric("Toplam boyut", _human_size(remote["total_bytes"]))
    cols[2].metric("Dizin manifestosu", f"{len(remote['manifests']):,}")

    if not remote["manifests"]:
        st.caption(
            "Henüz `.dir` manifestosu yok — DVC'ye bir dizin publish "
            "edildiğinde burada listelenir."
        )
        return

    for manifest in remote["manifests"]:

        with st.expander(
            f"`{manifest['dir_md5']}` — {manifest['nfiles']} dosya"
        ):

            file_rows = [
                {
                    "relpath": entry.get("relpath"),
                    "md5": entry.get("md5"),
                }
                for entry in manifest["files"]
            ]

            st.dataframe(
                pd.DataFrame(file_rows),
                use_container_width=True,
                hide_index=True,
            )

            st.caption(f"Manifest anahtarı: `{manifest['manifest_key']}`")


# ============================================================
# ANA UYGULAMA
# ============================================================

def main():

    data_docs_key = st.query_params.get("ge_data_docs")
    if data_docs_key:
        render_ge_data_docs_viewer(
            data_docs_key,
            st.query_params.get("ge_docs_version", "uncached"),
        )
        return

    # --------------------------------------------------------
    # PANEL DURUMU (başlıktan ÖNCE çözülür)
    # --------------------------------------------------------
    #
    # Seçili panel URL'deki "?panel=user|admin" ile taşınır. Başlığa
    # tıklamak sayfayı yeniden yükleyip session_state'i sıfırladığı için,
    # ilk render'da (ve her reload'da) panel bu query param'dan okunur;
    # yoksa varsayılan Kullanıcı Paneli'dir. Kullanıcı radio'yla panel
    # değiştirdiğinde query param da güncellenir (bkz. nav_col).
    if "panel_selector" not in st.session_state:
        st.session_state["panel_selector"] = PANEL_BY_CODE.get(
            st.query_params.get("panel", ""),
            PANEL_USER,
        )

    current_panel_code = PANEL_CODE_BY_LABEL[
        st.session_state["panel_selector"]
    ]

    # Başlığa tıklanınca bulunulan panelin ana sekmesine dönülür ve
    # PANEL KORUNUR (?panel=... eklenir) -- bkz. "?goto=runs" kontrolü.
    st.markdown(
        f'<a href="?goto=runs&panel={current_panel_code}" target="_self" '
        'style="text-decoration: none; color: inherit;">'
        '<h1 style="margin: 0;">'
        "İHA Veri Platformu — Pipeline Metrikleri & Katalog"
        "</h1></a>",
        unsafe_allow_html=True,
    )

    st.caption(
        "Kaynak: Dagster GraphQL API + ClickHouse. "
        "Katalog verisi Dagster asset materialization metadata'sından, "
        "telemetri verisi ise ClickHouse üzerinden okunmaktadır."
    )

    # (Kaydırma konumunu koruyan script, sekme seçimi belirlendikten
    # SONRA aşağıda enjekte edilir -- bkz. "_scroll_reset_pending".)

    # ========================================================
    # PANEL + SEKME SEÇİMİ + KONTROLLER (yan panel)
    # ========================================================
    #
    # Yan panelin en üstünde parolasız bir "Panel" seçici (iki buton)
    # vardır:
    #   - Kullanıcı Paneli -> yalnızca USER_TAB_LABELS ("Veri Gözat /
    #     Dışa Aktar", "Uçuş Rotası"); "Gösterilecek run sayısı" YOK.
    #   - Admin Paneli     -> MAIN_TAB_LABELS (tüm sekmeler) +
    #     "Gösterilecek run sayısı" slider'ı.
    #
    # Hem panel seçici hem de sekme seçimi için st.radio/st.tabs yerine
    # dikey sıralanmış butonlar kullanılır -- her buton aktifse "primary"
    # (dolu/renkli), değilse "secondary" (outline) tipiyle çizilir.
    # st.button, Streamlit'in iç DOM yapısı sürüm sürüm değişse de
    # (bkz. dashboard/requirements.txt içindeki pinlenmiş Streamlit
    # sürümü) kararlı kalan, sade bir bileşen olduğu için CSS hack'i
    # gerekmeden güvenilir biçimde "aktif/pasif" görünümü verir.
    #
    # URL'de tab=export ya da herhangi bir export_* parametresi
    # varsa (paylaşılan bir bağlantı üzerinden açılmışsa), İLK
    # render'da otomatik olarak Kullanıcı panelindeki "Veri Gözat /
    # Dışa Aktar" sekmesi seçilir. session_state["active_main_tab"]
    # zaten set edilmişse (kullanıcı elle başka bir sekmeye geçtiyse)
    # bu değer UYGULANMAZ -- yoksa kullanıcı sekme değiştirdikten sonra
    # her rerun'da URL'deki sekmeye geri dönerdi.
    #
    # "Gösterilecek run sayısı" ve "🔄 Şimdi yenile" de aynı panelde --
    # run_limit, content_col'daki sekme içerikleri render edilmeden
    # ÖNCE burada okunmalı (runs_df'i etkiler).

    # Başlığa tıklanınca eklenen "?goto=runs" -- başlık, bulunulan
    # panelin "ana sayfası"na dönen bir bağlantı gibi davranır ve PANELİ
    # DEĞİŞTİRMEZ (panel yukarıda "?panel=..." query param'ından zaten
    # çözüldü):
    #   - Admin panelinde   -> "Pipeline Metrikleri" sekmesi
    #   - Kullanıcı panelinde -> ilk kullanıcı sekmesi ("Veri Gözat /
    #     Dışa Aktar"); "Pipeline Metrikleri" bu panelde yoktur.
    if st.query_params.get("goto") == "runs":

        if st.session_state["panel_selector"] == PANEL_ADMIN:
            st.session_state["active_main_tab"] = MAIN_TAB_RUNS
        else:
            st.session_state["active_main_tab"] = USER_TAB_LABELS[0]

        st.session_state["_scroll_reset_pending"] = True

        del st.query_params["goto"]

    if "active_main_tab" not in st.session_state:

        shared_query_params = st.query_params.to_dict()

        if shared_query_params.get("tab") == "export" or any(
            key.startswith("export_")
            for key in shared_query_params
        ):
            # Paylaşılan bağlantı doğrudan Kullanıcı panelindeki "Veri
            # Gözat / Dışa Aktar" sekmesini açar.
            st.session_state["panel_selector"] = PANEL_USER
            st.session_state["active_main_tab"] = MAIN_TAB_EXPORT
        else:
            st.session_state["active_main_tab"] = _tabs_for_panel(
                st.session_state["panel_selector"]
            )[0]

    nav_col, content_col = st.columns(
        [1, 5],
        gap="large",
    )

    with nav_col:

        # --------------------------------------------------
        # PANEL SEÇİCİ (parolasız)
        # --------------------------------------------------
        #
        # Önceden küçük bir st.radio idi; göze çarpmıyor ve dashboard'a
        # oranla ufak kalıyordu. Artık alttaki sekme navigasyonuyla aynı
        # dilde, tam genişlikte iki buton olarak çizilir -- aktif panel
        # "primary" (dolu/renkli), diğeri "secondary" (outline).
        #
        # Seçili panel session_state["panel_selector"]'de tutulur
        # (başlangıç değeri main() başında "?panel=..." query param'ından
        # set edilir). Panel YALNIZCA butona basılınca değişir (her
        # render'da DEĞİL); o an _sync_panel_to_url() ile URL'deki
        # "panel" param'ı güncellenir, böylece başlığa tıklamak (sayfayı
        # yeniden yükler) ya da F5 panelde kalır. Aynı render içinde URL
        # yazan başka bir özellikle (ör. "🔗 Bağlantı Olarak Paylaş")
        # çakışmaması için sync yalnızca bu tıklama dalında yapılır.
        st.markdown("#### 🗂️ Panel")

        for panel_label in PANEL_LABELS:

            is_active_panel = (
                st.session_state["panel_selector"] == panel_label
            )

            if st.button(
                panel_label,
                key=f"panel_selector_btn_{panel_label}",
                type="primary" if is_active_panel else "secondary",
                use_container_width=True,
            ) and not is_active_panel:
                st.session_state["panel_selector"] = panel_label
                _sync_panel_to_url()
                # Yeni panelde görünmeyen aktif sekme aşağıda ilk görünür
                # sekmeye sabitlenir; kaydırmayı da en üste al.
                st.session_state["_scroll_reset_pending"] = True
                st.rerun()

        active_panel = st.session_state["panel_selector"]

        st.caption(
            "Kullanıcı Paneli: sade görünüm · Admin Paneli: tüm sekmeler"
        )

        is_admin_panel = active_panel == PANEL_ADMIN

        visible_tabs = _tabs_for_panel(active_panel)

        # Aktif sekme, seçili panelde görünmüyorsa (ör. panel yeni
        # değişti) ilk görünür sekmeye sabitle.
        if st.session_state["active_main_tab"] not in visible_tabs:
            st.session_state["active_main_tab"] = visible_tabs[0]
            st.session_state["_scroll_reset_pending"] = True

        st.divider()

        for tab_label in visible_tabs:

            is_active_tab = (
                st.session_state["active_main_tab"] == tab_label
            )

            if st.button(
                tab_label,
                key=f"main_nav_btn_{tab_label}",
                type="primary" if is_active_tab else "secondary",
                use_container_width=True,
            ):
                st.session_state["active_main_tab"] = tab_label
                # Sekme değişiminde kaydırma konumu KORUNMAZ -- yeni
                # sekmenin en üstünden başlanır (bkz. aşağıdaki kaydırma
                # script'i / "_scroll_reset_pending").
                st.session_state["_scroll_reset_pending"] = True
                st.rerun()

        st.divider()

        # "Gösterilecek run sayısı" yalnızca Admin panelinde gösterilir;
        # run verisi de (fetch_runs) yalnızca orada çekilir. Kullanıcı
        # panelinde makul bir varsayılan kullanılır.
        if is_admin_panel:
            run_limit = st.slider(
                "Gösterilecek run sayısı",
                10,
                200,
                50,
                step=10,
            )
        else:
            run_limit = 50

        if st.button(
            "🔄 Şimdi yenile",
            use_container_width=True,
        ):

            fetch_runs.clear()
            fetch_asset_catalog.clear()
            get_metadata_history_filters.clear()
            fetch_metadata_history.clear()
            load_alerts.clear()
            get_available_columns.clear()
            get_numeric_columns.clear()
            get_clickhouse_schema.clear()
            get_available_classes.clear()
            get_available_flights.clear()
            get_time_range.clear()
            get_lat_lon_bounds.clear()
            fetch_flight_route.clear()
            check_clickhouse_connection.clear()
            # "DVC / Veri Sürümleri" sekmesinin MinIO okumaları
            list_artifact_objects.clear()
            fetch_artifact_bytes.clear()
            fetch_dvc_remote_manifests.clear()

            st.rerun()

        st.caption(
            "Son sorgu: "
            f"{datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"
        )

    active_tab = st.session_state["active_main_tab"]

    # ========================================================
    # KAYDIRMA KONUMUNU KORU
    # ========================================================
    #
    # Streamlit her widget etkileşiminde tüm script'i yeniden çalıştırıp
    # tarayıcıyı sayfanın en üstüne kaydırıyor -- çok filtreli bu ekranda
    # bir multiselect'e dokununca "sayfa sıçradı" hissi veriyordu. Bu
    # script kaydırma konumunu sessionStorage'da tutup her rerun'dan
    # sonra geri yükler.
    #
    # Önceki sürüm tek bir requestAnimationFrame ile geri yüklüyordu;
    # bu, tablo/harita gibi dinamik içerik daha DOM'a yerleşmeden
    # çalıştığı için konum kısa kalıyor, sonra içerik büyüyünce sayfa
    # yine kayıyordu. Ayrıca sekme değiştirince bile eski konuma geri
    # dönüyordu. Bu sürüm:
    #   - içerik yerleşene kadar kısa bir pencere boyunca (~1.2 sn)
    #     tekrar tekrar dener, hedefe ulaşınca / sayfa daha fazla
    #     kaydırılamayınca durur,
    #   - geri yükleme sürerken kullanıcı elle kaydırırsa bırakır
    #     (programatik set ile arasında >40px sapma),
    #   - geri yükleme sürerken ara (kırpılmış) değeri KAYDETMEZ,
    #   - sekme değişiminde (_scroll_reset_pending) konumu koruMAZ,
    #     yeni sekmenin en üstüne gider.
    scroll_reset_pending = st.session_state.pop(
        "_scroll_reset_pending", False
    )

    components.html(
        f"""
        <script>
        (function () {{
          try {{
            var KEY = "dpm_scroll_y";
            var RESET = {"true" if scroll_reset_pending else "false"};
            var w = window.parent;
            var doc = w.document;
            var scroller = doc.scrollingElement || doc.documentElement;

            if (!w.__dpmScrollHooked) {{
              w.__dpmScrollHooked = true;
              w.addEventListener("scroll", function () {{
                if (w.__dpmRestoring) {{
                  // Geri yükleme sürüyor: bizim programatik set'imizden
                  // belirgin şekilde saparsa kullanıcı kaydırmıştır ->
                  // geri yüklemeyi bırak, yeni konumu kaydet.
                  if (Math.abs(scroller.scrollTop - (w.__dpmLastSet || 0)) > 40) {{
                    w.__dpmRestoring = false;
                  }} else {{
                    return;
                  }}
                }}
                try {{
                  w.sessionStorage.setItem(KEY, String(scroller.scrollTop));
                }} catch (e) {{}}
              }}, {{ passive: true }});
            }}

            if (RESET) {{
              w.__dpmRestoring = false;
              try {{ w.sessionStorage.removeItem(KEY); }} catch (e) {{}}
              w.requestAnimationFrame(function () {{ scroller.scrollTop = 0; }});
              return;
            }}

            var saved = w.sessionStorage.getItem(KEY);
            if (saved === null) {{ return; }}

            var target = parseFloat(saved);
            var attempts = 0;
            w.__dpmRestoring = true;

            var restore = function () {{
              if (!w.__dpmRestoring) {{ return; }}
              attempts += 1;
              var maxTop = scroller.scrollHeight - scroller.clientHeight;
              var clamped = Math.max(0, Math.min(target, maxTop));
              w.__dpmLastSet = clamped;
              scroller.scrollTop = clamped;
              var reached = Math.abs(scroller.scrollTop - target) < 2;
              var pageMaxedOut = maxTop <= target + 2;
              if (reached || pageMaxedOut || attempts >= 20) {{
                w.__dpmRestoring = false;
              }} else {{
                w.setTimeout(restore, 60);
              }}
            }};

            w.requestAnimationFrame(restore);
          }} catch (e) {{ /* farklı origin / storage kapalı -- sessizce geç */ }}
        }})();
        </script>
        """,
        height=0,
    )

    # ========================================================
    # RUN VERİSİNİ BİR KEZ ÇEK
    # ========================================================
    #
    # Run verisi ve alert bildirimleri yalnızca Admin panelinde
    # kullanılır ("Pipeline Metrikleri" / "Alertler" sekmeleri) --
    # Kullanıcı panelinde Dagster'a hiç gidilmez.

    if is_admin_panel:

        try:

            runs_df = fetch_runs(
                run_limit
            )

        except Exception as exc:

            runs_df = pd.DataFrame()

            st.error(
                f"Dagster'a bağlanılamadı: {exc}"
            )

        # ====================================================
        # YENİ ALERT BİLDİRİMİ (TOAST)
        # ====================================================
        #
        # Admin panelinde hangi sekme açık olursa olsun, yeni bir hata
        # oluştuğunda (veya bir hata çözüldüğünde) ekranın köşesinde
        # küçük bir bildirim gösterilir.

        notify_new_alerts(
            load_alerts()
        )

    else:

        runs_df = pd.DataFrame()

    with content_col:

        # ========================================================
        # PIPELINE METRİKLERİ
        # ========================================================

        if active_tab == MAIN_TAB_RUNS:

            if not runs_df.empty:

                render_run_kpis(
                    runs_df
                )

                st.divider()

                st.subheader(
                    "Durum Dağılımı"
                )

                render_status_chart(
                    runs_df
                )

                st.divider()

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

        if active_tab == MAIN_TAB_CATALOG:

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

            st.divider()

            render_metadata_history()

        # ========================================================
        # DVC / VERİ SÜRÜMLERİ
        # ========================================================

        if active_tab == MAIN_TAB_DVC:

            render_dvc_artifacts()

        # ========================================================
        # ALERTS
        # ========================================================

        if active_tab == MAIN_TAB_ALERTS:

            render_alerts(
                runs_df
            )

        # ========================================================
        # VERİ GÖZAT / EXPORT
        # ========================================================

        if active_tab == MAIN_TAB_EXPORT:

            render_data_export()

        # ========================================================
        # UÇUŞ ROTASI HARİTASI
        # ========================================================

        if active_tab == MAIN_TAB_FLIGHT_MAP:

            render_flight_map()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
