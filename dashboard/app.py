"""
A3 - İHA Veri Platformu
Pipeline Metrikleri + Asset Kataloğu + Telemetri Gözat/Dışa Aktar

Backend:
    ClickHouse (file() tablo fonksiyonuyla, dagster/data/processed
    klasöründeki parquet dosyalarını sorgu zamanında okur — veri
    ayrıca ClickHouse'un kendi depolama motoruna INSERT edilmez)

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
       - Metadata Geçmişi (Postgres, asset_metadata_history):
         asset / uçuş / tarih bazlı filtrelenebilir materialization
         geçmişi — bkz. docs/postgres_asset_metadata_schema.sql

    3. Alertler
       - Başarısız Dagster run'ları
       - failure_hook tarafından oluşturulan alert kayıtları

    4. Veri Gözat / Dışa Aktar
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
       - CSV dışa aktarma
"""

import io
import json
import os
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode

import clickhouse_connect
import folium
import pandas as pd
import psycopg2
import requests
import streamlit as st
import streamlit.components.v1 as components
from dotenv import load_dotenv
from folium.plugins import Draw
from streamlit_autorefresh import st_autorefresh
from streamlit_folium import st_folium


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
# GENEL AYARLAR
# ============================================================

REFRESH_OPTIONS = {
    "Kapalı": 0,
    "10 sn": 10,
    "30 sn": 30,
    "60 sn": 60,
}

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
MAIN_TAB_ALERTS = "🚨 Alertler"
MAIN_TAB_EXPORT = "Veri Gözat / Dışa Aktar"
MAIN_TAB_FLIGHT_MAP = "🗺️ Uçuş Rotası"

MAIN_TAB_LABELS = [
    MAIN_TAB_RUNS,
    MAIN_TAB_CATALOG,
    MAIN_TAB_ALERTS,
    MAIN_TAB_EXPORT,
    MAIN_TAB_FLIGHT_MAP,
]

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
    "flight_id",
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
    # dagster/assets/clickhouse.py bu isimde bir tabloya (aynı ortam
    # değişkeniyle, CLICKHOUSE_TABLE) INSERT yapar; burada aynı isim
    # kullanılarak iki taraf senkron kalır.
    return os.environ.get(
        "CLICKHOUSE_TABLE",
        "telemetry",
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
        f"`{get_clickhouse_database()}`.`{get_clickhouse_table()}`"
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
    DESCRIBE TABLE {get_clickhouse_source()}
    """

    try:

        result = client.query(query)

    except Exception as exc:

        # Pipeline henüz hiç çalışmadıysa (clickhouse_telemetry asset'i
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

    columns = get_available_columns()

    if "class" not in columns:
        return []

    client = get_clickhouse_client()

    query = f"""
    SELECT DISTINCT class
    FROM {get_clickhouse_source()}
    ORDER BY class
    """

    result = client.query(query)

    return [
        row[0]
        for row in result.result_rows
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

def get_time_range():

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

    if "latitude" not in columns or "longitude" not in columns:
        return None, None, None, None

    client = get_clickhouse_client()

    query = f"""
    SELECT
        min(latitude), max(latitude),
        min(longitude), max(longitude)
    FROM {get_clickhouse_source()}
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
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
    HİÇBİRİNİN içinde olmayan satırları tutar (örn. "Erzurum dışındaki
    uçuşlar" için Erzurum'u çevreleyen bir alan çizip "exclude" seçilir).

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

        conditions.append(
            "class IN {classes:Array(String)}"
        )

        parameters["classes"] = [
            str(x)
            for x in selected_classes
        ]

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

        if (
            "latitude" in available_columns
            and "longitude" in available_columns
        ):

            area_conditions = []

            for index, polygon in enumerate(valid_area_polygons):

                lon_param = f"area_lons_{index}"
                lat_param = f"area_lats_{index}"

                area_conditions.append(
                    "pointInPolygon((longitude, latitude), "
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
            # parantez içine alınır. area_mode="exclude" ise bu grubun
            # tamamı NOT ile tersine çevrilir -- yani "hiçbir alanın
            # içinde olmayan" satırlar tutulur (örn. "Erzurum dışındaki
            # uçuşlar").
            area_group = "(" + " OR ".join(area_conditions) + ")"

            if area_mode == "exclude":
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

            if column not in available_columns:
                continue

            if operator == RANGE_FILTER_OPERATOR:

                value2 = value_filter.get("value2")

                if value is None or value2 is None:
                    continue

                param_min = f"value_filter_{index}_min"
                param_max = f"value_filter_{index}_max"

                between_expr = (
                    f"`{column}` BETWEEN "
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
                f"`{column}` "
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
# SATIR SAYISI
# ============================================================

def count_filtered_rows(
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

    query = f"""
    SELECT count() FROM (
        SELECT DISTINCT *
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
# dagster/metadata_store.py her asset materialize olduğunda
# asset_metadata_history tablosuna bir satır ekler (bkz.
# docs/postgres_asset_metadata_schema.sql). Dashboard bu tabloyu sadece
# OKUR -- yazma işlemi tamamen Dagster tarafında olur. Bu, yukarıdaki
# ASSET_CATALOG_QUERY'nin (Dagster GraphQL, sadece SON materialization)
# aksine, asset / uçuş / tarih bazlı filtrelenebilir bir GEÇMİŞ sağlar.

def get_postgres_conn_params() -> dict:
    return dict(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
        dbname=os.environ.get("POSTGRES_DATABASE", "postgres"),
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
                "SELECT DISTINCT asset_key FROM asset_metadata_history "
                "ORDER BY asset_key"
            )
            assets = [row[0] for row in cur.fetchall()]

            cur.execute(
                "SELECT DISTINCT flight_id FROM asset_metadata_history "
                "WHERE flight_id IS NOT NULL ORDER BY flight_id"
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
            # flight_id = ANY(...) yerine OR flight_id IS NULL de eklenir:
            # dvc_published_telemetry gibi belirli bir uçuşa değil, tüm
            # partition'a ait asset'ler flight_id=NULL ile kaydediliyor
            # (bkz. dagster/assets/publishing.py). Sadece ANY(...) ile
            # filtrelenseydi, bir uçuş seçildiğinde bu tür asset'ler
            # sonuçlardan tamamen kaybolurdu.
            clauses.append(
                "(flight_id = ANY(%s) OR flight_id IS NULL)"
            )
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
            FROM asset_metadata_history
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


def render_metadata_history() -> None:

    st.subheader(
        "🗄️ Metadata Geçmişi"
    )

    st.caption(
        "Yukarıdaki tablo her asset'in yalnızca SON materialization'ını "
        "gösterir. Burada, her materialization'da Postgres'e kaydedilen "
        "geçmiş; asset / uçuş / tarih bazlı filtrelenebilir "
        "(asset_metadata_history, bkz. docs/postgres_asset_metadata_schema.sql)."
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
            "Henüz metadata geçmişi yok. Pipeline en az bir kez "
            "çalıştığında (ve POSTGRES_* ortam değişkenleri doğru "
            "ayarlandığında) bu bölüm dolacaktır."
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

    metric_col1, metric_col2, metric_col3 = st.columns(3)

    metric_col1.metric("Kayıt", len(history_df))
    metric_col2.metric("Asset", history_df["asset_key"].nunique())
    metric_col3.metric(
        "Uçuş",
        history_df["flight_id"].dropna().nunique(),
    )

    st.caption(
        "En fazla 500 kayıt gösterilir, en yeni materialization önce."
    )

    display_df = history_df[
        [
            "asset_key",
            "group_name",
            "partition_date",
            "flight_id",
            "row_count",
            "materialized_at",
            "run_id",
        ]
    ].rename(
        columns={
            "asset_key": "Asset",
            "group_name": "Grup",
            "partition_date": "Tarih",
            "flight_id": "Uçuş",
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
        display_metadata = {
            "flight_id": row["flight_id"] or "-",
            "asset_key": row["asset_key"],
            "partition_date": row["partition_date"],
            "materialized_at": row["materialized_at"],
        }

        for key, value in (row["metadata"] or {}).items():
            if key not in display_metadata:
                display_metadata[key] = value

        with st.expander(
            f"✈️ {row['flight_id'] or '-'} — {row['asset_key']} — "
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
                    "olan adımlar listeden gizlenir."
                ),
            )

        with filter_col2:

            time_filter = st.radio(
                "Zaman aralığı",
                options=TIME_FILTER_OPTIONS,
                index=0,
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

                    st.link_button(
                        "🔁 Dagster'da Aç ve Tekrar Çalıştır",
                        run_url,
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
    "all": "📄 Tüm Veri (Tek CSV)",
    "zip": "📦 Uçuş Bazlı ZIP",
    "each": "✈️ Uçuşları Tek Tek İndir",
    "merge": "🧩 Seçili Uçuşları Birleştir",
}


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
    başlamaması için, her format kendi alt fonksiyonunda ayrıca bir
    "Oluştur" butonuna basılmasını bekler (bkz. _render_all_data_csv_download
    vb.) -- dönüşüm yalnızca o butona basılınca çalışır.

    share_params: mevcut filtre durumunun URL query parametresi olarak
    kodlanmış hâli (bkz. _encode_export_state_to_query_params). Burada,
    format seçiminin hemen yanında "bu filtreleri bağlantı olarak
    paylaş" seçeneği için kullanılır.
    """

    time_suffix = (
        f"{start_time.strftime('%Y%m%d_%H%M%S')}_"
        f"{end_time.strftime('%Y%m%d_%H%M%S')}"
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
            full_share_params["tab"] = "export"

            for existing_key in list(st.query_params.keys()):
                if existing_key.startswith("export_") or existing_key == "tab":
                    del st.query_params[existing_key]

            st.query_params.update(full_share_params)

            st.session_state["export_share_query_string"] = urlencode(
                full_share_params
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

    # flight_groups her render'da groupby ile yeniden oluşturulduğu için
    # kendi id()'si her seferinde değişir; cache anahtarı bunun yerine
    # session_state'ten gelen (dolayısıyla rerun'lar arasında sabit
    # kalan) dataframe'in id()'sine dayanır.
    dataframe_id = id(dataframe)

    if selected_format == "zip":

        _render_flight_zip_download(
            flight_groups,
            time_suffix,
            dataframe_id,
        )

    elif selected_format == "each":

        _render_flight_individual_downloads(
            flight_groups,
            time_suffix,
            dataframe_id,
        )

    else:

        _render_flight_merge_download(
            flight_groups,
            time_suffix,
            dataframe_id,
        )


def _render_all_data_csv_download(
    dataframe: pd.DataFrame,
    time_suffix: str,
) -> None:

    st.caption(
        f"CSV dosyası: {len(dataframe):,} satır, "
        f"{len(dataframe.columns)} kolon"
    )

    # Dönüşüm yalnızca kullanıcı açıkça bu butona basınca çalışır --
    # aksi halde "Tüm Veri" formatı varsayılan seçili geldiği için sekme
    # açılır açılmaz (kullanıcı hiçbir şey yapmadan) büyük veri setinde
    # saniyeler süren bir CSV dönüşümü otomatik başlıyordu.

    cache_key = ("all", id(dataframe))

    if st.button(
        "📄 CSV Oluştur",
        type="primary",
        key="prepare_all_data_csv",
    ):

        # Büyük veri setlerinde bu dönüşüm birkaç saniye sürebiliyor;
        # spinner olmadan sekme "donmuş" gibi görünüyor ve kullanıcı bir
        # hata olduğunu düşünebiliyordu.

        with st.spinner(
            "Veriniz indirmeye hazırlanıyor, veri miktarına bağlı olarak "
            "bu işlem biraz sürebilir..."
        ):

            csv_bytes = dataframe.to_csv(
                index=False
            ).encode(
                "utf-8-sig"
            )

        st.session_state["download_all_data_cache"] = {
            "key": cache_key,
            "bytes": csv_bytes,
        }

    cache = st.session_state.get("download_all_data_cache")

    if cache and cache["key"] == cache_key:

        st.download_button(
            label="⬇️ Tüm Veriyi CSV Olarak İndir",
            data=cache["bytes"],
            file_name=f"au_air_telemetry_{time_suffix}.csv",
            mime="text/csv",
            type="primary",
            key="download_all_data_csv",
        )

    else:

        st.caption(
            "İndirme dosyasını oluşturmak için yukarıdaki butona basın."
        )


def _render_flight_zip_download(
    flight_groups: dict,
    time_suffix: str,
    dataframe_id: int,
) -> None:

    st.caption(
        f"{len(flight_groups)} uçuş, toplam "
        f"{sum(len(g) for g in flight_groups.values()):,} satır."
    )

    cache_key = ("zip", dataframe_id)

    if st.button(
        "📦 ZIP Oluştur",
        type="primary",
        key="prepare_flight_zip",
    ):

        with st.spinner(
            f"{len(flight_groups)} uçuş için ZIP hazırlanıyor..."
        ):

            zip_buffer = io.BytesIO()

            with zipfile.ZipFile(
                zip_buffer,
                mode="w",
                compression=zipfile.ZIP_DEFLATED,
            ) as zip_file:

                for flight, group_df in flight_groups.items():

                    csv_bytes = group_df.to_csv(
                        index=False
                    ).encode("utf-8-sig")

                    zip_file.writestr(
                        f"ucus_{flight}_{time_suffix}.csv",
                        csv_bytes,
                    )

        st.session_state["download_flight_zip_cache"] = {
            "key": cache_key,
            "bytes": zip_buffer.getvalue(),
        }

    cache = st.session_state.get("download_flight_zip_cache")

    if cache and cache["key"] == cache_key:

        st.download_button(
            label=(
                f"⬇️ Tüm Uçuşları ZIP Olarak İndir "
                f"({len(flight_groups)} dosya)"
            ),
            data=cache["bytes"],
            file_name=f"ucuslar_{time_suffix}.zip",
            mime="application/zip",
            type="primary",
            key="download_all_flights_zip",
        )

        st.caption(
            "Her uçuş için ayrı bir CSV dosyası içerir."
        )

    else:

        st.caption(
            "İndirme dosyasını oluşturmak için yukarıdaki butona basın."
        )


def _render_flight_individual_downloads(
    flight_groups: dict,
    time_suffix: str,
    dataframe_id: int,
) -> None:

    cache_key = ("each", dataframe_id)

    if st.button(
        "✈️ CSV'leri Oluştur",
        type="primary",
        key="prepare_flight_individual_csvs",
    ):

        with st.spinner(
            f"{len(flight_groups)} uçuş için CSV dosyaları hazırlanıyor..."
        ):

            flight_csv_bytes = {
                flight: group_df.to_csv(
                    index=False
                ).encode("utf-8-sig")
                for flight, group_df in flight_groups.items()
            }

        st.session_state["download_flight_individual_cache"] = {
            "key": cache_key,
            "bytes": flight_csv_bytes,
        }

    cache = st.session_state.get("download_flight_individual_cache")

    if not (cache and cache["key"] == cache_key):

        st.caption(
            "İndirme dosyalarını oluşturmak için yukarıdaki butona basın."
        )

        return

    flight_csv_bytes = cache["bytes"]

    for flight, group_df in sorted(flight_groups.items()):

        col_a, col_b = st.columns(
            [3, 1]
        )

        with col_a:

            st.write(
                f"**{flight}** — {len(group_df):,} satır"
            )

        with col_b:

            st.download_button(
                label="CSV indir",
                data=flight_csv_bytes[flight],
                file_name=f"ucus_{flight}_{time_suffix}.csv",
                mime="text/csv",
                key=f"download_flight_{flight}",
            )


def _render_flight_merge_download(
    flight_groups: dict,
    time_suffix: str,
    dataframe_id: int,
) -> None:
    """
    Filtreye uyan uçuşlar arasından kullanıcının seçtiği BİRKAÇININ
    (hepsinin değil) satırlarını tek bir CSV'de birleştirir -- örn.
    filtreye uyan 5 uçuş varken yalnızca uçuş1 ve uçuş2'yi birleştirip
    tek dosya olarak indirmek için. "Tüm Veri" formatından farkı budur:
    o format filtreye uyan TÜM uçuşları birleştirir, burası ise
    kullanıcının seçtiği bir alt kümeyi.
    """

    flight_options = sorted(flight_groups.keys())

    selected_flights_to_merge = st.multiselect(
        "Birleştirilecek uçuşlar",
        options=flight_options,
        help=(
            "Filtreye uyan uçuşlar arasından, tek bir CSV'de "
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

    cache_key = (
        "merge",
        dataframe_id,
        tuple(sorted(selected_flights_to_merge)),
    )

    if st.button(
        "🧩 Birleştirilmiş CSV Oluştur",
        type="primary",
        key="prepare_flight_merge_csv",
    ):

        with st.spinner(
            f"{len(selected_flights_to_merge)} uçuş birleştiriliyor..."
        ):

            merged_df = pd.concat(
                [
                    flight_groups[flight]
                    for flight in selected_flights_to_merge
                ],
                ignore_index=True,
            )

            csv_bytes = merged_df.to_csv(
                index=False
            ).encode("utf-8-sig")

        st.session_state["download_flight_merge_cache"] = {
            "key": cache_key,
            "bytes": csv_bytes,
        }

    cache = st.session_state.get("download_flight_merge_cache")

    if cache and cache["key"] == cache_key:

        # Dosya adına en fazla 3 uçuşun adı eklenir; daha fazlasında
        # dosya adı okunaksız uzayacağı için sadece sayı belirtilir.
        if len(selected_flights_to_merge) <= 3:
            flights_label = "_".join(sorted(selected_flights_to_merge))
        else:
            flights_label = f"{len(selected_flights_to_merge)}_ucus"

        st.download_button(
            label=(
                f"⬇️ Birleştirilmiş CSV'yi İndir "
                f"({len(selected_flights_to_merge)} uçuş)"
            ),
            data=cache["bytes"],
            file_name=f"ucuslar_birlesik_{flights_label}_{time_suffix}.csv",
            mime="text/csv",
            type="primary",
            key="download_flight_merge_csv",
        )

    else:

        st.caption(
            "İndirme dosyasını oluşturmak için yukarıdaki butona basın."
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

    params = {
        "export_st": start_time.strftime("%Y-%m-%dT%H:%M:%S"),
        "export_et": end_time.strftime("%Y-%m-%dT%H:%M:%S"),
    }

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
            f"Şema okunamadı: {exc}"
        )

        return

    if schema.empty:

        st.warning(
            f"`{get_clickhouse_database()}.{get_clickhouse_table()}` "
            "tablosu henüz oluşturulmamış. Dagster pipeline'ı en az bir "
            "kez çalışıp `clickhouse_telemetry` asset'ini materialize "
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
            "İşlenmiş parquet dosyalarında veri bulunamadı."
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

            if (
                "export_duration_enabled" not in st.session_state
                and "duration_filter" in pending_url_state
            ):
                st.session_state["export_duration_enabled"] = True
                st.session_state["export_duration_operator"] = (
                    pending_url_state["duration_filter"]["operator"]
                )
                st.session_state["export_duration_hours"] = float(
                    pending_url_state["duration_filter"]["hours"]
                )
                st.session_state["export_duration_hours2"] = float(
                    pending_url_state["duration_filter"].get(
                        "hours2", 0.0
                    )
                )

            duration_enabled = st.checkbox(
                "⏱️ Uçuş süresine göre filtrele",
                help=(
                    "Örn. 4 saatten kısa süren uçuşları filtrelemek için "
                    "operatörü \"<\", saati 4 seçin. Süre, her uçuşun ilk "
                    "ve son telemetri zaman damgası arasındaki farktan "
                    "hesaplanır."
                ),
                key="export_duration_enabled",
            )

            duration_filter = None

            if duration_enabled:

                dcol1, dcol2, dcol3 = st.columns([2, 2, 2])

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

                    duration_hours = st.number_input(
                        "Min Saat" if is_duration_range else "Saat",
                        min_value=0.0,
                        value=float(
                            st.session_state.get(
                                "export_duration_hours", 4.0
                            )
                        ),
                        step=0.5,
                        key="export_duration_hours",
                    )

                duration_hours2 = None

                if is_duration_range:

                    with dcol3:

                        duration_hours2 = st.number_input(
                            "Maks Saat",
                            min_value=0.0,
                            value=float(
                                st.session_state.get(
                                    "export_duration_hours2", 8.0
                                )
                            ),
                            step=0.5,
                            key="export_duration_hours2",
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

        with col1:

            start_time = st.datetime_input(
                "Başlangıç zamanı",
                value=min_time,
                key="export_start_time",
            )

        with col2:

            end_time = st.datetime_input(
                "Bitiş zamanı",
                value=max_time,
                key="export_end_time",
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
            has_lat_lon = (
                "latitude" in get_available_columns()
                and "longitude" in get_available_columns()
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

            if seed_active:

                for polygon in url_seed_polygons:

                    folium.Polygon(
                        locations=[
                            (lat, lon) for lon, lat in polygon
                        ],
                        color="#eda100",
                        weight=2,
                        dash_array="6,4",
                        fill=True,
                        fill_opacity=0.15,
                        tooltip="Paylaşılan bağlantıdan yüklenen alan",
                    ).add_to(area_map)

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

            map_output = st_folium(
                area_map,
                key=f"area_filter_map_{area_map_version}",
                height=420,
                use_container_width=True,
                returned_objects=["all_drawings"],
            )

            drawings = (
                (map_output or {}).get("all_drawings") or []
            )

            skipped_degenerate_shapes = 0
            drawn_polygons = []

            for drawing in drawings:

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

            # URL'den gelen alan(lar) (seed_active) ile haritada YENİ
            # çizilen alan(lar) birbirini SİLMEZ, birlikte (OR ile)
            # uygulanır -- bkz. yukarıdaki "ÖNEMLİ" notu.
            area_polygons = (
                list(url_seed_polygons) if seed_active else []
            ) + drawn_polygons

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

            if drawn_polygons and seed_active:

                st.success(
                    f"✅ Bağlantıdaki {len(url_seed_polygons)} alana, "
                    f"haritada eklediğiniz {len(drawn_polygons)} alan "
                    f"daha eklendi (toplam {len(area_polygons)}) — "
                    f"{area_summary_suffix} aşağıdaki filtrelerle "
                    "birlikte (bölgeler OR ile birleştirilerek) "
                    "uygulanacak."
                )

            elif drawn_polygons:

                st.success(
                    f"✅ {len(drawn_polygons)} alan seçildi — "
                    f"{area_summary_suffix} aşağıdaki filtrelerle "
                    "birlikte (bölgeler OR ile birleştirilerek) "
                    "uygulanacak."
                )

            elif seed_active:

                st.info(
                    f"🔗 {len(url_seed_polygons)} alan paylaşılan "
                    "bağlantıdan yüklendi (haritada kesikli çizgiyle "
                    f"gösteriliyor) — {area_summary_suffix} aşağıdaki "
                    "filtrelerle birlikte uygulanacak. Ek bir alan daha "
                    "eklemek için haritada yeni bir şekil çizin."
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
                "Class",
                options=available_classes,
                help="Boş bırakılırsa tüm class değerleri seçilir.",
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

                if vf["column"] in numeric_columns and valid_operator:

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

    if start_time > end_time:

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
        active_filters.append(f"{len(selected_classes)} class")

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

        route_df = route_df.dropna(
            subset=["latitude", "longitude", "altitude"]
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

    # streamlit-folium'un JS tarafı bazen bileşenin iframe yüksekliğini
    # (Streamlit.setFrameHeight argümansız çağrıldığında document.body.
    # scrollHeight'a düşüyor) kararsız ölçer; height'ı !important ile
    # sabitlemek bunu geçersiz kılar (bkz. Alan Bazlı Filtre haritasındaki
    # aynı düzeltme). CSS, yalnızca bu haritayı saran container'a
    # (st.container(key=...) ile eklenen "st-key-..." sınıfı) scope
    # edilir ki Alan Bazlı Filtre haritasının kendi (420px) yüksekliğini
    # etkilemesin.
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
    # SIDEBAR (sadece bağlantı bilgisi)
    # ========================================================
    #
    # Çalıştırma kontrolleri (run sayısı, otomatik yenileme, "Şimdi
    # yenile") artık aşağıdaki yan sekme panelinde -- bkz. "SEKME
    # SEÇİMİ + KONTROLLER". Sidebar sadece salt-okunur bağlantı
    # bilgisini gösterir.

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
            f"ClickHouse tablosu: "
            f"{get_clickhouse_database()}.{get_clickhouse_table()}"
        )

        st.caption(
            f"Parquet yedeği: "
            f"{get_processed_files_glob()}"
        )

    # ========================================================
    # SEKME SEÇİMİ + KONTROLLER (yan panel)
    # ========================================================
    #
    # st.radio/st.tabs yerine, ekranın solunda dikey sıralanmış
    # butonlarla "sekme" görünümü oluşturulur -- her buton aktif
    # sekmeyse "primary" (dolu/renkli), değilse "secondary"
    # (outline) tipiyle çizilir. st.button, Streamlit'in iç DOM
    # yapısı sürüm sürüm değişse de (bkz. dashboard/requirements.txt
    # içindeki pinlenmiş streamlit==1.38.0) kararlı kalan, sade bir
    # bileşen olduğu için CSS hack'i gerekmeden güvenilir biçimde
    # "aktif/pasif" görünümü verir.
    #
    # URL'de tab=export ya da herhangi bir export_* parametresi
    # varsa (paylaşılan bir bağlantı üzerinden açılmışsa), İLK
    # render'da otomatik olarak "Veri Gözat / Dışa Aktar" sekmesi
    # seçilir. session_state["active_main_tab"] zaten set edilmişse
    # (kullanıcı elle başka bir sekmeye geçtiyse) bu değer
    # UYGULANMAZ -- yoksa kullanıcı sekme değiştirdikten sonra her
    # rerun'da (örn. otomatik yenileme) URL'deki sekmeye geri dönerdi.
    #
    # Run sayısı / otomatik yenileme / "Şimdi yenile" de aynı panelde
    # -- bunlar runs_df'i (aşağıda) ve otomatik yenilemeyi etkilediği
    # için değerleri (run_limit, refresh_seconds) content_col'daki
    # sekme içerikleri render edilmeden ÖNCE burada okunmalı.

    if "active_main_tab" not in st.session_state:

        shared_query_params = st.query_params.to_dict()

        if shared_query_params.get("tab") == "export" or any(
            key.startswith("export_")
            for key in shared_query_params
        ):
            st.session_state["active_main_tab"] = MAIN_TAB_EXPORT
        else:
            st.session_state["active_main_tab"] = MAIN_TAB_RUNS

    nav_col, content_col = st.columns(
        [1, 5],
        gap="large",
    )

    with nav_col:

        for tab_label in MAIN_TAB_LABELS:

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
                st.rerun()

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
            "🔄 Şimdi yenile",
            use_container_width=True,
        ):

            fetch_runs.clear()
            fetch_asset_catalog.clear()
            load_alerts.clear()
            get_available_columns.clear()
            get_numeric_columns.clear()
            get_clickhouse_schema.clear()
            get_available_classes.clear()
            get_available_flights.clear()
            check_clickhouse_connection.clear()

            st.rerun()

        st.caption(
            "Son sorgu: "
            f"{datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"
        )

    active_tab = st.session_state["active_main_tab"]

    # ========================================================
    # AUTO REFRESH
    # ========================================================

    # "Veri Gözat / Dışa Aktar" akışında otomatik yenileme, gerçek
    # ortamda doğrulanmış şekilde export'u bozuyordu:
    #
    #  1) Sadece interval değerini büyütmek (ör. 24 saate çekmek)
    #     YETERLİ DEĞİL — streamlit_autorefresh bileşeninin JS tarafı
    #     zaten çalışan bir zamanlayıcı varken yeni interval'i
    #     güvenilir şekilde uygulamıyor. Bu yüzden bileşen, interval
    #     değiştirilerek değil doğrudan HİÇ MONTE EDİLMEYEREK devre
    #     dışı bırakılır (aşağıdaki st.empty() ile).
    #
    #  2) Bunu yalnızca "veri getirildikten sonra" (export_df set
    #     edildikten sonra) yapmak da yetersizdi: otomatik yenileme
    #     hâlâ aktifken "📥 Veriyi Getir ve Önizle" tıklaması, o anda
    #     tetiklenen bir yenilemeyle yarışıp sunucuya hiç ulaşmamış
    #     gibi kayboluyordu — export_df bir türlü set edilemiyor,
    #     "3️⃣ İndir" sonsuza dek boş kalıyordu. Bu yüzden duraklatma,
    #     satır sayısı hesaplandığı anda (export_row_count set
    #     edilir edilmez) başlar; "Veriyi Getir ve Önizle" tıklandığı
    #     an otomatik yenileme zaten devre dışıdır, yarış oluşmaz.
    #     (~874k satırlık bir export ile: otomatik yenileme AÇIKKEN
    #     asla tamamlanmadı, bu düzeltmeyle ~20-30 saniyede sorunsuz
    #     tamamlandı.)
    #
    #  3) Aynı yarış, satır sayısı hesaplanmadan ÖNCE "1️⃣ Filtrele"
    #     adımındaki Alan Bazlı Filtre haritasını (streamlit-folium/
    #     Leaflet iframe'i) da bozuyordu: harita ekrandayken bir
    #     otomatik yenileme rerun'u tetiklenirse, iframe'in Leaflet
    #     içeriği tam yeniden mount olmadan komponent yeniden
    #     boyutlandırılıyor ve iframe'in ölçülen yüksekliği 420px
    #     yerine ~1900px'e sıçrayıp haritanın altında dev bir boş
    #     alan bırakıyor, sayfanın geri kalanını aşağı itiyordu.
    #     Bu yüzden duraklatma yalnızca export_in_progress ile değil,
    #     kullanıcı "Veri Gözat / Dışa Aktar" sekmesindeyken de
    #     (haritayla etkileşim satır sayısı hesaplanmadan önce
    #     olduğu için) uygulanır.

    export_in_progress = (
        st.session_state.get("export_row_count") is not None
        or st.session_state.get("export_df") is not None
        or active_tab == MAIN_TAB_EXPORT
    )

    autorefresh_slot = st.empty()

    if refresh_seconds and not export_in_progress:

        with autorefresh_slot:

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
    # YENİ ALERT BİLDİRİMİ (TOAST)
    # ========================================================
    #
    # Hangi sekme açık olursa olsun, yeni bir hata oluştuğunda (veya
    # bir hata çözüldüğünde) ekranın köşesinde küçük bir bildirim
    # gösterilir. Auto-refresh açıksa bu her yenilemede tetiklenir.

    notify_new_alerts(
        load_alerts()
    )

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