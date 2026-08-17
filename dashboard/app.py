"""
A3 - Katalog / Pipeline Metrik Dashboard'u

docs/postgres_manifest_schema.sql içindeki `conversion_manifest` tablosunu
okuyup pipeline'ın (tab -> parquet -> clickhouse) durumunu görselleştirir.

Ortam değişkenleri (docker-compose'da servis olarak set edilmeli):
    POSTGRES_HOST       (varsayılan: postgres)
    POSTGRES_PORT       (varsayılan: 5432)
    POSTGRES_DB         (varsayılan: pipeline)
    POSTGRES_USER
    POSTGRES_PASSWORD
"""

import os
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from streamlit_autorefresh import st_autorefresh

st.set_page_config(
    page_title="İHA Veri Platformu - Katalog & Metrikler",
    layout="wide",
)

REFRESH_OPTIONS = {"Kapalı": 0, "10 sn": 10, "30 sn": 30, "60 sn": 60}


def get_db_url() -> str:
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "pipeline")
    user = os.environ.get("POSTGRES_USER", "postgres")
    password = os.environ.get("POSTGRES_PASSWORD", "postgres")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


@st.cache_resource
def get_engine() -> Engine:
    return create_engine(get_db_url(), pool_pre_ping=True)


@st.cache_data(ttl=10)
def load_manifest() -> pd.DataFrame:
    query = text(
        """
        SELECT
            id, tab_file_name, ham_file_name, flight_id,
            status, attempt_count, max_attempts,
            row_count_tab, row_count_parquet, row_count_clickhouse,
            content_fingerprint,
            parquet_object_key, parquet_size_bytes,
            clickhouse_loaded_at, error_detail,
            created_at, updated_at
        FROM conversion_manifest
        ORDER BY updated_at DESC
        """
    )
    with get_engine().connect() as conn:
        df = pd.read_sql(query, conn)
    return df


def reconciliation_mask(df: pd.DataFrame) -> pd.Series:
    """Üç katman arasında satır sayısı uyuşmazlığı olan, tamamlanmış kayıtlar."""
    done = df["status"] == "done"
    mismatch = (df["row_count_tab"] != df["row_count_parquet"]) | (
        df["row_count_parquet"] != df["row_count_clickhouse"]
    )
    return done & mismatch


def render_kpis(df: pd.DataFrame) -> None:
    total = len(df)
    counts = df["status"].value_counts().to_dict()
    done = counts.get("done", 0)
    pending = counts.get("pending", 0)
    processing = counts.get("processing", 0)
    failed = counts.get("verification_failed", 0) + counts.get("needs_review", 0)
    mismatches = int(reconciliation_mask(df).sum()) if total else 0

    cols = st.columns(6)
    cols[0].metric("Toplam Dosya", f"{total:,}")
    cols[1].metric("Tamamlandı", f"{done:,}")
    cols[2].metric("Bekliyor", f"{pending:,}")
    cols[3].metric("İşleniyor", f"{processing:,}")
    cols[4].metric("Hatalı / İnceleme Bekliyor", f"{failed:,}")
    cols[5].metric(
        "Mutabakat Uyuşmazlığı",
        f"{mismatches:,}",
        delta=None if mismatches == 0 else "dikkat",
        delta_color="inverse",
    )


def render_status_chart(df: pd.DataFrame) -> None:
    st.subheader("Durum Dağılımı")
    if df.empty:
        st.info("Henüz manifest kaydı yok.")
        return
    status_counts = df["status"].value_counts().rename_axis("status").reset_index(name="count")
    st.bar_chart(status_counts.set_index("status"))


def render_reconciliation_table(df: pd.DataFrame) -> None:
    st.subheader("Üç Katman Mutabakat Kontrolü (tab / parquet / clickhouse)")
    mismatched = df[reconciliation_mask(df)]
    if mismatched.empty:
        st.success("Tamamlanan kayıtlarda satır sayısı uyuşmazlığı bulunamadı.")
    else:
        st.warning(f"{len(mismatched)} kayıtta katmanlar arası satır sayısı uyuşmuyor.")
        st.dataframe(
            mismatched[
                [
                    "tab_file_name",
                    "row_count_tab",
                    "row_count_parquet",
                    "row_count_clickhouse",
                    "updated_at",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )


def render_error_table(df: pd.DataFrame) -> None:
    st.subheader("Hatalı / İncelenmesi Gereken Kayıtlar")
    errored = df[df["status"].isin(["verification_failed", "needs_review"])]
    if errored.empty:
        st.success("Hatalı veya incelemede kayıt yok.")
        return
    st.dataframe(
        errored[
            [
                "tab_file_name",
                "status",
                "attempt_count",
                "max_attempts",
                "error_detail",
                "updated_at",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )


def render_full_table(df: pd.DataFrame) -> None:
    st.subheader("Katalog")

    status_filter = st.multiselect(
        "Duruma göre filtrele",
        options=sorted(df["status"].unique().tolist()) if not df.empty else [],
        default=[],
    )
    filtered = df if not status_filter else df[df["status"].isin(status_filter)]

    search = st.text_input("Dosya adı / uçuş id ara")
    if search:
        mask = filtered["tab_file_name"].str.contains(search, case=False, na=False) | filtered[
            "flight_id"
        ].astype(str).str.contains(search, case=False, na=False)
        filtered = filtered[mask]

    st.dataframe(filtered, use_container_width=True, hide_index=True)


def main() -> None:
    st.title("İHA Veri Platformu — Katalog & Pipeline Metrikleri")
    st.caption(
        "conversion_manifest tablosundan canlı okunur "
        "(A2'nin dvc/pipeline'ı ve A1'in ingestion'ı burada görünmez, "
        "yalnızca tab -> parquet -> clickhouse dağıtım durumu izlenir)."
    )

    with st.sidebar:
        st.header("Ayarlar")
        refresh_label = st.selectbox("Otomatik yenileme", list(REFRESH_OPTIONS.keys()), index=2)
        refresh_seconds = REFRESH_OPTIONS[refresh_label]
        if st.button("Şimdi yenile"):
            load_manifest.clear()
        st.caption(f"Son sorgu: {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}")

    if refresh_seconds:
        st_autorefresh(interval=refresh_seconds * 1000, key="manifest_refresh")

    try:
        df = load_manifest()
    except Exception as exc:  # noqa: BLE001
        st.error(f"Veritabanına bağlanılamadı: {exc}")
        st.stop()

    render_kpis(df)
    st.divider()

    left, right = st.columns([1, 1])
    with left:
        render_status_chart(df)
    with right:
        render_error_table(df)

    st.divider()
    render_reconciliation_table(df)
    st.divider()
    render_full_table(df)


if __name__ == "__main__":
    main()
