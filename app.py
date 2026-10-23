"""
Streaming Data Dashboard  (Kafka + Spark + MongoDB + Streamlit)

Tabs
  Live        - consumes the Kafka topic and refreshes every N seconds (default 15)
  History     - every record collected in MongoDB, with filters
  Aggregation - Spark window aggregates (MongoDB) or an interactive roll-up
  About       - API, pipeline and why the dashboard matters

Run:  streamlit run app.py
"""

import json
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import config
import exporters
from storage import MongoStore

try:
    from kafka import KafkaConsumer, TopicPartition
    from kafka.errors import KafkaError
except ImportError:  # pragma: no cover
    KafkaConsumer = None

st.set_page_config(page_title="Stream Monitor", page_icon="📡", layout="wide",
                   initial_sidebar_state="expanded")

st.markdown(
    """
    <style>
      .block-container {padding-top: 2.2rem;}
      .pill {display:inline-block; padding:2px 10px; border-radius:999px; font-size:.78rem;
             font-weight:600; margin-right:6px; border:1px solid rgba(128,128,128,.35);}
      .pill.ok   {background:rgba(16,185,129,.15); color:#059669;}
      .pill.warn {background:rgba(245,158,11,.15); color:#b45309;}
      .pill.bad  {background:rgba(239,68,68,.15);  color:#dc2626;}
      div[data-testid="stMetric"] {border-radius:12px;}
    </style>
    """,
    unsafe_allow_html=True,
)

LIVE_COLUMNS = ["timestamp", "value", "metric_type", "sensor_id", "source",
                "unit", "location", "latitude", "longitude"]
RANGES = {"Last 15 minutes": timedelta(minutes=15), "Last hour": timedelta(hours=1),
          "Last 6 hours": timedelta(hours=6), "Last 24 hours": timedelta(hours=24),
          "Last 7 days": timedelta(days=7), "Last 30 days": timedelta(days=30),
          "All time": None, "Custom": "custom"}
ROLLUPS = {"1 minute": "1min", "5 minutes": "5min", "15 minutes": "15min",
           "1 hour": "1h", "1 day": "1D"}
PX_COLORS = ["#2563EB", "#F97316", "#10B981", "#A855F7", "#EF4444", "#0EA5E9", "#EAB308", "#64748B"]


# =========================================================================== #
# Storage / demo helpers
# =========================================================================== #
@st.cache_resource(show_spinner=False)
def get_store(uri: str, db: str) -> MongoStore:
    return MongoStore(uri=uri, db=db)


@st.cache_data(ttl=10, show_spinner=False)
def mongo_available(uri: str, db: str) -> bool:
    try:
        return get_store(uri, db).ping()
    except Exception:  # noqa: BLE001
        return False


@st.cache_data(show_spinner=False)
def demo_dataframe(hours: int = 24) -> pd.DataFrame:
    """Synthetic data so the UI can be explored with no services running."""
    rng = np.random.default_rng(42)
    end = pd.Timestamp.now(tz="UTC").floor("15s")
    ts = pd.date_range(end=end, periods=hours * 240, freq="15s")
    frames = []
    cities = (("manila", 28, 0.35, 14.60, 120.98), ("quezon_city", 24, 0.30, 14.68, 121.04),
              ("cebu_city", 15, 0.25, 10.32, 123.89))
    for city, base, vol, lat, lon in cities:
        walk = np.clip(base + np.cumsum(rng.normal(0, vol, len(ts))), 2, None)
        frames.append(pd.DataFrame({"timestamp": ts, "value": walk.round(2), "metric_type": "pm2_5",
                                    "sensor_id": city, "source": "demo", "unit": "μg/m³",
                                    "location": city.replace("_", " ").title(),
                                    "latitude": lat, "longitude": lon}))
    return pd.concat(frames, ignore_index=True)[LIVE_COLUMNS]


def to_tz(df: pd.DataFrame, tz: str) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    for col in ("timestamp", "window_start", "window_end"):
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], utc=True).dt.tz_convert(tz)
    return out


def rollup(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """Pandas equivalent of the Spark window aggregation, for any bucket size."""
    cols = ["window_start", "window_end", "source", "sensor_id", "metric_type", "unit",
            "avg_value", "min_value", "max_value", "stddev_value", "count"]
    if df.empty:
        return pd.DataFrame(columns=cols)
    d = df.copy()
    d["window_start"] = d["timestamp"].dt.floor(freq)
    d["unit"] = d["unit"].fillna("")
    d["source"] = d["source"].fillna("")
    out = (d.groupby(["window_start", "source", "sensor_id", "metric_type", "unit"])["value"]
             .agg(avg_value="mean", min_value="min", max_value="max", stddev_value="std", count="count")
             .reset_index())
    out["window_end"] = out["window_start"] + pd.Timedelta(freq)
    return out[cols]


# =========================================================================== #
# Sidebar
# =========================================================================== #
def setup_sidebar() -> Dict:
    st.sidebar.title("📡 Stream Monitor")
    st.sidebar.caption("API → Kafka → Spark → MongoDB → Streamlit")

    with st.sidebar.expander("🔌 Connections"):
        broker = st.text_input("Kafka broker", config.KAFKA_BOOTSTRAP)
        topic = st.text_input("Kafka topic", config.KAFKA_TOPIC)
        uri = st.text_input("MongoDB URI", config.MONGO_URI)
        db = st.text_input("MongoDB database", config.MONGO_DB)

    st.sidebar.subheader("⏱ Live refresh")
    auto = st.sidebar.toggle("Auto-refresh", value=True)
    interval = st.sidebar.slider("Interval (seconds)", 5, 60, config.DEFAULT_REFRESH_SECONDS, step=5,
                                 disabled=not auto)
    st.sidebar.subheader("🌏 Display")
    tz = st.sidebar.selectbox("Time zone", ["Asia/Manila", "UTC", "America/New_York", "Europe/London",
                                            "Asia/Tokyo"], index=0 if config.DISPLAY_TZ == "Asia/Manila" else 1)
    demo_ok = st.sidebar.toggle("Use demo data when offline", value=True,
                                help="Lets you explore the UI without Kafka / MongoDB running.")
    return {"broker": broker, "topic": topic, "uri": uri, "db": db, "auto": auto,
            "interval": interval, "tz": tz, "demo_ok": demo_ok}


def pill(text: str, kind: str) -> str:
    return f'<span class="pill {kind}">{text}</span>'


# =========================================================================== #
# Live: Kafka consumer
# =========================================================================== #
def _get_consumer(cfg: Dict):
    """Per-session consumer. Returns (consumer|None, message). Retries at most every 20 s."""
    key = f"consumer::{cfg['broker']}::{cfg['topic']}"
    state = st.session_state
    if state.get(key) is not None:
        return state[key], "connected"
    if KafkaConsumer is None:
        return None, "kafka-python not installed"
    if time.time() - state.get(key + "::last_try", 0) < 20:
        return None, state.get(key + "::err", "retrying soon")
    state[key + "::last_try"] = time.time()
    try:
        # connection-timeout option was renamed in kafka-python 3.x
        timeout_opt = ("bootstrap_timeout_ms" if "bootstrap_timeout_ms" in KafkaConsumer.DEFAULT_CONFIG
                       else "api_version_auto_timeout_ms")
        consumer = KafkaConsumer(
            bootstrap_servers=[cfg["broker"]], group_id=None, enable_auto_commit=False,
            auto_offset_reset="latest", consumer_timeout_ms=1000,
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            **{timeout_opt: 3000},
        )
        parts = consumer.partitions_for_topic(cfg["topic"])
        if not parts:
            consumer.close()
            raise RuntimeError(f"topic '{cfg['topic']}' not found yet - start the producer")
        tps = [TopicPartition(cfg["topic"], p) for p in parts]
        consumer.assign(tps)
        consumer.seek_to_end(*tps)
        state[key] = consumer
        state[key + "::err"] = ""
        return consumer, "connected"
    except Exception as exc:  # noqa: BLE001
        state[key] = None
        kind = type(exc).__name__
        if any(w in kind for w in ("Timeout", "Connection", "NoBrokers")):
            state[key + "::err"] = f"cannot reach Kafka at {cfg['broker']} - is the broker running?"
        else:
            state[key + "::err"] = f"{kind}: {exc}"[:160]
        return None, state[key + "::err"]


def consume_kafka_data(cfg: Dict, poll_ms: int = 1500) -> Tuple[Optional[pd.DataFrame], str]:
    """Poll Kafka and append to this session's rolling buffer."""
    consumer, msg = _get_consumer(cfg)
    if consumer is None:
        return None, msg
    buf: List[dict] = st.session_state.setdefault("live_buffer", [])
    try:
        batches = consumer.poll(timeout_ms=poll_ms, max_records=1000)
    except Exception as exc:  # noqa: BLE001 - drop broken consumer, retry later
        st.session_state[f"consumer::{cfg['broker']}::{cfg['topic']}"] = None
        return None, f"{type(exc).__name__}: {exc}"[:160]
    for records in batches.values():
        for r in records:
            d = r.value
            if isinstance(d, dict) and all(k in d for k in ("timestamp", "value", "metric_type", "sensor_id")):
                buf.append({k: d.get(k) for k in LIVE_COLUMNS})
    del buf[:-5000]
    return _buffer_df(buf), "connected"


def _buffer_df(buf: List[dict]) -> pd.DataFrame:
    df = pd.DataFrame(buf, columns=LIVE_COLUMNS)
    if df.empty:
        return df
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["timestamp", "value"]).drop_duplicates(
        subset=["timestamp", "sensor_id", "metric_type"])
    return df.sort_values("timestamp").reset_index(drop=True)


def get_live_data(cfg: Dict) -> Tuple[pd.DataFrame, str, str]:
    """Returns (dataframe, mode, note); mode in kafka | mongodb | demo | none."""
    mongo_ok = mongo_available(cfg["uri"], cfg["db"])
    store = get_store(cfg["uri"], cfg["db"]) if mongo_ok else None
    df, note = consume_kafka_data(cfg)

    if df is not None:  # Kafka reachable
        if store is not None and df.empty and not st.session_state.get("seeded"):
            seed = store.query_readings(start=datetime.now(timezone.utc) - timedelta(hours=1), limit=5000)
            st.session_state["live_buffer"] = seed.to_dict("records") if not seed.empty else []
            st.session_state["seeded"] = True
            df = _buffer_df(st.session_state["live_buffer"])
        if df.empty:
            note = "Connected to Kafka - waiting for messages from the producer ..."
        return df, "kafka", note
    if store is not None:
        try:
            df = store.query_readings(start=datetime.now(timezone.utc) - timedelta(hours=2), limit=5000)
            return df, "mongodb", f"Kafka unavailable ({note}). Showing the latest data stored in MongoDB."
        except Exception as exc:  # noqa: BLE001
            note = f"{note}; MongoDB error: {exc}"
    if cfg["demo_ok"]:
        d = demo_dataframe()
        return d[d["timestamp"] >= d["timestamp"].max() - pd.Timedelta(hours=2)], "demo", note
    return pd.DataFrame(columns=LIVE_COLUMNS), "none", note


# =========================================================================== #
# Charts
# =========================================================================== #
def line_chart(df: pd.DataFrame, y: str = "value", title: str = "", unit: str = "", height: int = 380):
    fig = px.line(df, x="timestamp", y=y, color="sensor_id", color_discrete_sequence=PX_COLORS,
                  markers=len(df) < 120, template="plotly_white", title=title)
    fig.update_layout(height=height, hovermode="x unified", margin=dict(l=10, r=10, t=80, b=10),
                      xaxis_title=None, yaxis_title=unit or None, legend_title_text="",
                      title=dict(y=0.95, yanchor="top"),
                      legend=dict(orientation="h", yanchor="bottom", y=1.0, x=0))
    fig.update_traces(line=dict(width=2))
    return fig


def geo_map(df: pd.DataFrame, size_col: Optional[str] = None):
    g = df.dropna(subset=["latitude", "longitude"])
    if g.empty:
        return None
    last = g.sort_values("timestamp").groupby("sensor_id").tail(1)
    trail = g.sort_values("timestamp").groupby("sensor_id").tail(200)
    fig = px.scatter_geo(last, lat="latitude", lon="longitude", hover_name="sensor_id",
                         hover_data={"value": True, "location": True, "latitude": ":.2f", "longitude": ":.2f"},
                         color="sensor_id", color_discrete_sequence=PX_COLORS, projection="natural earth")
    fig.add_trace(go.Scattergeo(lat=trail["latitude"], lon=trail["longitude"], mode="lines",
                                line=dict(width=1, color="rgba(100,116,139,.5)"), showlegend=False,
                                hoverinfo="skip"))
    fig.update_traces(marker=dict(size=12), selector=dict(mode="markers"))
    fig.update_layout(height=360, margin=dict(l=0, r=0, t=10, b=0))
    return fig


# =========================================================================== #
# Live view
# =========================================================================== #
def display_real_time_view(cfg: Dict) -> None:
    @st.fragment(run_every=cfg["interval"] if cfg["auto"] else None)
    def _live():
        with st.spinner("Polling Kafka ..."):
            raw, mode, note = get_live_data(cfg)
        df = to_tz(raw, cfg["tz"])

        badge = {"kafka": pill("● Kafka live", "ok"), "mongodb": pill("● MongoDB fallback", "warn"),
                 "demo": pill("● Demo data (services offline)", "warn"), "none": pill("● No data", "bad")}[mode]
        head_l, head_r = st.columns([3, 2])
        head_l.markdown(badge + pill(f"refresh {cfg['interval']}s" if cfg["auto"] else "refresh paused",
                                     "ok" if cfg["auto"] else "warn"), unsafe_allow_html=True)
        head_r.caption(f"Updated {datetime.now():%H:%M:%S} · {len(df):,} records in view")
        if mode != "kafka" or df.empty:
            st.info(note or "No data yet.")
        if df.empty:
            return

        f1, f2, f3 = st.columns([2, 3, 2])
        metrics = sorted(df["metric_type"].unique())
        metric = f1.selectbox("Metric", metrics, index=metrics.index(next((m for m in ("pm2_5", "price_usd") if m in metrics), metrics[0])),
                              key="live_metric")
        msub = df[df["metric_type"] == metric]
        sensors = sorted(msub["sensor_id"].unique())
        chosen = f2.multiselect("Sensors", sensors, default=sensors[:6], key=f"live_sensors_{metric}")
        window = f3.select_slider("Window", [5, 15, 30, 60, 120], value=30, format_func=lambda m: f"{m} min",
                                  key="live_window")
        cutoff = msub["timestamp"].max() - pd.Timedelta(minutes=window)
        view = msub[(msub["sensor_id"].isin(chosen)) & (msub["timestamp"] >= cutoff)]
        if view.empty:
            st.warning("No records for this selection in the chosen window.")
            return

        unit = view["unit"].dropna().iloc[0] if view["unit"].notna().any() else ""
        cols = st.columns(min(len(chosen), 5) or 1)
        for col, sensor in zip(cols, chosen[:5]):
            s = view[view["sensor_id"] == sensor]["value"]
            if s.empty:
                continue
            delta = (s.iloc[-1] - s.iloc[-2]) if len(s) > 1 else None
            col.metric(sensor, f"{s.iloc[-1]:,.2f} {unit}".strip(),
                       None if delta is None else f"{delta:+,.2f}", border=True)

        norm = st.checkbox("Compare as index (start = 100)", value=exporters.scales_differ(view),
                           key=f"live_norm_{metric}", help="Puts sensors with very different scales on one axis.")
        plot_df, plot_unit = (exporters.normalize(view), "index (start = 100)") if norm else (view, unit)
        st.plotly_chart(line_chart(plot_df, title=f"{metric} - last {window} min", unit=plot_unit), width="stretch")

        fig = geo_map(view)
        if fig is not None:
            with st.expander("🌍 Geo view (latest position + trail)"):
                st.plotly_chart(fig, width="stretch")
        with st.expander("📋 Latest raw records"):
            st.dataframe(view.sort_values("timestamp", ascending=False).head(200), width="stretch",
                         hide_index=True, height=300)

    st.subheader("Live stream")
    _live()


# =========================================================================== #
# Shared filter controls + export
# =========================================================================== #
def filter_controls(prefix: str, cfg: Dict, options: Dict[str, List[str]]) -> Dict:
    c1, c2, c3, c4 = st.columns([2, 2, 2, 2])
    rng = c1.selectbox("Time range", list(RANGES), index=list(RANGES).index("All time"), key=f"{prefix}_range")
    now = datetime.now(timezone.utc)
    start = end = None
    if rng == "Custom":
        today = datetime.now().date()
        picked = c1.date_input("Dates", (today - timedelta(days=1), today), key=f"{prefix}_dates")
        if isinstance(picked, (list, tuple)) and len(picked) == 2:
            tz = pd.Timestamp.now(tz=cfg["tz"]).tz
            start = pd.Timestamp(picked[0]).tz_localize(tz).tz_convert("UTC").to_pydatetime()
            end = (pd.Timestamp(picked[1]).tz_localize(tz) + pd.Timedelta(days=1)).tz_convert("UTC").to_pydatetime()
    elif RANGES[rng] is not None:
        start, end = now - RANGES[rng], now
    metrics = c2.multiselect("Metric", options["metric_type"], key=f"{prefix}_metric",
                             placeholder="All metrics")
    sensors = c3.multiselect("Sensor", options["sensor_id"], key=f"{prefix}_sensor",
                             placeholder="All sensors")
    sources = c4.multiselect("API source", options["source"], key=f"{prefix}_source",
                             placeholder="All sources")
    return {"start": start, "end": end, "metrics": metrics, "sensors": sensors, "sources": sources,
            "label": rng}


def load_options(cfg: Dict, mongo_ok: bool) -> Dict[str, List[str]]:
    if mongo_ok:
        store = get_store(cfg["uri"], cfg["db"])
        return {k: store.distinct(k) for k in ("metric_type", "sensor_id", "source")}
    d = demo_dataframe()
    return {k: sorted(d[k].dropna().unique()) for k in ("metric_type", "sensor_id", "source")}


@st.cache_data(ttl=15, show_spinner="Querying MongoDB ...")
def _cached_query(uri, db, kind, start, end, metrics, sensors, sources, limit):
    store = get_store(uri, db)
    fn = store.query_readings if kind == "readings" else store.query_aggregates
    return fn(start=start, end=end, metrics=list(metrics), sensors=list(sensors), sources=list(sources),
              limit=limit)


def query_historical_data(cfg: Dict, flt: Dict, kind: str = "readings", limit: int = 100_000) -> Tuple[pd.DataFrame, bool]:
    """Returns (df in UTC, is_demo). Falls back to demo data only if the user allowed it."""
    if mongo_available(cfg["uri"], cfg["db"]):
        df = _cached_query(cfg["uri"], cfg["db"], kind, flt["start"], flt["end"], tuple(flt["metrics"]),
                           tuple(flt["sensors"]), tuple(flt["sources"]), limit)
        return df, False
    if not cfg["demo_ok"]:
        return pd.DataFrame(columns=LIVE_COLUMNS), False
    d = demo_dataframe()
    if flt["start"] is not None:
        d = d[(d["timestamp"] >= flt["start"]) & (d["timestamp"] <= (flt["end"] or d["timestamp"].max()))]
    for col, key in (("metric_type", "metrics"), ("sensor_id", "sensors"), ("source", "sources")):
        if flt[key]:
            d = d[d[col].isin(flt[key])]
    return d.reset_index(drop=True), True


def export_panel(df: pd.DataFrame, title: str, flt_desc: List[str], key: str) -> None:
    st.markdown("##### 📤 Export")
    if df.empty:
        st.caption("Nothing to export for the current filters.")
        return
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    c = st.columns(5)
    c[0].download_button("📗 Excel (.xlsx)", data=lambda: exporters.xlsx_bytes(df, title),
                         file_name=f"{key}_{stamp}.xlsx", key=f"{key}_x", width="stretch",
                         mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    c[1].download_button("📕 PDF report", data=lambda: exporters.pdf_bytes(df, title, flt_desc),
                         file_name=f"{key}_{stamp}.pdf", mime="application/pdf", key=f"{key}_p", width="stretch")
    c[2].download_button("🖼 Chart (.png)", data=lambda: exporters.image_bytes(df, title, "png"),
                         file_name=f"{key}_{stamp}.png", mime="image/png", key=f"{key}_i", width="stretch")
    c[3].download_button("🖼 Chart (.jpg)", data=lambda: exporters.image_bytes(df, title, "jpg"),
                         file_name=f"{key}_{stamp}.jpg", mime="image/jpeg", key=f"{key}_j", width="stretch")
    c[4].download_button("📄 CSV", data=lambda: exporters.csv_bytes(df), file_name=f"{key}_{stamp}.csv",
                         mime="text/csv", key=f"{key}_c", width="stretch")


def describe_filters(flt: Dict) -> List[str]:
    out = [f"Range: {flt['label']}"]
    for label, key in (("Metrics", "metrics"), ("Sensors", "sensors"), ("Sources", "sources")):
        if flt[key]:
            out.append(f"{label}: {', '.join(flt[key])}")
    return out


# =========================================================================== #
# History view
# =========================================================================== #
def display_historical_view(cfg: Dict) -> None:
    st.subheader("History - everything collected")
    mongo_ok = mongo_available(cfg["uri"], cfg["db"])
    flt = filter_controls("hist", cfg, load_options(cfg, mongo_ok))
    raw, is_demo = query_historical_data(cfg, flt)
    if is_demo:
        st.warning("MongoDB is not reachable - showing **demo data**.")
    elif not mongo_ok:
        st.error("MongoDB is not reachable and demo data is disabled.")
        return
    df = to_tz(raw, cfg["tz"])
    if df.empty:
        st.info("No records match these filters. Run the producer + Spark job, or widen the filters.")
        return

    stats = get_store(cfg["uri"], cfg["db"]).stats() if mongo_ok else None
    k = st.columns(4)
    k[0].metric("Records (filtered)", f"{len(df):,}", border=True)
    k[1].metric("Records in database", f"{stats['total']:,}" if stats else "demo", border=True)
    k[2].metric("Sensors", df["sensor_id"].nunique(), border=True)
    span = df["timestamp"].max() - df["timestamp"].min()
    hours, rem = divmod(int(span.total_seconds()), 3600)
    k[3].metric("Time span", f"{hours // 24}d {hours % 24}h {rem // 60}m" if hours >= 24 else f"{hours}h {rem // 60}m",
                border=True)

    opts = sorted(df["metric_type"].unique())
    metric = st.selectbox("Chart metric", opts, index=opts.index(next((m for m in ("pm2_5", "price_usd") if m in opts), opts[0])),
                          key="hist_chart_metric")
    sub = df[df["metric_type"] == metric]
    if len(sub) > 6000:  # keep the browser fast; table + export still hold everything
        sub = sub.iloc[:: int(np.ceil(len(sub) / 6000))]
        st.caption("Chart downsampled to ~6,000 points for speed - the table and exports contain all rows.")
    unit = sub["unit"].dropna().iloc[0] if sub["unit"].notna().any() else ""
    norm = st.checkbox("Compare as index (start = 100)", value=exporters.scales_differ(sub),
                       key=f"hist_norm_{metric}", help="Puts sensors with very different scales on one axis.")
    plot_df, plot_unit = (exporters.normalize(sub), "index (start = 100)") if norm else (sub, unit)
    st.plotly_chart(line_chart(plot_df, title=f"{metric} history", unit=plot_unit), width="stretch")

    left, right = st.columns(2)
    box = px.box(sub, x="sensor_id", y="value", color="sensor_id", color_discrete_sequence=PX_COLORS,
                 template="plotly_white", title="Distribution per sensor")
    box.update_layout(showlegend=False, height=320, margin=dict(l=10, r=10, t=50, b=10), xaxis_title=None)
    left.plotly_chart(box, width="stretch")
    right.markdown("**Summary statistics**")
    right.dataframe(exporters.summarize(df), width="stretch", hide_index=True, height=300)

    st.markdown("##### 🗂 All records")
    st.dataframe(df.sort_values("timestamp", ascending=False), width="stretch", hide_index=True, height=380,
                 column_config={"timestamp": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm:ss"),
                                "value": st.column_config.NumberColumn(format="%.4f")})
    export_panel(df, f"History export - {metric}", describe_filters(flt), "history")


# =========================================================================== #
# Aggregation view
# =========================================================================== #
def display_aggregation_view(cfg: Dict) -> None:
    st.subheader("Aggregation")
    mongo_ok = mongo_available(cfg["uri"], cfg["db"])
    mode = st.radio("Aggregation source", ["⚡ Spark windows (from MongoDB)", "🧮 Interactive roll-up"],
                    horizontal=True, label_visibility="collapsed")
    flt = filter_controls("agg", cfg, load_options(cfg, mongo_ok))

    if mode.startswith("⚡"):
        agg, is_demo = query_historical_data(cfg, flt, kind="aggregates", limit=50_000)
        if is_demo:
            agg = rollup(agg, "1min")  # demo fallback: emulate the Spark output
            st.warning("MongoDB is not reachable - showing demo data aggregated in 1-minute windows.")
        st.caption(f"Computed continuously by Spark Structured Streaming (window = {config.SPARK_WINDOW}, "
                   f"watermark = {config.SPARK_WATERMARK}).")
    else:
        freq_label = st.select_slider("Bucket size", list(ROLLUPS), value="5 minutes")
        raw, is_demo = query_historical_data(cfg, flt)
        if is_demo:
            st.warning("MongoDB is not reachable - showing demo data.")
        raw = raw if raw.empty else raw.assign(timestamp=pd.to_datetime(raw["timestamp"], utc=True))
        agg = rollup(raw, ROLLUPS[freq_label])

    agg = to_tz(agg, cfg["tz"])
    if agg.empty:
        st.info("No aggregates for these filters yet. Make sure the Spark job is running, or widen the filters.")
        return
    agg = agg.copy()
    for c in ("avg_value", "min_value", "max_value", "stddev_value"):
        agg[c] = pd.to_numeric(agg[c], errors="coerce").round(4)

    metric = st.selectbox("Chart metric", sorted(agg["metric_type"].unique()), key="agg_chart_metric")
    sub = agg[agg["metric_type"] == metric].rename(columns={"window_start": "timestamp"})
    stat = st.radio("Statistic", ["avg_value", "min_value", "max_value", "stddev_value", "count"],
                    horizontal=True, key="agg_stat")
    fig = line_chart(sub, y=stat, title=f"{metric} - {stat}")
    if sub["sensor_id"].nunique() == 1 and stat == "avg_value":
        fig.add_trace(go.Scatter(x=pd.concat([sub["timestamp"], sub["timestamp"][::-1]]),
                                 y=pd.concat([sub["max_value"], sub["min_value"][::-1]]), fill="toself",
                                 fillcolor="rgba(37,99,235,.12)", line=dict(width=0), hoverinfo="skip",
                                 name="min-max range"))
    st.plotly_chart(fig, width="stretch")

    st.dataframe(agg.sort_values("window_start", ascending=False), width="stretch", hide_index=True, height=340)

    # export uses the same schema as raw data so it works with the shared exporters
    exp = agg.rename(columns={"window_start": "timestamp", "avg_value": "value"})[
        ["timestamp", "sensor_id", "metric_type", "source", "unit", "value", "min_value", "max_value",
         "stddev_value", "count"]]
    export_panel(exp, f"Aggregation export - {metric}", describe_filters(flt), "aggregates")


# =========================================================================== #
# About
# =========================================================================== #
def display_about() -> None:
    st.subheader("About this project")
    st.graphviz_chart(
        """digraph G { rankdir=LR; node [shape=box, style="rounded,filled", fillcolor="#EFF6FF",
        color="#2563EB", fontname="Helvetica"]; edge [color="#64748B"];
        API [label="Open-Meteo API\\n(air quality)"];
        P [label="Producer\\n(producer.py)"]; K [label="Kafka\\ntopic"];
        S [label="Spark Structured\\nStreaming"]; M [label="MongoDB\\nreadings + aggregates"];
        D [label="Streamlit\\ndashboard"];
        API -> P [label=" HTTP/JSON"]; P -> K; K -> S; K -> D [label=" live", style=dashed];
        S -> M; M -> D [label=" history"]; }"""
    )
    st.markdown(
        """
**Pipeline** - A producer polls a free public API (every 60 s by default) and publishes JSON messages to **Kafka**.
**Spark Structured Streaming** validates the messages, stores every record in **MongoDB** and computes
windowed aggregates (avg / min / max / std / count). **Streamlit** reads Kafka directly for the live view
and MongoDB for history and aggregation.

**Data sources** (switch with `DATA_SOURCE` in `.env`):
        """
    )
    from sources import SOURCES

    st.table(pd.DataFrame([{"name": n, "description": c.description} for n, c in SOURCES.items()]))


# =========================================================================== #
# Main
# =========================================================================== #
def main() -> None:
    cfg = setup_sidebar()
    mongo_ok = mongo_available(cfg["uri"], cfg["db"])
    st.sidebar.markdown("---")
    st.sidebar.markdown(pill("MongoDB ✓" if mongo_ok else "MongoDB ✗", "ok" if mongo_ok else "bad"),
                        unsafe_allow_html=True)
    st.sidebar.caption("Kafka status is shown on the Live tab.")

    st.title("📡 Real-Time Stream Monitor")
    t1, t2, t3, t4 = st.tabs(["🔴 Live", "🗂 History", "📐 Aggregation", "ℹ️ About"])
    with t1:
        display_real_time_view(cfg)
    with t2:
        display_historical_view(cfg)
    with t3:
        display_aggregation_view(cfg)
    with t4:
        display_about()


if __name__ == "__main__":
    main()
