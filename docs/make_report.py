"""Generates docs/Activity_Report.pdf (draft). Edit the CONFIG block, then:  python docs/make_report.py"""
import io
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch
from reportlab.lib import colors
from reportlab.lib.enums import TA_JUSTIFY
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (Image, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table,
                                TableStyle)

# ------------------------------ CONFIG -------------------------------------
STUDENT = "Your Name"
COURSE = "Course code - Big Data"
SECTION = "Section"
DATE = "October 2026"
TITLE = "Big Data Application Pipeline: Real-Time Stream Monitor"
OUT = os.path.join(os.path.dirname(__file__), "Activity_Report.pdf")
# ---------------------------------------------------------------------------

NAVY = colors.HexColor("#1E3A8A")
ss = getSampleStyleSheet()
body = ParagraphStyle("body", parent=ss["BodyText"], alignment=TA_JUSTIFY, leading=14, fontSize=10)
h1 = ParagraphStyle("h1", parent=ss["Heading1"], textColor=NAVY, fontSize=15, spaceBefore=14, keepWithNext=1)
h2 = ParagraphStyle("h2", parent=ss["Heading2"], textColor=NAVY, fontSize=12, keepWithNext=1)
cap = ParagraphStyle("cap", parent=ss["Italic"], fontSize=8.5, textColor=colors.grey)
code = ParagraphStyle("code", parent=ss["Code"], fontSize=7.5, leading=9.5, backColor=colors.HexColor("#F1F5F9"),
                      borderPadding=5)


def P(t, s=body):
    return Paragraph(t, s)


def bullets(items):
    return [Paragraph("&bull; " + i, body) for i in items]


def table(rows, widths, header=True):
    t = Table([[Paragraph(str(c), ParagraphStyle("c", parent=body, fontSize=8.5, leading=10.5,
                                                 alignment=0, textColor=colors.white if (header and r == 0) else colors.black))
                for c in row] for r, row in enumerate(rows)], colWidths=widths, repeatRows=1 if header else 0)
    st = [("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#CBD5E1")), ("VALIGN", (0, 0), (-1, -1), "TOP"),
          ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F1F5F9")])]
    if header:
        st.append(("BACKGROUND", (0, 0), (-1, 0), NAVY))
    t.setStyle(TableStyle(st))
    return t


def placeholder(text):
    t = Table([[Paragraph(f"<b>[Insert screenshot]</b><br/>{text}", ParagraphStyle(
        "ph", parent=body, alignment=1, textColor=colors.grey))]], colWidths=[16.5 * cm], rowHeights=[4.2 * cm])
    t.setStyle(TableStyle([("BOX", (0, 0), (-1, -1), 1, colors.HexColor("#94A3B8")),
                           ("VALIGN", (0, 0), (-1, -1), "MIDDLE")]))
    return t


def architecture_png():
    fig, ax = plt.subplots(figsize=(11, 3.2))
    ax.set_xlim(0, 110); ax.set_ylim(0, 32); ax.axis("off")
    boxes = [(2, "Public APIs\nCoinGecko · ISS\nOpen-Meteo · USGS", "#DBEAFE"), (24, "Kafka Producer\nproducer.py\n(every 15 s)", "#DBEAFE"),
             (46, "Apache Kafka\ntopic\nstreaming-data", "#FEF3C7"), (68, "Spark Structured\nStreaming\nspark_job.py", "#FFEDD5"),
             (90, "MongoDB\nreadings +\naggregates", "#DCFCE7")]
    for x, label, fc in boxes:
        ax.add_patch(FancyBboxPatch((x, 12), 18, 14, boxstyle="round,pad=0.3,rounding_size=1.5", fc=fc, ec="#1E3A8A", lw=1.2))
        ax.text(x + 9, 19, label, ha="center", va="center", fontsize=8.5)
    for a, b in zip([20, 42, 64, 86], [24, 46, 68, 90]):
        ax.add_patch(FancyArrowPatch((a + 0.6, 19), (b - 0.6, 19), arrowstyle="-|>", mutation_scale=12, color="#475569"))
    ax.add_patch(FancyBboxPatch((46, 0.5), 52, 6, boxstyle="round,pad=0.3,rounding_size=1.5", fc="#EDE9FE", ec="#1E3A8A", lw=1.2))
    ax.text(72, 3.5, "Streamlit dashboard  (Live · History · Aggregation · Export)", ha="center", va="center", fontsize=9)
    ax.add_patch(FancyArrowPatch((55, 11.5), (55, 7.4), arrowstyle="-|>", mutation_scale=12, color="#475569", ls="--"))
    ax.text(56.5, 9.4, "live (direct)", fontsize=7.5, color="#475569")
    ax.add_patch(FancyArrowPatch((99, 11.5), (92, 7.4), arrowstyle="-|>", mutation_scale=12, color="#475569"))
    ax.text(97.5, 9, "history", fontsize=7.5, color="#475569")
    buf = io.BytesIO(); fig.savefig(buf, format="png", dpi=170, bbox_inches="tight"); plt.close(fig)
    return buf.getvalue()


def footer(c, d):
    c.saveState(); c.setFont("Helvetica", 8); c.setFillColor(colors.grey)
    c.drawString(2 * cm, 1.1 * cm, f"{TITLE}"); c.drawRightString(A4[0] - 2 * cm, 1.1 * cm, f"Page {d.page}"); c.restoreState()


s = []
s += [P(TITLE, ParagraphStyle("t", parent=ss["Title"], textColor=NAVY, fontSize=20)),
      P("<b>Activity Report</b>", ParagraphStyle("st", parent=body, alignment=1, fontSize=12)), Spacer(1, 6),
      table([["Name", STUDENT], ["Course / Section", f"{COURSE} / {SECTION}"], ["Date", DATE],
             ["Repository", "https://github.com/&lt;your-username&gt;/big-data-streamlit-template"]], [4 * cm, 12.5 * cm], header=False)]

s += [P("1. Objective", h1),
      P("To design and implement an end-to-end big-data streaming application that ingests live data from a public "
        "Application Programming Interface (API), moves it through a <b>MongoDB-Kafka-Spark</b> pipeline, and presents it in a "
        "live dashboard (refreshing every 15 seconds) together with a complete history, data aggregation and formatted "
        "data export.")]

s += [P("2. API Used", h1),
      P("The producer polls free, key-less public APIs. Two are enabled by default and two more can be switched on with a single "
        "setting (<font face='Courier'>DATA_SOURCE</font>) because every API is wrapped by the same adapter interface."),
      table([["API", "Endpoint", "Data collected", "Notes"],
             ["CoinGecko (default)", "api.coingecko.com/api/v3/simple/price", "USD & PHP price and 24 h change for Bitcoin, Ethereum, Solana, Dogecoin, XRP",
              "No key; free tier is rate-limited; prices refresh roughly every 30-60 s"],
             ["wheretheiss.at (default)", "api.wheretheiss.at/v1/satellites/25544", "ISS latitude, longitude, altitude (km), velocity (km/h)",
              "No key; ~1 request/s limit; changes every second"],
             ["Open-Meteo Air Quality (optional)", "air-quality-api.open-meteo.com/v1/air-quality", "PM2.5, PM10, US AQI, O3, NO2, CO for 5 Philippine cities",
              "No key; hourly model data"],
             ["USGS Earthquake (optional)", "earthquake.usgs.gov/fdsnws/event/1/query", "Magnitude and depth of earthquakes around the Philippines",
              "No key; event-driven, de-duplicated by event id"]], [3.3 * cm, 4.6 * cm, 5 * cm, 3.6 * cm]),
      Spacer(1, 6),
      P("Every API response is normalised into one schema: <font face='Courier'>timestamp</font> (ISO-8601 UTC), "
        "<font face='Courier'>value</font> (numeric), <font face='Courier'>metric_type</font>, <font face='Courier'>sensor_id</font>, plus "
        "<font face='Courier'>record_id</font>, <font face='Courier'>source</font>, <font face='Courier'>unit</font>, "
        "<font face='Courier'>location</font>, <font face='Courier'>latitude/longitude</font>. This lets the downstream Spark job and "
        "dashboard stay generic - changing the API never requires changing them.")]

s += [P("3. Pipeline Implemented: MongoDB - Kafka - Spark", h1),
      Image(io.BytesIO(architecture_png()), width=17 * cm, height=17 * cm * 0.34),
      P("Figure 1. System architecture.", cap),
      table([["Stage", "Technology / file", "Role"],
             ["Ingest", "Python producer (producer.py, sources.py)", "Polls the API every 15 s with retry/back-off; a failing API never stops the others; publishes JSON keyed by sensor_id."],
             ["Buffer", "Apache Kafka (topic streaming-data)", "Durable, ordered, decoupled message log; lets the dashboard and Spark consume independently."],
             ["Process", "Spark Structured Streaming (spark_job.py)", "Parses and validates JSON, drops malformed records, writes each valid record to MongoDB, and computes 1-minute tumbling-window aggregates (avg, min, max, std, count) with a 2-minute watermark."],
             ["Store", "MongoDB (storage.py)", "Collections <i>readings</i> (raw history) and <i>aggregates</i> (Spark windows). Upserts keyed by record_id make replays idempotent; compound indexes speed time-range queries."],
             ["Serve", "Streamlit + Plotly (app.py)", "Live view reads Kafka directly; History and Aggregation views query MongoDB; exports via exporters.py."]],
            [2.2 * cm, 4.8 * cm, 9.5 * cm]),
      Spacer(1, 6),
      P("<b>Why this combination?</b> Kafka absorbs bursts and decouples producers from consumers; Spark gives scalable, "
        "fault-tolerant stream processing with event-time windows and checkpointing; MongoDB's flexible documents and time indexes "
        "suit semi-structured sensor/API data. Together they scale from this single-machine demo to many partitions and workers "
        "without redesigning the application.")]

s += [P("4. Dashboard Features", h1),
      *bullets(["<b>Live view</b> - auto-refreshes every 15 s (adjustable, pausable) using Streamlit fragments so only the live panel reloads; per-sensor KPI cards with change since the last reading; trend chart; geographic map with trail for position data; automatic fall-back to MongoDB if Kafka is unreachable.",
                "<b>History view</b> - presents <i>all</i> collected records with filters for time range, metric, sensor and API source; summary statistics; distribution plot; full sortable table.",
                "<b>Aggregation (additional points)</b> - Spark window aggregates from MongoDB, plus an interactive roll-up from 1 minute to 1 day with avg/min/max/std/count and a min-max band.",
                "<b>UI/UX (additional points)</b> - status badges for data mode, KPI cards, index-to-100 comparison when sensors have very different scales (e.g. BTC vs SOL), time-zone selector (default Asia/Manila), responsive layout, clear banners when demo data is shown.",
                "<b>Export (additional points)</b> - Excel (.xlsx) with formatted header, filters, summary sheet and native chart; PDF report with statistics, chart and latest rows; PNG and JPG chart images; CSV."]),
      Spacer(1, 4), placeholder("Live tab with KPI cards and trend chart"), Spacer(1, 4),
      placeholder("History tab with filters, summary and full table"), Spacer(1, 4),
      placeholder("Aggregation tab (Spark windows) and Export buttons")]

s += [P("5. Significance of the Data Dashboard", h1),
      P("A streaming dashboard turns a continuous flow of raw API responses into information people can act on while it is still "
        "relevant. The <b>live view</b> shortens the time between an event (a price move, an earthquake, a spike in PM2.5) and a decision. "
        "The <b>history view</b> preserves every record so trends, anomalies and seasonality can be examined later - something the "
        "source APIs themselves usually do not offer for free. <b>Aggregation</b> reduces noisy high-frequency data to meaningful "
        "statistics, and <b>export</b> lets results be shared with people who do not use the dashboard. "
        "Because the pipeline is source-agnostic, the same architecture can monitor air quality for public health, seismic activity for disaster "
        "preparedness, or market data for financial analysis - demonstrating why big-data tooling (Kafka, Spark, MongoDB) is used for "
        "high-volume, high-velocity data instead of a single script writing to a file.")]

s += [P("6. Testing", h1),
      P("Automated tests (<font face='Courier'>pytest</font>) cover: API adapters with mocked responses (schema, missing values, bad payloads, de-duplication); "
        "the MongoDB layer (idempotent upserts, filtered queries, aggregates); Spark parsing and window aggregation on a local Spark session; "
        "the producer's error isolation and Kafka keying; all five export formats (including empty data); and headless rendering of the dashboard "
        "with and without a database."),
      P("<i>Add here the results of your own end-to-end run (screenshots of the Kafka producer log, Spark batch log and MongoDB collection counts).</i>"),
      placeholder("Terminal: producer sending records / Spark batch logs / MongoDB document count")]

s += [P("7. Limitations and Future Work", h1),
      *bullets(["Free APIs update at their own pace (e.g. CoinGecko about every 30-60 s), so consecutive 15 s polls can repeat a value.",
                "Spark writes each micro-batch to MongoDB through the driver; for much larger volumes the MongoDB Spark Connector should be used.",
                "Late data beyond the 2-minute watermark is stored in <i>readings</i> but excluded from window aggregates.",
                "Future work: anomaly detection / alerting, multi-broker Kafka, authentication on the dashboard."])]

s += [P("8. Conclusion", h1),
      P("The project delivers a working MongoDB-Kafka-Spark pipeline fed by real public APIs, with a 15-second live dashboard, a complete "
        "history, Spark-based aggregation and multi-format export. The modular design (adapter-based sources, schema-first messages, "
        "idempotent storage) makes it easy to extend and to explain.")]

s += [P("References", h1),
      *bullets(["CoinGecko API documentation - https://docs.coingecko.com/reference/simple-price",
                "wheretheiss.at API - https://wheretheiss.at/w/developer",
                "Open-Meteo Air Quality API - https://open-meteo.com/en/docs/air-quality-api",
                "USGS Earthquake Catalog API - https://earthquake.usgs.gov/fdsnws/event/1/",
                "Apache Spark Structured Streaming + Kafka Integration Guide - https://spark.apache.org/docs/latest/streaming/",
                "Apache Kafka documentation - https://kafka.apache.org/documentation/",
                "MongoDB documentation - https://www.mongodb.com/docs/"])]

SimpleDocTemplate(OUT, pagesize=A4, leftMargin=2 * cm, rightMargin=2 * cm, topMargin=1.8 * cm, bottomMargin=2 * cm,
                  title=TITLE, author=STUDENT).build(s, onFirstPage=footer, onLaterPages=footer)
print("wrote", OUT)
