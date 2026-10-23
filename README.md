# 📡 Real-Time Stream Monitor — Kafka · Spark · MongoDB · Streamlit

A big-data streaming application that pulls **live data from public APIs**, pushes it through
**Kafka**, processes it with **Spark Structured Streaming**, stores it in **MongoDB**, and shows it on a
**Streamlit** dashboard (live view that refreshes every 15 s, full history, aggregation, and export).

```
 Open-Meteo API ──► Producer ──► Kafka ──┬──► Spark Structured Streaming ──► MongoDB ──┐
 (air quality)     (producer.py)       │   (validate · store · window aggregates)    │ history +
                                        └────────────── live (direct consumer) ───────┴─► Streamlit
```

## Pipeline chosen: **MongoDB – Kafka – Spark**

| Stage | File | What it does |
|---|---|---|
| Ingest | `sources.py`, `producer.py` | Calls the API every 60 s, builds schema-conformant records, publishes JSON to Kafka (keyed by `sensor_id`) |
| Buffer | Kafka topic `streaming-data` | Decouples producer from consumers; ordered per sensor |
| Process | `spark_job.py` | Parses/validates JSON, upserts every record into `readings`, computes **5-minute window aggregates** (avg/min/max/std/count) into `aggregates` |
| Store | `storage.py`, MongoDB | Idempotent upserts (`_id = record_id`), indexed time-series queries |
| Serve | `app.py`, `exporters.py` | Live / History / Aggregation / About tabs, Excel · PDF · PNG · JPG · CSV export |

### Message schema
`timestamp` (ISO-8601 UTC) · `value` (number) · `metric_type` · `sensor_id` — plus `record_id`, `source`, `unit`, `location`, `latitude`, `longitude`, `source_timestamp`.

## Data sources (all free, **no API key**)

The project uses **Open-Meteo Air Quality (`airquality`)** by default. The other adapters are optional; set `DATA_SOURCE` in `.env` (a comma-separated list works):

| `DATA_SOURCE` | API | Metrics | Why it's interesting |
|---|---|---|---|
| `crypto` | [CoinGecko](https://docs.coingecko.com/reference/simple-price) | `price_usd`, `price_php`, `change_24h_pct` for 5 coins | Market monitoring, volatility |
| `iss` | [wheretheiss.at](https://wheretheiss.at/w/developer) | `altitude_km`, `velocity_kmh` + lat/lon | Changes every second → very "live"; geo-map |
| `airquality` *(default, the API used)* | [Open-Meteo Air Quality](https://open-meteo.com/en/docs/air-quality-api) | PM2.5, PM10, US AQI, O₃, NO₂, CO for Manila, Quezon City, Cebu, Davao, Baguio | Public-health relevance |
| `earthquake` | [USGS Earthquake API](https://earthquake.usgs.gov/fdsnws/event/1/) | magnitude, depth for events around the Philippines | Event-driven data; disaster awareness |

Other no-key APIs that would fit the same adapter pattern: Binance public tickers, Open-Meteo weather, NASA EONET, Frankfurter FX rates, OpenSky flights.
To add one: subclass `DataSource` in `sources.py` and register it in `SOURCES`.

## Setup

**Prerequisites:** Python 3.10+, Java 11/17/21 (for Spark), Docker (for Kafka + MongoDB).

```bash
conda create -n bigdata python=3.10.13 && conda activate bigdata
pip install -r requirements.txt
cp .env.example .env          # optional – defaults work for local dev
docker compose up -d          # Kafka (KRaft) on :9092, MongoDB on :27017
```

## Run (4 terminals)

```bash
python producer.py            # 1. API → Kafka   (every 60 s)
python spark_job.py           # 2. Kafka → Spark → MongoDB   (first run downloads the Kafka connector)
streamlit run app.py          # 3. dashboard at http://localhost:8501
```

Useful flags: `python producer.py --source airquality,earthquake --interval 30` ·
`python spark_job.py --starting-offsets earliest --window "10 minutes"`

> The dashboard runs even with nothing started: a sidebar toggle ("Use demo data when offline") shows
> synthetic data so you can explore the UI. A banner always tells you when you are *not* looking at real data.

## Dashboard

* **🔴 Live** – consumes Kafka directly, auto-refresh every 15 s (adjustable 5–60 s, pausable), per-sensor KPI cards with deltas, trend chart, geo-map with trail, raw records. If Kafka is down it falls back to the latest data in MongoDB.
* **🗂 History** – *all* stored records with filters (time range, metric, sensor, API source), summary statistics, distribution plot, full table.
* **📐 Aggregation** – Spark window aggregates from MongoDB, or an interactive roll-up (1 min → 1 day) of the raw data; avg/min/max/std/count.
* **📤 Export** – Excel (formatted, with summary + native chart), PDF report, PNG/JPG chart, CSV.
* Time-zone selector (default Asia/Manila); data is always stored in UTC.

## Tests

```bash
pytest -q        # API adapters, Mongo layer, exports, Spark transforms (local Spark), producer, UI smoke tests
```
The Spark test starts a small local Spark session (needs Java). Tests use `mongomock`, so no database is required.

## Troubleshooting

| Symptom | Fix |
|---|---|
| Live tab says *cannot reach Kafka* | `docker compose up -d`; check `KAFKA_BOOTSTRAP` |
| *topic not found yet* | start `producer.py` once (topic is auto-created) |
| Spark can't download Kafka connector | needs internet on first run (`spark.jars.packages`) |
| `airquality` returns HTTP 429 | Open-Meteo free tier allows ~10,000 calls/day – keep `--interval` at 60 s or more |
| Windows + Spark errors | run Spark job inside WSL2 |
| History empty but producer runs | the Spark job must be running – it is what writes to MongoDB |

## Notes & limits
* Spark's foreachBatch writer collects each micro-batch to the driver before bulk-writing to MongoDB — fine for API-scale data (tens of records per batch); for very high volume switch to the MongoDB Spark Connector.
* Window aggregates use event-time with a 10-minute watermark; records arriving later than that still land in `readings` but not in `aggregates`.
* Open-Meteo air-quality values are model data that update about hourly, so consecutive polls often return the same numbers; the dashboard still refreshes every 15 s and shows new values as soon as they arrive.
