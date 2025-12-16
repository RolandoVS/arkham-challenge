### Arkham Challenge — Nuclear Outages Pipeline

End-to-end mini data pipeline:

- **Extract** nuclear outage data from the EIA Open Data API (v2)
- **Store** raw data in **Parquet**
- **Model** into a simple **star schema** (2 dims + 1 fact)
- **Serve** data via a minimal **FastAPI** service

Reference: `https://www.eia.gov/opendata/`

---

### Quick start

#### Install + activate

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

#### Create `.env`

Create a `.env` file in the repo root:

```dotenv
# Required for connector + /refresh
EIA_API_KEY=YOUR_EIA_KEY_HERE

# Optional: enable API auth
API_TOKEN=my-secret-token

# Optional: override defaults
RAW_PATH=raw_data.parquet
MODELED_DIR=modeled
OUTPUT_FILE=raw_data.parquet

# Optional: connector behavior
INCREMENTAL=1
EARLY_STOP_ON_OLD_PERIOD=1

# Optional: connector logging to file (still logs to console)
LOG_FILE=logs/connector.log
LOG_FILE_MODE=a
```

---

### Part 1 — Connector (raw extract → `raw_data.parquet`)

Run the connector:

```bash
python connector.py
```

#### Incremental extraction (bonus)

When `INCREMENTAL=1` and `OUTPUT_FILE` already exists, the connector:
- reads the existing Parquet
- de-dupes on `(period, facility, generator)`
- appends only new rows
- optionally stops paging early when it’s past the newest `period` already stored

```bash
INCREMENTAL=1 python connector.py
```

If you want to disable the early-stop optimization:

```bash
INCREMENTAL=1 EARLY_STOP_ON_OLD_PERIOD=0 python connector.py
```

#### Logs to file

```bash
LOG_FILE=logs/connector.log LOG_FILE_MODE=a python connector.py
```

---

### Part 2 — Data model (raw → modeled star schema)

Build modeled tables from `raw_data.parquet`:

```bash
python data_model.py
```

Print debugging previews:

```bash
python data_model.py --print-raw --print-modeled --head 20
```

<img width="1297" height="1076" alt="Screenshot 2025-12-16 at 12 11 50 a m" src="https://github.com/user-attachments/assets/1b6af33b-bd74-49d2-920c-5227dfcc5f9c" />

Outputs written to `modeled/`:
- `modeled/dim_plant.parquet`
- `modeled/dim_date.parquet`
- `modeled/fact_outage.parquet`

#### ER diagram

The ER diagram is committed as **`er.png`**.

<img width="619" height="759" alt="er" src="https://github.com/user-attachments/assets/2d66d8e5-2334-4832-b380-539c585a8f1d" />

---

### Part 3 — API (FastAPI)

Start the API (loads `.env`):

```bash
python -m uvicorn api:app --reload --env-file .env --log-level info
```

Open Swagger UI:
- `http://127.0.0.1:8000/docs`

#### Authentication (bonus)

If `API_TOKEN` is set, both endpoints require:

- Header: `Authorization: Bearer <API_TOKEN>`

Example:

```bash
curl -H "Authorization: Bearer $API_TOKEN" "http://127.0.0.1:8000/data?page=1&limit=50"
```

If you hit “Invalid token” while `.env` is correct, an already-exported `API_TOKEN` may be overriding `.env`.
Start the server like this to force `.env` to win:

```bash
env -u API_TOKEN python -m uvicorn api:app --reload --env-file .env --log-level info
```

#### Endpoints

##### `POST /refresh`

Triggers: **connector extraction** → **rebuild modeled tables** (atomic swap) → cache invalidation.

```bash
curl -X POST -H "Authorization: Bearer $API_TOKEN" "http://127.0.0.1:8000/refresh"
```

Optional debug preview:

```bash
curl -X POST -H "Authorization: Bearer $API_TOKEN" \
  "http://127.0.0.1:8000/refresh?preview=true&head=5" | python -m json.tool
```

##### `GET /data`

Returns outage data joined with dimensions, with filters + pagination.

Query params:
- `page` (default 1)
- `limit` (default 100, max 1000)
- `start_date`, `end_date` (filter by outage start date, `YYYY-MM-DD`)
- `facility_id`
- `generator`
- `plant_key`
- `plant_name` (case-insensitive substring match)

Examples:

```bash
curl -H "Authorization: Bearer $API_TOKEN" \
  "http://127.0.0.1:8000/data?page=1&limit=25"

curl -H "Authorization: Bearer $API_TOKEN" \
  "http://127.0.0.1:8000/data?facility_id=1715"

curl -H "Authorization: Bearer $API_TOKEN" \
  "http://127.0.0.1:8000/data?start_date=2025-01-01&end_date=2025-12-31"
```

---

### Basic analytics examples (rubric)

These examples show how the modeled tables answer real questions.

#### 1) Plants with the longest total outage hours

```bash
python - <<'PY'
import pandas as pd

dim_plant = pd.read_parquet("modeled/dim_plant.parquet")
fact = pd.read_parquet("modeled/fact_outage.parquet")

top = (
    fact.merge(dim_plant, on="PlantKey", how="left")
    .groupby(["EIA_FacilityID", "PlantName"], as_index=False)["OutageDurationHours"]
    .sum()
    .sort_values("OutageDurationHours", ascending=False)
    .head(10)
)
print(top)
PY
```

#### 2) Outage counts by month (based on outage start date)

```bash
python - <<'PY'
import pandas as pd

dim_date = pd.read_parquet("modeled/dim_date.parquet")
fact = pd.read_parquet("modeled/fact_outage.parquet")

df = fact.merge(dim_date[["DateKey", "Year", "Month"]], on="DateKey", how="left")
counts = df.groupby(["Year", "Month"]).size().reset_index(name="OutageEvents")
print(counts.sort_values(["Year", "Month"]).tail(24))
PY
```

---

### Assumptions / decisions

- **Outage “events” derived from daily rows**: raw data is daily; consecutive daily rows for the same `(facility, generator)` are collapsed into one outage event.
- **Exclusive end timestamp**: `OutageEndTimestamp = last_date + 1 day` (end is exclusive).
- **Minimal schema**: modeled tables only include fields derivable from the raw feed.
- **Incremental extraction**: de-dupes on `(period, facility, generator)`.
- **Incremental early-stop**: assumes the API returns newest `period` first; disable with `EARLY_STOP_ON_OLD_PERIOD=0` if needed.
- **API efficiency**: `/data` caches the joined view in memory and invalidates on refresh; refresh writes modeled data to a temp dir and atomically swaps it in.

---

### Tests

```bash
pytest -q
```

---

### Repo map (high-level)

- `connector.py`: runnable connector script (shim)
- `arkham_connector/`: connector implementation (client/settings/incremental/runner)
- `data_model.py`: star-schema builder
- `modeled/`: modeled Parquets (generated)
- `api.py`: API entrypoint shim (`uvicorn api:app`)
- `arkham_api/`: API implementation (auth/storage/query/refresh/main)
- `tests/`: unit/integration tests
- `er.png`: ER diagram image


