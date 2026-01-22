# 🌱🌍♻️ GreenDIGIT KPI Calculation Service — CI → CFP

FastAPI microservice that retrieves Location (lat/lon) from [GOC DB](https://goc.egi.eu), and **Carbon Intensity (CI)** from [wattprint](https://api.wattprint.eu/). It derives **Effective CI** and **Carbon Footprint (CFP)**. More KPIs to be included in the future.

## How the calculation works

$$
\mathrm{CI}_{\mathrm{eff}} = \mathrm{CI} \times \mathrm{PUE}
$$

$$
\mathrm{CFP}\;[\mathrm{gCO_2e}] = \mathrm{CI}_{\mathrm{eff}}\;[\mathrm{gCO_2e/kWh}] \times E\;[\mathrm{kWh}]
$$

Also in kg: 

$$ 
\mathrm{CFP}_{kg} = \mathrm{CFP}/1000 
$$

## Folder structure
```text
greendigit-cigrid/
├─ kpi_service/
│  ├─ app/
│  │  ├─ main.py
│  │  └─ requirements.txt
│  └─ Dockerfile
├─ data/
│  ├─ sites_raw.json
│  └─ sites_latlngpue.json
└─ README.md
```

## Data prerequisites (GOC DB → Sites catalogue)

### Data prerequisites (GOC DB → Sites catalogue)

This service expects a sites catalogue with coordinates (and optional PUE), typically produced by two helper services:

1. **goc_db_fetch_service** → fetches grid sites from GOC DB and writes a raw JSON without coordinates/PUE.
   - Example:
     ```bash
     python fetch_goc_db.py --format json --output sites_raw.json
     ```

2. **gocdb_postprocess** → enriches the raw list with realistic coordinates and a static PUE (e.g., 1.4) to produce `sites_enriched.json`.
   - Example:
     ```bash
     python gocdb_postprocess.py sites_raw.json data/sites_enriched.json
     ```

The resulting file shape (used by this CI service via `SITES_JSON`):
```json
[
  {"site_name":"CERN-PROD","latitude":46.2331,"longitude":6.0559,"pue":1.4},
  {"site_name":"RAL-LCG2","latitude":51.5714,"longitude":-1.3080,"pue":1.4}
]
```

### Sites catalogue handling & cache
- Source file: set `SITES_JSON` (default `/data/sites_latlngpue.json`).
- On startup the service loads that array, normalizes it into a dict keyed by `site_name`, and writes it to `sites_cache.json` alongside the source file (override with `SITES_CACHE_JSON`).
- The cache is refreshed automatically on a timer (default every 3 hours); configure via `PUE_REFRESH_HOURS` (set to `0` or non-numeric to disable).
- Cache shape:
  ```json
  {
    "CERN-PROD": {"lat":46.2331,"lon":6.0559,"pue":1.4,"country":null,"roc":null}
  }
  ```
- Lookups read from the cache first; if a requested site is missing, the source file is reloaded and the cache is refreshed.

## Quick usage

### `/ci` — compute CI / Effective CI (and optional CFP)

**JSON example**
```json
{
  "lat": 52.0,
  "lon": 5.0,
  "start": "2025-09-04T12:00:00Z",
  "end": "2025-09-04T15:00:00Z",
  "pue": 1.4
}
```

**Curl**
```bash
curl -s -X POST http://localhost:8111/ci   -H "Authorization: Bearer $TOKEN"   -H "Content-Type: application/json"   -d '{"lat":52.0,"lon":5.0,"start":"2025-09-04T12:00:00Z","end":"2025-09-04T15:00:00Z","pue":1.4}'
```

### `/pue` — resolve site location and PUE by site name

**JSON example**
```json
{
  "site_name": "CERN-PROD"
}
```

**Curl**
```bash
curl -s -X POST http://localhost:8111/pue   -H "Authorization: Bearer $TOKEN"   -H "Content-Type: application/json"   -d '{"site_name":"CERN-PROD"}'
```

### `/transform-and-forward` — inject KPIs into `fact_site_event` and forward to SQL adapter
- Accepts `MetricsEnvelope` JSON, enriches with CI/PUE/CFP, then forwards to `CNR_SQL_FORWARD_URL`.
- Typically used by partner pipelines; see `kpi_service/app/main.py` for payload shape.

### Auth
All protected endpoints require:
```
Authorization: Bearer <jwt>
```

### Notes
- PUE can be per-site (in `data/sites_enriched.json`) or default via `PUE_DEFAULT`.
- The service validates tokens by calling your auth server’s `/verify_token` (configure `AUTH_VERIFY_URL`).
