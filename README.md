# 🌱🌍♻️ GreenDIGIT KPI Calculation Service — CI → CFP

FastAPI microservice that retrieves Location (lat/lon) from [GOC DB](https://goc.egi.eu), and **Carbon Intensity (CI)** from [wattprint](https://api.wattprint.eu/). It derives **Effective CI** and **Carbon Footprint (CFP)**. More KPIs to be included in the future.

## Roadmap
- [ ] Apply ICTF X502 certification to get Location and PUE from sites (GOC DB).
- [ ] Implement dynamic calculation of the CI. For the moment we're using mock data (generated).
- [ ] Include WUE, PUE, and other KPIs.

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

## Quick usage

### `/ci` — compute CI / Effective CI (and optional CFP)

**JSON example**
```json
{
  "lat": 52.0,
  "lon": 5.0,
  "time": "2025-09-04T12:00:00Z",
  "pue": 1.4
}
```

**Curl**
```bash
curl -s -X POST http://localhost:8011/ci   -H "Authorization: Bearer $TOKEN"   -H "Content-Type: application/json"   -d '{"lat":52.0,"lon":5.0,"time":"2025-09-04T12:00:00Z","pue":1.4}'
```

### `/rank-sites` — order sites by best Effective CI at a start time

**JSON example**
```json
{
  "start_time": "2025-09-04T12:00:00Z"
}
```

**Curl**
```bash
curl -s -X POST http://localhost:8011/rank-sites   -H "Authorization: Bearer $TOKEN"   -H "Content-Type: application/json"   -d '{"start_time":"2025-09-04T12:00:00Z"}'
```

### Auth
All protected endpoints require:
```
Authorization: Bearer <jwt>
```

### Notes
- PUE can be per-site (in `data/sites_enriched.json`) or default via `PUE_DEFAULT`.
- The service validates tokens by calling your auth server’s `/verify_token` (configure `AUTH_VERIFY_URL`).
