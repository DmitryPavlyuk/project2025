# Meteorological Operational Data — Weather App

Latvian **meteorological operational data** Weather App.

Data source: data.gov.lv

Updated on-demand by nikita.mickevics

---

## Installation

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
```

---

## Firestore Initialization (snippet)

```python
factory = FirestoreClientFactory(
    project="project2025-bff4c",
    client_secret_path="secrets/client_secret.json",     # don't forget to add this file!!!
    token_path="secrets/.oauth_token.json",              # automatically generated (exp: <7 days)
    scopes=["https://www.googleapis.com/auth/datastore"],
)
db = factory.get_client()
```

### IMPORTANT: Add your OAuth client secret

* Place your **OAuth 2.0 Client ID JSON** file at:

  * `secrets/client_secret.json`
* This file is downloaded from **Google Cloud Console → APIs & Services → Credentials → Create Credentials → OAuth client ID (Desktop app)**.
* Ensure the file is **not committed** to version control. Add this to `.gitignore`:

```gitignore
# OAuth secrets
secrets/client_secret.json
secrets/.oauth_token.json
```

### Token file behavior

* After the first run, the library generates `secrets/.oauth_token.json` automatically (contains refresh token & expiry info).
* Token typically expires in **< 7 days**; the factory will refresh it as needed.
* You can safely delete the token file to force a new OAuth flow.

---

## Firestore Document Schema (per ABBR)

Data window is 48 hours

```json
{
  "ABBREVIATION": "CCTMX",
  "EN_DESCRIPTION": "The hourly maximum amount of cloud cover",
  "LV_DESCRIPTION": "Stundas maksimālais mākoņu daudzums",
  "SCALE": 1,
  "LOWER_LIMIT": 0,
  "UPPER_LIMIT": 9,
  "MEASUREMENT_UNIT": "Oktas",
  "TOTAL_STATIONS": 13,
  "OBSERVATIONS": [
    {
      "STATION_ID": "RIZO99MS",
      "NAME": "Zosēni",
      "WMO_ID": "26339",
      "BEGIN_DATE": "1945-04-24T00:00:00",
      "LATITUDE": 570806.0,
      "LONGITUDE": 255420.0,
      "GAUSS1": 615332.93,
      "GAUSS2": 334024.86,
      "GEOGR1": 25.9056,
      "GEOGR2": 57.135,
      "ELEVATION": 187.72,
      "ELEVATION_PRESSURE": 189.37,
      "DATETIME_EPOCH": 1761667200,
      "DATETIME_LV": "2025-10-28T18:00:00+02:00",
      "VALUE": 8.0
    }
  ]
}
```

---

# Available ABBREVIATIONS

Below are the supported meteorological metric abbreviations.

## Python list

```python
ABBR_LIST = [
    "HATMN", "HPRAB", "HPRSL", "HRLH", "HSNOW", "HTDRY", "HWDAV", "HWDMX", "HWNDS",
    "VSBAV", "WNS10", "WPGST", "VSBA", "SNOWA", "WNDD10", "PHENO", "PRSL", "RLH",
    "TDRY", "LI10I", "LICC", "LIGC", "LIMAXI", "LITOT", "HATMX", "HWSMX", "PRSS",
    "SAJT", "CCTMX", "UVIL"
]
```

## Short reference

* **HATMN** — The hourly minimum of air temperature
* **HPRAB** — The total amount of precipitation within an hour
* **HPRSL** — The hourly average atmospheric pressure at sea level
* **HRLH** — The hourly average relative humidity
* **HSNOW** — The hourly average snow depth
* **HTDRY** — The hourly average air temperature
* **HWDAV** — The hourly average wind direction
* **HWDMX** — The direction of the hourly maximum wind gusts
* **HWNDS** — The hourly average wind speed
* **VSBAV** — The hourly average meteorological visibility
* **WNS10** — Average wind speed during the observation time
* **WPGST** — Maximum wind gusts during the observation time
* **VSBA** — Meteorological visibility during the observation time
* **SNOWA** — Snow depth during the observation time
* **WNDD10** — Average wind direction during the observation time
* **PHENO** — Atmospheric phenomena
* **PRSL** — Atmospheric pressure at sea level during the observation time
* **RLH** — Relative humidity during the observation time
* **TDRY** — Air temperature during the observation time
* **LI10I** — Number of lightning strikes with current > 10 kA
* **LICC** — Number of cloud-cloud lightning strikes
* **LIGC** — Number of cloud-ground lightning strikes
* **LIMAXI** — Maximum current of lightning strikes
* **LITOT** — Total number of lightning strikes
* **HATMX** — The hourly maximum of air temperature
* **HWSMX** — The hourly maximum wind gusts
* **PRSS** — Atmospheric pressure at station level during the observation time
* **SAJT** — Apparent temperature during the observation time
* **CCTMX** — The hourly maximum amount of cloud cover
* **UVIL** — Ultraviolet radiation index during the observation time

## License

TBA

---

## Maintainers

* Your Name [mickevics.n@gmail.com](mailto:mickevics.n@gmail.com)

---

## Changelog

* 2025-10-29 — Initial commit: Database + Data API
