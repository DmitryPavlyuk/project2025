import requests
import math
from dataclasses import dataclass
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

BASE = "https://data.gov.lv/dati/lv/api/action/datastore_search"

RES_OBS = "17460efb-ae99-4d1d-8144-1068f184b05f"  # observations (many rows)
RES_ABB = "38b462ac-08b9-4168-9d6e-cbaedc2e775d"  # abbreviation dictionary (<= 50)
RES_STA = "c32c7afd-0d05-44fd-8b24-1de85b4bf11d"  # stations (<= 300)

RIGA = ZoneInfo("Europe/Riga")

class CkanFetchError(RuntimeError):
    """Raised when a CKAN request fails or returns an invalid payload."""

@dataclass
class MeteoComposer:
    target_abbr: str
    batch: int = 10_000  # <= 32,000

    @staticmethod
    def _to_float(x) -> Optional[float]:
        try:
            if isinstance(x, (int, float)):
                return float(x)
            if x is None or x == "":
                return None
            return float(str(x).replace(",", "."))
        except Exception:
            return None

    @staticmethod
    def _to_epoch_seconds_lv(dt_str: Any) -> Optional[int]:
        """
        Parse formats; interpret naive strings as Europe/Riga local time.
        Return UTC epoch seconds (int) or None.
        """
        if not dt_str:
            return None
        s = str(dt_str).strip()

        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                dt_local = datetime.strptime(s, fmt).replace(tzinfo=RIGA)
                return int(dt_local.astimezone(timezone.utc).timestamp())
            except ValueError:
                continue

        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=RIGA)
            return int(dt.astimezone(timezone.utc).timestamp())
        except Exception:
            return None

    @staticmethod
    def _epoch_to_iso_lv(epoch_seconds: Optional[int]) -> Optional[str]:
        if epoch_seconds is None:
            return None
        try:
            return datetime.fromtimestamp(epoch_seconds, tz=RIGA).isoformat()
        except Exception:
            return None

    def _fetch_all(self, resource_id: str, batch: int) -> List[Dict[str, Any]]:
        """Paginate CKAN datastore_search"""
        records: List[Dict[str, Any]] = []
        offset = 0
        total = None

        while True:
            params = {
                "resource_id": resource_id,
                "limit": batch,
                "offset": offset,
                "include_total": True,
            }
            try:
                r = requests.get(BASE, params=params, timeout=60)
                r.raise_for_status()
                payload = r.json()
            except requests.RequestException as e:
                raise CkanFetchError(f"HTTP error for resource {resource_id}: {e}") from e
            except ValueError as e:
                raise CkanFetchError(f"Invalid JSON for resource {resource_id}: {e}") from e

            if not isinstance(payload, dict) or not payload.get("success"):
                raise CkanFetchError(f"CKAN returned unsuccessful response for {resource_id}")

            result = payload.get("result") or {}
            chunk = result.get("records") or []
            if total is None:
                total = result.get("total")

            if not chunk:
                break

            records.extend(chunk)
            offset += batch
            if total is not None and offset >= total:
                break

        if not records:
            raise CkanFetchError(f"No records received for resource {resource_id}")
        return records

    def build_payload(self) -> Dict[str, Any]:
        """
        Build payload for `self.target_abbr` by fetching CKAN abbrev meta, stations, and observations.
        - Parses source DATETIME as Europe/Riga local time → UTC epoch (`DATETIME_EPOCH`) + ISO local (`DATETIME_LV`).
        - Coerces numerics; joins station fields; sorts by NAME desc, DATETIME_EPOCH desc.
        - Raises CkanFetchError if no observations found.

        Returns schema:
        {
        "ABBREVIATION": str,
        "EN_DESCRIPTION": str,
        "LV_DESCRIPTION": str,
        "SCALE": int|float|str|None,
        "LOWER_LIMIT": int|float|str|None,
        "UPPER_LIMIT": int|float|str|None,
        "MEASUREMENT_UNIT": str,
        "TOTAL_STATIONS": int,
        "OBSERVATIONS": [
            {
            "STATION_ID": str,
            "NAME": str,
            "WMO_ID": str,
            "BEGIN_DATE": str,
            "LATITUDE": float|None,
            "LONGITUDE": float|None,
            "GAUSS1": float|None,
            "GAUSS2": float|None,
            "GEOGR1": float|None,
            "GEOGR2": float|None,
            "ELEVATION": float|None,
            "ELEVATION_PRESSURE": float|None,
            "DATETIME_EPOCH": int|float|None,   # UTC epoch
            "DATETIME_LV": str|None,            # ISO in Europe/Riga
            "VALUE": float|None
            }
        ]
        }
        """
        abbr_rows = self._fetch_all(RES_ABB, batch=50)
        abbr_map = {r.get("ABBREVIATION"): r for r in abbr_rows}
        abbr_meta = abbr_map.get(self.target_abbr, {})

        sta_rows = self._fetch_all(RES_STA, batch=300)
        stations: Dict[str, Dict[str, Any]] = {}
        for r in sta_rows:
            sid = str(r.get("STATION_ID", "")).strip()
            if not sid:
                continue
            stations[sid] = {
                "NAME": r.get("NAME") or "",
                "WMO_ID": r.get("WMO_ID") or "",
                "BEGIN_DATE": r.get("BEGIN_DATE") or "",
                "LATITUDE": self._to_float(r.get("LATITUDE")),
                "LONGITUDE": self._to_float(r.get("LONGITUDE")),
                "GAUSS1": self._to_float(r.get("GAUSS1")),
                "GAUSS2": self._to_float(r.get("GAUSS2")),
                "GEOGR1": self._to_float(r.get("GEOGR1")),
                "GEOGR2": self._to_float(r.get("GEOGR2")),
                "ELEVATION": self._to_float(r.get("ELEVATION")),
                "ELEVATION_PRESSURE": self._to_float(r.get("ELEVATION_PRESSURE")),
            }

        obs_rows = self._fetch_all(RES_OBS, batch=self.batch)
        filtered = [r for r in obs_rows if str(r.get("ABBREVIATION", "")).strip() == self.target_abbr]
        if not filtered:
            raise CkanFetchError(f"No observations found for ABBREVIATION='{self.target_abbr}'")

        observations: List[Dict[str, Any]] = []
        for r in filtered:
            sid = str(r.get("STATION_ID", "")).strip()
            meta = stations.get(sid, {})
            epoch = self._to_epoch_seconds_lv(r.get("DATETIME"))
            observations.append({
                "STATION_ID": sid,
                "NAME": meta.get("NAME", ""),
                "WMO_ID": meta.get("WMO_ID", ""),
                "BEGIN_DATE": meta.get("BEGIN_DATE", ""),
                "LATITUDE": meta.get("LATITUDE"),
                "LONGITUDE": meta.get("LONGITUDE"),
                "GAUSS1": meta.get("GAUSS1"),
                "GAUSS2": meta.get("GAUSS2"),
                "GEOGR1": meta.get("GEOGR1"),
                "GEOGR2": meta.get("GEOGR2"),
                "ELEVATION": meta.get("ELEVATION"),
                "ELEVATION_PRESSURE": meta.get("ELEVATION_PRESSURE"),
                "DATETIME_EPOCH": epoch,                       # UTC epoch
                "DATETIME_LV": self._epoch_to_iso_lv(epoch),   # ISO in Europe/Riga
                "VALUE": self._to_float(r.get("VALUE")),
            })

        # Sort: NAME desc, then DATETIME_EPOCH desc
        def sort_key(o: Dict[str, Any]):
            name = o.get("NAME") or ""
            dt = o.get("DATETIME_EPOCH")
            dt_val = dt if isinstance(dt, (int, float)) else -math.inf
            return (name, dt_val)

        observations.sort(key=sort_key, reverse=True)

        total_stations = len({o["STATION_ID"] for o in observations if o["STATION_ID"]})

        payload = {
            "ABBREVIATION": self.target_abbr,
            "EN_DESCRIPTION": abbr_meta.get("EN_DESCRIPTION", ""),
            "LV_DESCRIPTION": abbr_meta.get("LV_DESCRIPTION", ""),
            "SCALE": abbr_meta.get("SCALE", ""),
            "LOWER_LIMIT": abbr_meta.get("LOWER_LIMIT", ""),
            "UPPER_LIMIT": abbr_meta.get("UPPER_LIMIT", ""),
            "MEASUREMENT_UNIT": abbr_meta.get("MEASUREMENT_UNIT", ""),
            "TOTAL_STATIONS": total_stations,
            "OBSERVATIONS": observations,
        }
        return payload