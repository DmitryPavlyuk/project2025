import os
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple, Iterable
from google.cloud import firestore

@dataclass
class FirestoreWriter:
    """
    One document per ABBREVIATION in collection 'meteorological_operational_data'.
    - save_payload: full replace
    - save_incremental: append only newer rows AND always prune to the last `prune_hours`
                        per station relative to each station's latest DATETIME_EPOCH.
    """
    db: Optional[firestore.Client] = None
    collection_name: str = "meteorological_operational_data"
    size_warn_bytes: int = 900_000
    prune_hours: int = 48  # keep only last 48h per station by default

    def __post_init__(self):
        if self.db is None:
            self.db = firestore.Client()
        self._col_ref = self.db.collection(self.collection_name)

    @staticmethod
    def _approx_doc_size_bytes(doc: Dict[str, Any]) -> int:
        try:
            return len(json.dumps(doc, ensure_ascii=False))
        except Exception:
            return 0

    @staticmethod
    def _validate_payload(payload: Dict[str, Any]) -> str:
        if not isinstance(payload, dict):
            raise ValueError("Payload must be a dict.")
        abbr = payload.get("ABBREVIATION")
        if not isinstance(abbr, str) or not abbr.strip():
            raise ValueError("Payload missing 'ABBREVIATION' (string).")
        obs = payload.get("OBSERVATIONS")
        if not isinstance(obs, list):
            raise ValueError("Payload missing 'OBSERVATIONS' (list).")
        return abbr.strip()

    @staticmethod
    def _epoch_field_value(obs: Dict[str, Any]) -> Optional[float]:
        v = obs.get("DATETIME_EPOCH")
        if isinstance(v, (int, float)):
            return float(v)
        try:
            return float(v) if v is not None else None
        except Exception:
            return None

    @staticmethod
    def _get_station_id(obs: Dict[str, Any]) -> str:
        v = obs.get("STATION_ID")
        return str(v).strip() if v is not None else ""

    @staticmethod
    def _sort_key(o: Dict[str, Any]) -> Tuple[str, float]:
        name = (o.get("NAME") or "")
        ep = o.get("DATETIME_EPOCH")
        ep_val = ep if isinstance(ep, (int, float)) else -math.inf
        return (name, ep_val)

    @staticmethod
    def _latest_by_station(observations: List[Dict[str, Any]]) -> Dict[str, float]:
        latest: Dict[str, float] = {}
        for o in observations:
            sid = FirestoreWriter._get_station_id(o)
            ep = FirestoreWriter._epoch_field_value(o)
            if not sid or ep is None:
                continue
            cur = latest.get(sid)
            if cur is None or ep > cur:
                latest[sid] = ep
        return latest

    @staticmethod
    def _prune_by_station_window(
        observations: List[Dict[str, Any]],
        window_seconds: int
    ) -> List[Dict[str, Any]]:
        """Keep only rows with DATETIME_EPOCH >= (station_latest - window_seconds) per station."""
        latest = FirestoreWriter._latest_by_station(observations)
        keep: List[Dict[str, Any]] = []
        for o in observations:
            sid = FirestoreWriter._get_station_id(o)
            ep = FirestoreWriter._epoch_field_value(o)
            if not sid or ep is None:
                continue
            latest_ep = latest.get(sid)
            if latest_ep is None or ep >= (latest_ep - window_seconds):
                keep.append(o)
        return keep

    def save_payload(self, payload: Dict[str, Any]) -> None:
        """Overwrite the Firestore doc for this ABBR with the full payload (no merge).
        Prints a size warning near the 1 MiB limit and logs the OBS count."""
        abbr = self._validate_payload(payload)
        size_est = self._approx_doc_size_bytes(payload)
        if size_est >= self.size_warn_bytes:
            print(f"[WARN] {abbr}: estimated JSON size {size_est/1024:.1f} KB may approach/exceed Firestore 1 MiB limit.")
        self._col_ref.document(abbr).set(payload, merge=False)
        print(f"[OK] replace {self.collection_name}/{abbr} (OBS={len(payload['OBSERVATIONS'])})")

    def save_incremental(self, payload: Dict[str, Any]) -> None:
        """
        Append only newer rows per station, then ALWAYS prune to last `prune_hours`
        relative to each station's latest DATETIME_EPOCH. Resort and update totals.
        If doc doesn't exist yet, write full payload (no need to prune then).
        """
        abbr = self._validate_payload(payload)
        doc_ref = self._col_ref.document(abbr)
        snap = doc_ref.get()

        if not snap.exists:
            return self.save_payload(payload)

        existing = snap.to_dict() or {}
        existing_obs = existing.get("OBSERVATIONS", [])
        if not isinstance(existing_obs, list):
            existing_obs = []

        latest_map = self._latest_by_station(existing_obs)
        seen_pairs = {
            (self._get_station_id(o), self._epoch_field_value(o))
            for o in existing_obs
            if self._epoch_field_value(o) is not None
        }

        incoming = payload["OBSERVATIONS"]
        new_obs: List[Dict[str, Any]] = []
        for o in incoming:
            sid = self._get_station_id(o)
            ep = self._epoch_field_value(o)
            if not sid or ep is None:
                continue
            if (sid, ep) in seen_pairs:
                continue
            last = latest_map.get(sid, -math.inf)
            if ep > last:
                new_obs.append(o)

        merged_obs = existing_obs + new_obs

        window_seconds = int(self.prune_hours * 3600)
        merged_obs = self._prune_by_station_window(merged_obs, window_seconds)

        merged_obs.sort(key=self._sort_key, reverse=True)
        total_stations = len({self._get_station_id(o) for o in merged_obs if self._get_station_id(o)})

        updated = dict(existing)
        for k in ("ABBREVIATION","EN_DESCRIPTION","LV_DESCRIPTION","SCALE","LOWER_LIMIT","UPPER_LIMIT","MEASUREMENT_UNIT"):
            if k in payload:
                updated[k] = payload[k]
        updated["TOTAL_STATIONS"] = total_stations
        updated["OBSERVATIONS"] = merged_obs

        size_est = self._approx_doc_size_bytes(updated)
        if size_est >= self.size_warn_bytes:
            print(f"[WARN] {abbr}: estimated JSON size {size_est/1024:.1f} KB may approach/exceed Firestore 1 MiB limit.")

        doc_ref.set(updated, merge=False)
        print(
            f"[OK] incremental {self.collection_name}/{abbr}: "
            f"added {len(new_obs)} new, kept {len(merged_obs)} after {self.prune_hours}h prune"
        )

@dataclass
class FirestoreReader:
    allowed_abb: Optional[List[str]] = None
    db: Optional[firestore.Client] = None
    collection_name: str = "meteorological_operational_data"

    def __post_init__(self):
        if self.db is None:
            self.db = firestore.client()
        self._col_ref = self.db.collection(self.collection_name)

    def _check_allowed(self, abbr: str) -> None:
        if self.allowed_abb is not None and abbr not in self.allowed_abb:
            raise ValueError(f"ABBREVIATION '{abbr}' is not allowed.")

    @staticmethod
    def _reorder_top_level(doc: Dict[str, Any]) -> Dict[str, Any]:
        """Sort keys A→Z, but place 'OBSERVATIONS' last (if present)."""
        if not isinstance(doc, dict):
            return doc
        doc = dict(doc)
        doc.setdefault("ABBREVIATION", doc.get("ABBREVIATION"))
        obs = doc.pop("OBSERVATIONS", None)
        ordered = {k: doc[k] for k in sorted(doc.keys(), key=str.lower)}
        if obs is not None:
            ordered["OBSERVATIONS"] = obs
        return ordered

    # --- PUBLIC API: return Python dict ---
    def get(self, abbr: str, reorder: bool = True) -> Dict[str, Any]:
        """
        Return the full Firestore document for ABBR as a Python dict.

        ABBR = Meteorological parameters
        
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
        self._check_allowed(abbr)
        doc = self.fetch(self.db, self.collection_name, abbr) 
        return self._reorder_top_level(doc) if reorder else doc

    @staticmethod
    def fetch(db: firestore.Client, collection_name: str, abbr: str) -> Dict[str, Any]:
        """
        Low-level read: fetch document {collection_name}/{abbr} using the provided client.
        Returns the stored dict (adds ABBREVIATION if missing) or raises FileNotFoundError.
        """
        abbr_norm = (abbr or "").strip()
        if not abbr_norm:
            raise ValueError("ABBREVIATION must be a non-empty string.")
        col_ref = db.collection(collection_name)
        snap = col_ref.document(abbr_norm).get()
        if not snap.exists:
            raise FileNotFoundError(f"Document not found: {collection_name}/{abbr_norm}")
        doc = snap.to_dict() or {}
        doc.setdefault("ABBREVIATION", abbr_norm)
        return doc

    def save_json(self, abbr: str, out_folder: Optional[str] = None, indent: int = 2) -> Path:
        """Fetch ABBR doc and write it as JSON to disk (defaults to './<ABBR>.json')."""
        data = self.get(abbr, reorder=True)
        folder = Path(out_folder) if out_folder else Path(".")
        folder.mkdir(parents=True, exist_ok=True)
        target = folder / f"{abbr}.json"
        with target.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=indent)
            print(f"[OK] read {self.collection_name}/{abbr}: saved {abbr}.json to {target}")
        return target

    def export_collection(self, out_path: str = "json_debug_files/meteorological_operational_data.json", indent: int = 2) -> Path:
        """Export the whole collection to a single JSON mapping ABBR -> document."""
        data: Dict[str, Dict[str, Any]] = {}
        count = 0
        for snap in self._col_ref.stream():
            abbr = snap.id
            doc = snap.to_dict() or {}
            doc.setdefault("ABBREVIATION", abbr)
            data[abbr] = self._reorder_top_level(doc)
            count += 1

        path = Path(out_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=indent)
            print(f"[OK] read {self.collection_name}: saved {count} documents to {path}")
        return path