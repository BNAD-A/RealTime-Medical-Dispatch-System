from pathlib import Path
import csv
import random
from datetime import datetime
from typing import Any, Optional, Dict

from utils_geo import haversine_km

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
STRUCTURED_DIR = DATA_DIR / "structured"

DISPATCH_RAW_CSV = RAW_DIR / "dispatch_raw.csv"
DISPATCH_STRUCTURED_CSV = STRUCTURED_DIR / "dispatch_structured.csv"
HOPITAUX_STRUCTURED_CSV = STRUCTURED_DIR / "hopitaux_structured.csv"
APPELS_STRUCTURED_CSV = STRUCTURED_DIR / "appels_structured.csv"


def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    """Convertit en float (gère virgule décimale), sinon renvoie default."""
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    v = str(value).strip()
    if v == "" or v.upper() in {"NA", "NAN", "NONE", "NULL"}:
        return default
    v = v.replace(",", ".")
    try:
        return float(v)
    except ValueError:
        return default


def get_priority_from_motif(motif: Optional[str]) -> str:
    if not motif:
        return "Moyenne"
    m = motif.lower()
    if any(k in m for k in ["arrêt", "cardiaque", "détresse", "accident", "traumatisme", "inconscient", "grave"]):
        return "Haute"
    if any(k in m for k in ["bobologie", "léger", "consultation", "renseignement"]):
        return "Basse"
    return "Moyenne"


def compute_zone_from_lat_lon(lat: Optional[float], lon: Optional[float]) -> str:
    """Zonage simple Maroc (heuristique)."""
    if lat is None or lon is None:
        return "centre"
    if lat > 34:
        return "nord"
    if lat < 31:
        return "sud"
    if lon < -8:
        return "ouest"
    if lon > -5:
        return "est"
    return "centre"


def detect_delimiter(csv_path: Path, fallback: str = ",") -> str:
    """
    Détecte automatiquement le délimiteur (',' ou ';') pour éviter
    les fichiers RAW qui changent.
    """
    try:
        sample = csv_path.read_text(encoding="utf-8", errors="ignore")[:4096]
        comma = sample.count(",")
        semi = sample.count(";")
        if semi > comma:
            return ";"
        if comma > 0:
            return ","
        try:
            return csv.Sniffer().sniff(sample, delimiters=";,").delimiter
        except Exception:
            return fallback
    except Exception:
        return fallback


def load_csv_as_dict(path: Path, key_field: str) -> Dict[str, Dict[str, str]]:
    data: Dict[str, Dict[str, str]] = {}
    if not path.exists():
        return data

    delim = detect_delimiter(path, fallback=";")
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=delim)
        for row in reader:
            k = (row.get(key_field) or "").strip()
            if k:
                data[k] = row
    return data


COLUMNS = [
    "id_dispatch",
    "id_appel",
    "motif_appel",
    "latitude_patient",
    "longitude_patient",
    "id_ambulance",
    "id_hopital",
    "distance_km",
    "temps_estime",
    "priorite",
    "taux_saturation",
    "zone_interv",
    "timestamp",
]


def build_structured_dispatch_record(
    dispatch_row: Dict[str, str],
    appel_row: Optional[Dict[str, str]] = None,
    hopital_row: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    # 1) IDs
    id_dispatch = (dispatch_row.get("id_dispatch") or dispatch_row.get("id") or "").strip()
    id_appel = (dispatch_row.get("id_appel") or "").strip()
    id_ambulance = (dispatch_row.get("id_ambulance") or "").strip()
    id_hopital = (dispatch_row.get("id_hopital") or "").strip()

    # 2) Motif
    motif = None
    if appel_row:
        motif = appel_row.get("motif_appel") or appel_row.get("motif")  # tolérance
    if not motif:
        motif = dispatch_row.get("motif_appel") or dispatch_row.get("motif")

    # 3) Coord patient (prend dispatch puis appel)
    lat_p = safe_float(dispatch_row.get("latitude_pat") or dispatch_row.get("latitude_patient"))
    lon_p = safe_float(dispatch_row.get("longitude_pat") or dispatch_row.get("longitude_patient"))

    if appel_row:
        if lat_p is None:
            lat_p = safe_float(appel_row.get("latitude_patient") or appel_row.get("latitude_pat") or appel_row.get("latitude"))
        if lon_p is None:
            lon_p = safe_float(appel_row.get("longitude_patient") or appel_row.get("longitude_pat") or appel_row.get("longitude"))

    # 4) Zone
    zone = (dispatch_row.get("zone_interv") or dispatch_row.get("zone_intervention") or "").strip()
    if not zone:
        zone = compute_zone_from_lat_lon(lat_p, lon_p)

    # 5) Distance
    dist_km = safe_float(dispatch_row.get("distance_km"))
    if (dist_km is None or dist_km == 0) and (lat_p is not None) and hopital_row:
        lat_h = safe_float(hopital_row.get("latitude_hopital") or hopital_row.get("latitude"))
        lon_h = safe_float(hopital_row.get("longitude_hopital") or hopital_row.get("longitude"))
        if lat_h is not None and lon_h is not None:
            dist_km = haversine_km(lat_p, lon_p, lat_h, lon_h)
            # arrondi propre
            dist_km = round(float(dist_km), 4) if dist_km is not None else None

    # 6) Temps estimé (évite le 15.0 fixe si on a une distance)
    temps_est = safe_float(dispatch_row.get("temps_estime"))
    if temps_est is None or (temps_est == 15.0 and dist_km is not None and dist_km > 0.5):
        if dist_km is not None:
            temps_est = round(dist_km * 1.5 + 5, 2)  # 1.5 min/km + 5 min
        else:
            temps_est = 15.0

    # 7) Priorité
    priorite = (dispatch_row.get("priorite") or "").strip()
    if not priorite:
        priorite = get_priority_from_motif(motif)

    # 8) Saturation
    taux_sat = safe_float(dispatch_row.get("taux_saturation"))
    if taux_sat is None:
        taux_sat = round(random.uniform(0.1, 0.9), 4)

    # 9) Timestamp (garde celui du dispatch si présent)
    timestamp = (dispatch_row.get("timestamp") or "").strip()
    if not timestamp:
        timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S+00:00")

    return {
        "id_dispatch": id_dispatch,
        "id_appel": id_appel,
        "motif_appel": motif,
        "latitude_patient": lat_p,
        "longitude_patient": lon_p,
        "id_ambulance": id_ambulance,
        "id_hopital": id_hopital,
        "distance_km": dist_km,
        "temps_estime": temps_est,
        "priorite": priorite,
        "taux_saturation": taux_sat,
        "zone_interv": zone,
        "timestamp": timestamp,
    }


def main():
    print("[INFO] Chargement des données structurées (Appels & Hôpitaux)...")

    appels_by_id = load_csv_as_dict(APPELS_STRUCTURED_CSV, "id_appel")
    hopitaux_by_id = load_csv_as_dict(HOPITAUX_STRUCTURED_CSV, "id_hopital")

    STRUCTURED_DIR.mkdir(parents=True, exist_ok=True)

    if not DISPATCH_RAW_CSV.exists():
        raise FileNotFoundError(f"[ERROR] Fichier raw introuvable: {DISPATCH_RAW_CSV}")

    raw_delim = detect_delimiter(DISPATCH_RAW_CSV, fallback=",")

    print(f"[INFO] RAW: {DISPATCH_RAW_CSV} (delimiter détecté = '{raw_delim}')")
    print(f"[INFO] STRUCTURED: {DISPATCH_STRUCTURED_CSV} (delimiter = ';', encoding = utf-8-sig)")

    count = 0

    with open(DISPATCH_RAW_CSV, newline="", encoding="utf-8-sig") as f_in, \
         open(DISPATCH_STRUCTURED_CSV, "w", newline="", encoding="utf-8-sig") as f_out:

        reader = csv.DictReader(f_in, delimiter=raw_delim)
        writer = csv.DictWriter(f_out, fieldnames=COLUMNS, delimiter=";")
        writer.writeheader()

        for row in reader:
            id_app = (row.get("id_appel") or "").strip()
            id_hop = (row.get("id_hopital") or "").strip()

            rec = build_structured_dispatch_record(
                row,
                appels_by_id.get(id_app),
                hopitaux_by_id.get(id_hop),
            )
            writer.writerow(rec)
            count += 1

    print(f"[OK] Terminé : {count} lignes écrites.")


if __name__ == "__main__":
    main()
