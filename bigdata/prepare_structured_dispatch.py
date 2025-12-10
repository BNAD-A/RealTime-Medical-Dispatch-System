from pathlib import Path
import csv
import random
from datetime import datetime
from typing import Any, Optional, Dict
from utils_geo import haversine_km

# === Chemins de base ===
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
STRUCTURED_DIR = DATA_DIR / "structured"

# === Fichiers utilisés ===
DISPATCH_RAW_CSV = RAW_DIR / "dispatch_raw.csv"
DISPATCH_STRUCTURED_CSV = STRUCTURED_DIR / "dispatch_structured.csv"
HOPITAUX_STRUCTURED_CSV = STRUCTURED_DIR / "hopitaux_structured.csv"
APPELS_STRUCTURED_CSV = STRUCTURED_DIR / "appels_structured.csv"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    """Convertit en float si possible, sinon renvoie default."""
    if value is None or value == "":
        return default
    try:
        return float(value)
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

def compute_zone_from_lat_lon(lat: Optional[float], lon: Optional[float]) -> Optional[str]:
    if lat is None or lon is None:
        return "centre"
    if lat > 34: return "nord"
    if lat < 31: return "sud"
    if lon < -8: return "ouest"
    if lon > -5: return "est"
    return "centre"

def load_csv_as_dict(path: Path, key_field: str) -> Dict[str, Dict[str, str]]:
    data: Dict[str, Dict[str, str]] = {}
    if not path.exists():
        return data
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            k = row.get(key_field)
            if k: data[k] = row
    return data

# ---------------------------------------------------------------------------
# Construction de l'enregistrement structuré
# ---------------------------------------------------------------------------

COLUMNS = [
    "id_dispatch", "id_appel", "motif_appel", "latitude_patient", "longitude_patient",
    "id_ambulance", "id_hopital", "distance_km", "temps_estime",
    "priorite", "taux_saturation", "zone_interv", "timestamp"
]

def build_structured_dispatch_record(
    dispatch_row: Dict[str, str],
    appel_row: Optional[Dict[str, str]] = None,
    hopital_row: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    
    # 1. Champs de base
    id_dispatch = dispatch_row.get("id_dispatch") or dispatch_row.get("id")
    id_appel = dispatch_row.get("id_appel")
    id_ambulance = dispatch_row.get("id_ambulance")
    id_hopital = dispatch_row.get("id_hopital")

    # 2. Motif
    motif = None
    if appel_row: motif = appel_row.get("motif_appel")
    if not motif: motif = dispatch_row.get("motif_appel")

    # 3. Coordonnées (CORRIGÉ POUR POWER BI)
    # On cherche d'abord dans le dispatch
    lat_p = safe_float(dispatch_row.get("latitude_pat") or dispatch_row.get("latitude_patient"))
    lon_p = safe_float(dispatch_row.get("longitude_pat") or dispatch_row.get("longitude_patient"))
    
    # Si vide, on cherche dans l'appel lié (en essayant tous les noms de colonnes possibles)
    if lat_p is None and appel_row:
        lat_p = safe_float(
            appel_row.get("latitude_patient") or 
            appel_row.get("latitude_pat") or 
            appel_row.get("latitude")  # Nom standard dans appels_structured
        )
    if lon_p is None and appel_row:
        lon_p = safe_float(
            appel_row.get("longitude_patient") or 
            appel_row.get("longitude_pat") or 
            appel_row.get("longitude") # Nom standard dans appels_structured
        )

    # 4. Zone
    zone = dispatch_row.get("zone_interv") or dispatch_row.get("zone_intervention")
    if not zone:
        zone = compute_zone_from_lat_lon(lat_p, lon_p)

    # 5. Distance (Calcul si manquant)
    dist_km = safe_float(dispatch_row.get("distance_km"))
    if (dist_km is None or dist_km == 0) and lat_p and hopital_row:
        lat_h = safe_float(hopital_row.get("latitude_hopital") or hopital_row.get("latitude"))
        lon_h = safe_float(hopital_row.get("longitude_hopital") or hopital_row.get("longitude"))
        if lat_h and lon_h:
            dist_km = haversine_km(lat_p, lon_p, lat_h, lon_h)

    # I. Temps Estimé (Correction pour éviter le 15.0 fixe)
    temps_est = safe_float(dispatch_row.get("temps_estime"))
    
    # Si le temps est manquant ou égal au défaut (15.0) alors qu'on a une vraie distance
    if temps_est is None or (temps_est == 15.0 and dist_km and dist_km > 0.5):
        if dist_km is not None:
            # Formule : 1.5 min par km + 5 min de prise en charge
            temps_est = round(dist_km * 1.5 + 5, 2)
        else:
            temps_est = 15.0 # Vrai fallback si aucune donnée

    # J. Priorité
    priorite = dispatch_row.get("priorite")
    if not priorite:
        priorite = get_priority_from_motif(motif)

    # K. Taux Saturation
    taux_sat = safe_float(dispatch_row.get("taux_saturation"))
    if taux_sat is None:
        taux_sat = round(random.uniform(0.1, 0.9), 4)

    # M. Timestamp
    timestamp = dispatch_row.get("timestamp")
    if not timestamp:
        timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

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

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Chargement des données annexes...")
    # On charge les référentiels
    appels_by_id = load_csv_as_dict(APPELS_STRUCTURED_CSV, "id_appel")
    hopitaux_by_id = load_csv_as_dict(HOPITAUX_STRUCTURED_CSV, "id_hopital")

    STRUCTURED_DIR.mkdir(parents=True, exist_ok=True)

    if not DISPATCH_RAW_CSV.exists():
        print("Erreur: Fichier raw introuvable.")
        return

    print(f"Écriture dans {DISPATCH_STRUCTURED_CSV}...")
    
    with open(DISPATCH_RAW_CSV, newline="", encoding="utf-8") as f_in, \
            open(DISPATCH_STRUCTURED_CSV, "w", newline="", encoding="utf-8") as f_out:

        reader = csv.DictReader(f_in)
        writer = csv.DictWriter(f_out, fieldnames=COLUMNS)
        writer.writeheader()

        count = 0
        for row in reader:
            id_app = row.get("id_appel")
            id_hop = row.get("id_hopital")
            
            # On passe les infos complètes pour reconstruire la ligne
            rec = build_structured_dispatch_record(
                row,
                appels_by_id.get(id_app),
                hopitaux_by_id.get(id_hop)
            )
            writer.writerow(rec)
            count += 1

    print(f"Terminé : {count} lignes traitées avec succès.")

if __name__ == "__main__":
    main()