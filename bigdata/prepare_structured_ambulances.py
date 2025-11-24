import csv
from pathlib import Path

# Racine = dossier bigdata
ROOT_DIR = Path(__file__).resolve().parent

RAW_FILE = ROOT_DIR / "data" / "raw" /  "ambulances_raw.csv"
STRUCTURED_DIR = ROOT_DIR / "data" / "structured"
STRUCTURED_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = STRUCTURED_DIR / "ambulances_clean.csv"

FIELDNAMES = [
    "id_ambulance",
    "code_ambulance",
    "plaque_immatriculation",
    "ville",
    "latitude",
    "longitude",
    "statut",
    "vitesse_kmh",
    "zone",
    "carburant_pourcent",
    "actif",
    "timestamp",
]

def main():
    if not RAW_FILE.exists():
        print(f"[ERROR] Fichier brut introuvable : {RAW_FILE}")
        return

    print(f"[INFO] Lecture du fichier brut : {RAW_FILE}")

    # On garde le dernier état connu de chaque ambulance
    last_state = {}

    with RAW_FILE.open(mode="r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            amb_id = row.get("id_ambulance")
            if not amb_id:
                continue
            # on écrase, donc on garde la dernière ligne pour chaque id
            last_state[amb_id] = row

    print(f"[INFO] Nombre d'ambulances uniques : {len(last_state)}")

    with OUT_FILE.open(mode="w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=FIELDNAMES)
        writer.writeheader()

        for amb in last_state.values():
            writer.writerow(amb)

    print(f"[INFO] Fichier structuré écrit dans : {OUT_FILE}")

if __name__ == "__main__":
    main()
