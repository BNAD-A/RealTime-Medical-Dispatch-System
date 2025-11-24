from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "data" / "raw"
STRUCTURED_DIR = BASE_DIR / "data" / "structured"


def main():
    """
    Lit data/raw/dispatch_raw.csv,
    nettoie les données,
    écrit data/structured/dispatch_structured.csv
    """
    STRUCTURED_DIR.mkdir(parents=True, exist_ok=True)

    input_path = RAW_DIR / "dispatch_raw.csv"
    output_path = STRUCTURED_DIR / "dispatch_structured.csv"

    print(f"Lecture du fichier : {input_path}")
    df = pd.read_csv(input_path)

    # 1. Nettoyage de base
    df = df.drop_duplicates()
    required_cols = ["id_appel", "id_ambulance", "id_hopital"]
    df = df.dropna(subset=[c for c in required_cols if c in df.columns])

    # 2. Colonnes numériques
    numeric_cols = ["distance_ambulance_km", "temps_hopital_estime_min"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 3. Timestamp
    if "timestamp_dispatch" in df.columns:
        df["timestamp_dispatch"] = pd.to_datetime(
            df["timestamp_dispatch"], errors="coerce"
        )

    df.to_csv(output_path, index=False)
    print(f"✅ Dispatch nettoyé → {output_path}")


if __name__ == "__main__":
    main()

