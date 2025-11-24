from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "data" / "raw"
STRUCTURED_DIR = BASE_DIR / "data" / "structured"


def main():
    """
    Lit data/raw/hopitaux_raw.csv,
    nettoie les données,
    écrit data/structured/hopitaux_structured.csv
    """
    STRUCTURED_DIR.mkdir(parents=True, exist_ok=True)

    input_path = RAW_DIR / "hopitaux_raw.csv"
    output_path = STRUCTURED_DIR / "hopitaux_structured.csv"

    print(f"Lecture du fichier : {input_path}")
    df = pd.read_csv(input_path)

    # 1. Nettoyage de base
    df = df.drop_duplicates()
    if "id_hopital" in df.columns:
        df = df.dropna(subset=["id_hopital"])

    # 2. Colonnes numériques
    numeric_cols = ["capacite_lits", "lits_occupees", "latitude", "longitude"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 3. Dates
    for col in ["last_update", "timestamp"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # 4. Taux d'occupation
    if {"capacite_lits", "lits_occupees"}.issubset(df.columns):
        df["taux_occupation"] = (
            df["lits_occupees"] / df["capacite_lits"]
        ).round(3)

    # 5. Sauvegarde
    df.to_csv(output_path, index=False)
    print(f"✅ Hopitaux nettoyés → {output_path}")


if __name__ == "__main__":
    main()
