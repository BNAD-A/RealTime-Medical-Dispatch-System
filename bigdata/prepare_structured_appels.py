from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "data" / "raw"
STRUCTURED_DIR = BASE_DIR / "data" / "structured"


def main():
    """
    Lit data/raw/appels_raw.csv,
    nettoie les données,
    écrit data/structured/appels_structured.csv
    """
    STRUCTURED_DIR.mkdir(parents=True, exist_ok=True)

    input_path = RAW_DIR / "appels_raw.csv"
    output_path = STRUCTURED_DIR / "appels_structured.csv"

    print(f"Lecture du fichier : {input_path}")
    df = pd.read_csv(input_path)

    # 1. Nettoyage de base
    df = df.drop_duplicates()
    if "id_appel" in df.columns:
        df = df.dropna(subset=["id_appel"])

    # 2. Timestamp
    if "timestamp_appel" in df.columns:
        df["timestamp_appel"] = pd.to_datetime(
            df["timestamp_appel"], errors="coerce"
        )

    # 3. Coordonnées
    for col in ["latitude", "longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 4. Gravité normalisée
    if "gravite" in df.columns:
        df["gravite"] = df["gravite"].astype(str).str.upper().str.strip()
        mapping = {
            "FAIBLE": "FAIBLE",
            "MOYENNE": "MOYENNE",
            "ELEVEE": "ELEVEE",
            "ÉLEVÉE": "ELEVEE",
            "CRITIQUE": "CRITIQUE",
        }
        df["gravite"] = df["gravite"].map(mapping).fillna("INCONNUE")

    df.to_csv(output_path, index=False)
    print(f"✅ Appels nettoyés → {output_path}")


if __name__ == "__main__":
    main()
