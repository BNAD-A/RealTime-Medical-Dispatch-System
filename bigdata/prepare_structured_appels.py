from pathlib import Path
import pandas as pd
import numpy as np

BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "data" / "raw"
STRUCTURED_DIR = BASE_DIR / "data" / "structured"

def main():
    """
    Lit data/raw/appels_raw.csv,
    nettoie les données (Gravité 1/2/3, Statut Traité),
    écrit data/structured/appels_structured.csv
    """
    STRUCTURED_DIR.mkdir(parents=True, exist_ok=True)

    input_path = RAW_DIR / "appels_raw.csv"
    output_path = STRUCTURED_DIR / "appels_structured.csv"

    if not input_path.exists():
        print(f"❌ Fichier introuvable : {input_path}")
        return

    print(f"Lecture du fichier : {input_path}")
    df = pd.read_csv(input_path)

    # 1. Nettoyage de base
    df = df.drop_duplicates()
    if "id_appel" in df.columns:
        df = df.dropna(subset=["id_appel"])

    # 2. Gestion intelligente de la Gravité
    # On normalise d'abord en string
    if "gravite" in df.columns:
        df["gravite"] = df["gravite"].astype(str).str.strip()
    else:
        df["gravite"] = np.nan

    # Fonction pour corriger la gravité ligne par ligne
    def fix_gravite(row):
        g = str(row.get("gravite", "")).upper()
        m = str(row.get("motif_appel", "")).lower()

        # Si c'est déjà un chiffre valide, on le garde (ou on le formate)
        if g in ["1", "1.0", "FAIBLE"]: return "1 - Faible"
        if g in ["2", "2.0", "MOYENNE"]: return "2 - Moyenne"
        if g in ["3", "3.0", "ELEVEE", "HAUTE", "CRITIQUE"]: return "3 - Critique"
        
        # Si la gravité est inconnue/nan, on devine via le Motif
        if "arrêt" in m or "cardiaque" in m or "respiratoire" in m or "inconscient" in m:
            return "3 - Critique"
        if "accident" in m or "traumatisme" in m or "fracture" in m:
            return "2 - Moyenne"
        if "bobologie" in m or "fièvre" in m or "toux" in m or "consul" in m:
            return "1 - Faible"
        
        # Valeur par défaut si vraiment rien ne matche
        return "2 - Moyenne"

    # Appliquer la correction
    df["gravite"] = df.apply(fix_gravite, axis=1)

    # 3. Forcer le statut à "Traité" (pour le Dashboard)
    # Si l'appel est dans ce fichier, c'est qu'il est entré dans le système
    df["statut_appel"] = "Traité"

    # 4. Nettoyage Timestamp et Coordonnées
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    
    # S'assurer que les lat/lon sont des nombres
    for col in ["latitude", "longitude", "latitude_patient", "longitude_patient"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 5. Sauvegarde
    df.to_csv(output_path, index=False)
    print(f"✅ Appels nettoyés avec succès → {output_path}")
    print(f"   Exemple de gravités : {df['gravite'].unique()}")

if __name__ == "__main__":
    main()