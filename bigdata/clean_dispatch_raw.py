import os
import time
import pandas as pd
from pathlib import Path
import tempfile

BASE_DIR = Path(__file__).resolve().parent
RAW_PATH = BASE_DIR / "data" / "raw" / "dispatch_raw.csv"

ENCODING = "utf-8-sig"  
SEP_OUT = ";"  
WRITE_RETRIES = 5
WRITE_BACKOFF_SEC = 0.4


def _read_dispatch_csv(path: Path) -> pd.DataFrame:
    """
    Lecture robuste: essaye ';' puis ','.
    """
    if not path.exists():
        raise FileNotFoundError(f"Fichier introuvable: {path}")

    df = pd.read_csv(path, sep=";", encoding=ENCODING)
    if len(df.columns) <= 1:
        df = pd.read_csv(path, sep=",", encoding=ENCODING)

    df.columns = df.columns.str.strip()
    return df


def _atomic_write_csv(df: pd.DataFrame, path: Path, sep: str = ";") -> None:
    """
    Ecrit df dans un fichier temporaire situé DANS LE MEME DOSSIER que `path`,
    puis remplace atomiquement.
    -> évite "Invalid cross-device link" (/tmp -> /mnt/c)
    -> réduit les corruptions si Power BI lit en même temps
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(
        mode="w",
        delete=False,
        dir=str(path.parent),
        suffix=".tmp",
        encoding=ENCODING,
        newline=""
    ) as tmp:
        tmp_path = Path(tmp.name)
        df.to_csv(tmp_path, index=False, sep=sep, encoding=ENCODING)

    for i in range(WRITE_RETRIES):
        try:
            os.replace(tmp_path, path)  
            return
        except PermissionError:
            time.sleep(WRITE_BACKOFF_SEC * (i + 1))

    try:
        tmp_path.unlink(missing_ok=True)
    except Exception:
        pass

    raise PermissionError(f"Impossible d'écrire {path} (fichier verrouillé ?)")


def _basic_clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ajuste ici selon ta logique:
    - types
    - colonnes obligatoires
    - nettoyage texte/accents
    """
    expected = [
        "id_dispatch", "id_appel", "motif_appel",
        "latitude_patient", "longitude_patient",
        "id_ambulance", "id_hopital",
        "distance_km", "temps_estime",
        "priorite", "taux_saturation",
        "zone_interv", "timestamp"
    ]

    df.columns = df.columns.str.strip()

    missing = [c for c in expected if c not in df.columns]
    if missing:
        print(f"[WARN] Colonnes manquantes dans raw: {missing}")

    for col in ["latitude_patient", "longitude_patient", "distance_km", "temps_estime", "taux_saturation"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["motif_appel", "priorite", "zone_interv", "id_dispatch", "id_appel"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    df = df.dropna(how="all")

    return df


def main():
    print(f"🧹 CLEAN RAW dispatch: {RAW_PATH}")

    df = _read_dispatch_csv(RAW_PATH)
    df = _basic_clean(df)

    _atomic_write_csv(df, RAW_PATH, sep=SEP_OUT)

    print(f"✅ RAW nettoyé et réécrit: {RAW_PATH} | lignes={len(df)} | colonnes={len(df.columns)}")


if __name__ == "__main__":
    main()
