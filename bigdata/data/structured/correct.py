import pandas as pd
import os
from pathlib import Path

# --- CONFIGURATION ---
# On cherche le fichier intelligemment (soit dossier courant, soit data/structured)
BASE_DIR = Path(__file__).resolve().parent
FICHIER_CIBLE = "dispatch_structured.csv"

# Logique de recherche du fichier
path_fichier = BASE_DIR / "data" / "structured" / FICHIER_CIBLE
if not path_fichier.exists():
    path_fichier = BASE_DIR / FICHIER_CIBLE

# ---------------------------------------------------------

if path_fichier.exists():
    print(f"📂 Fichier trouvé : {path_fichier}")

    # 1. LECTURE ROBUSTE
    try:
        # On tente de lire avec ; (format attendu) et utf-8-sig (accents)
        df = pd.read_csv(path_fichier, sep=';', encoding='utf-8-sig')
        
        # Si échec (tout dans 1 colonne), c'est peut-être un vieux fichier avec virgules
        if len(df.columns) <= 1:
            print("⚠️ Séparateur ';' échoué, tentative avec virgule...")
            df = pd.read_csv(path_fichier, sep=',', encoding='utf-8-sig')

    except Exception as e:
        print(f"❌ Erreur de lecture : {e}")
        exit()

    # 2. NETTOYAGE DES NOMS DE COLONNES
    # Retire les espaces traîtres : " distance_km " -> "distance_km"
    df.columns = df.columns.str.strip()

    print(f"📊 Colonnes : {df.columns.tolist()}")
    print(f"🔢 Lignes avant : {len(df)}")

    # 3. VERIFICATION ET FILTRAGE
    if 'distance_km' in df.columns:
        # Force la conversion en nombres (transforme les erreurs en NaN)
        df['distance_km'] = pd.to_numeric(df['distance_km'], errors='coerce')
        
        # Filtre : <= 50km ET on retire les lignes où la distance est vide (NaN)
        df_propre = df[df['distance_km'] <= 50].dropna(subset=['distance_km'])

        # 4. SAUVEGARDE CRITIQUE (Le point le plus important !)
        # sep=';' -> INDISPENSABLE pour que Excel/PowerBI FR lisent les colonnes
        # encoding='utf-8-sig' -> INDISPENSABLE pour les accents
        df_propre.to_csv(path_fichier, index=False, sep=';', encoding='utf-8-sig')
        
        print(f"✅ Lignes après nettoyage : {len(df_propre)}")
        print("💾 Succès ! Fichier sauvegardé en format compatible Excel (point-virgule).")
    else:
        print("❌ ERREUR : La colonne 'distance_km' est introuvable.")

else:
    print(f"❌ Erreur : Impossible de trouver {FICHIER_CIBLE}")