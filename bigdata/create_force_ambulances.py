import pandas as pd
import random
import os
from datetime import datetime, timezone

# Configuration
OUTPUT_FILE = "data/structured/ambulances_clean.csv"
VILLES_CIBLES = [
    {"ville": "Casablanca", "lat": 33.5731, "lon": -7.5898},
    {"ville": "Rabat", "lat": 34.0209, "lon": -6.8416},
    {"ville": "Marrakech", "lat": 31.6295, "lon": -7.9811},
    {"ville": "Tanger", "lat": 35.7595, "lon": -5.8340},
    {"ville": "Fès", "lat": 34.0181, "lon": -5.0078},
    {"ville": "Agadir", "lat": 30.4278, "lon": -9.5981},
    {"ville": "Oujda", "lat": 34.6814, "lon": -1.9086},
    {"ville": "Laayoune", "lat": 27.1253, "lon": -13.1625},
]

def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def main():
    print(">>> Création forcée d'une flotte d'ambulances...")
    
    data = []
    id_counter = 1
    
    # On crée 15 ambulances PAR VILLE pour garantir la disponibilité locale
    for v in VILLES_CIBLES:
        for i in range(15):
            amb = {
                "id_ambulance": id_counter,
                "code_ambulance": f"AMB-{id_counter:03d}",
                "plaque_immatriculation": f"{random.randint(1000,9999)}-A-{random.randint(1,99)}",
                "ville": v["ville"],
                # On les place aléatoirement dans un rayon de ~3-5km du centre
                "latitude": v["lat"] + random.uniform(-0.03, 0.03),
                "longitude": v["lon"] + random.uniform(-0.03, 0.03),
                "statut": "disponible",  # FORCE DISPONIBLE
                "vitesse_kmh": 0.0,
                "zone": "Centre",
                "carburant_pourcent": 100,
                "actif": True,
                "timestamp": now_iso()
            }
            data.append(amb)
            id_counter += 1

    # Création du dossier si nécessaire
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    # Sauvegarde CSV
    df = pd.DataFrame(data)
    df.to_csv(OUTPUT_FILE, index=False)
    
    print(f"[SUCCÈS] Fichier '{OUTPUT_FILE}' généré avec {len(df)} ambulances DISPONIBLES.")
    print(">>> Vous pouvez relancer le service dispatch !")

if __name__ == "__main__":
    main()