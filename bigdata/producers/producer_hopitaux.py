import os
import sys
import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

# Pour l'import depuis config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_HOPITAUX

# --- LISTE ÉTENDUE ET RÉALISTE DES HÔPITAUX ---
# Couvre les villes de votre dashboard (Oujda, Laayoune, Agadir, etc.)
DONNEES_HOPITAUX_MAROC = [
    # --- CASABLANCA (Grande capacité pour absorber la charge) ---
    {"id_fixe": 101, "nom": "CHU Ibn Rochd", "ville": "Casablanca", "lat": 33.585, "lon": -7.615, "cap": 120},
    {"id_fixe": 102, "nom": "Hôpital Cheikh Khalifa", "ville": "Casablanca", "lat": 33.560, "lon": -7.630, "cap": 90},
    {"id_fixe": 103, "nom": "Hôpital Moulay Youssef", "ville": "Casablanca", "lat": 33.590, "lon": -7.600, "cap": 70},

    # --- RABAT ---
    {"id_fixe": 201, "nom": "CHU Ibn Sina", "ville": "Rabat", "lat": 33.980, "lon": -6.830, "cap": 100},
    {"id_fixe": 202, "nom": "Hôpital Militaire", "ville": "Rabat", "lat": 33.990, "lon": -6.840, "cap": 85},
    {"id_fixe": 203, "nom": "Hôpital Cheikh Zaid", "ville": "Rabat", "lat": 33.970, "lon": -6.850, "cap": 75},

    # --- MARRAKECH ---
    {"id_fixe": 301, "nom": "CHU Mohammed VI", "ville": "Marrakech", "lat": 31.650, "lon": -8.020, "cap": 100},
    {"id_fixe": 302, "nom": "Hôpital Ibn Tofail", "ville": "Marrakech", "lat": 31.640, "lon": -8.010, "cap": 70},

    # --- TANGER ---
    {"id_fixe": 401, "nom": "CHU Tanger Mohammed VI", "ville": "Tanger", "lat": 35.760, "lon": -5.800, "cap": 90},
    {"id_fixe": 402, "nom": "Hôpital Duc de Tovar", "ville": "Tanger", "lat": 35.780, "lon": -5.810, "cap": 60},

    # --- FES ---
    {"id_fixe": 501, "nom": "CHU Hassan II", "ville": "Fès", "lat": 34.020, "lon": -4.980, "cap": 100},
    {"id_fixe": 502, "nom": "Hôpital Al Ghassani", "ville": "Fès", "lat": 34.030, "lon": -4.990, "cap": 65},

    # --- AGADIR ---
    {"id_fixe": 601, "nom": "CHU Agadir", "ville": "Agadir", "lat": 30.420, "lon": -9.600, "cap": 90},
    {"id_fixe": 602, "nom": "Hôpital Hassan II", "ville": "Agadir", "lat": 30.410, "lon": -9.580, "cap": 70},

    # --- OUJDA ---
    {"id_fixe": 701, "nom": "CHU Mohammed VI Oujda", "ville": "Oujda", "lat": 34.680, "lon": -1.910, "cap": 85},
    {"id_fixe": 702, "nom": "Hôpital Al Farabi", "ville": "Oujda", "lat": 34.670, "lon": -1.920, "cap": 60},

    # --- LAAYOUNE ---
    {"id_fixe": 801, "nom": "Hôpital Moulay Hassan", "ville": "Laayoune", "lat": 27.150, "lon": -13.200, "cap": 70},
    {"id_fixe": 802, "nom": "Hôpital Militaire 3e", "ville": "Laayoune", "lat": 27.160, "lon": -13.190, "cap": 60},
]

SPECIALITES = ["Traumatologie", "Neurologie", "Pédiatrie", "Urgences", "Cardiologie"]
NIVEAUX_URGENCE = ["niveau_1", "niveau_2", "niveau_3"]

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def generer_liste_hopitaux():
    """Génère la liste initiale basée sur les données dures."""
    hopitaux = []
    for h in DONNEES_HOPITAUX_MAROC:
        capacite = h["cap"]
        # Simulation d'occupation réaliste (entre 30% et 70% au démarrage)
        lits_occ = random.randint(int(capacite * 0.3), int(capacite * 0.7))

        hop = {
            "id_hopital": h["id_fixe"],
            "nom": h["nom"],
            "ville": h["ville"],
            "latitude": h["lat"],
            "longitude": h["lon"],
            "capacite_totale": capacite,
            "lits_occuppees": lits_occ,
            "taux_saturation": round(lits_occ / capacite, 4),
            "niveau_urgence": random.choice(NIVEAUX_URGENCE),
            "specialite_principale": random.choice(SPECIALITES),
            "temps_moyen_prise_en_charge_min": random.randint(15, 60),
            "timestamp": now_iso(),
        }
        hopitaux.append(hop)
    return hopitaux

def mettre_a_jour_hopital(hop: dict) -> dict:
    """Fait varier l'occupation des lits."""
    capacite = hop["capacite_totale"]
    occ = hop["lits_occuppees"]
    
    # Variation aléatoire (-2 à +3 patients)
    variation = random.randint(-2, 3)
    occ = max(0, min(capacite, occ + variation))

    hop["lits_occuppees"] = occ
    hop["taux_saturation"] = round(occ / capacite, 4)
    hop["timestamp"] = now_iso()
    return hop

def main():
    print(f"[INFO] Démarrage Producer Hôpitaux vers {KAFKA_BOOTSTRAP_SERVERS}")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    hopitaux = generer_liste_hopitaux()
    print(f"[INFO] {len(hopitaux)} hôpitaux chargés (Casa, Rabat, Laayoune, Oujda...).")

    try:
        while True:
            for hop in hopitaux:
                hop = mettre_a_jour_hopital(hop)
                producer.send(TOPIC_HOPITAUX, value=hop)
                print(f"[UPDATE] {hop['nom']} : {hop['taux_saturation']*100:.1f}%")
            
            producer.flush()
            print("[CYCLE] Mise à jour des hôpitaux envoyée.")
            time.sleep(5) 

    except KeyboardInterrupt:
        print("Arrêt.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()