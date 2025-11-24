import os
import sys

# Pour pouvoir faire "from config import ..."
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_HOPITAUX,
    VILLES_MAROC,
)

# -----------------------
# Constantes de simulation
# -----------------------

SPECIALITES = [
    "Traumatologie",
    "Neurologie",
    "Pédiatrie",
    "Réanimation",
    "Cardiologie",
    "Urgences générales",
]

NIVEAUX_URGENCE = ["niveau_1", "niveau_2", "niveau_3"]

CAPACITE_MIN = 80
CAPACITE_MAX = 200


def now_iso() -> str:
    """Retourne un timestamp UTC au format ISO 8601."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def generer_liste_hopitaux():
    """
    Génère une liste d'hôpitaux à partir de VILLES_MAROC.

    Chaque hôpital respecte le schéma JSON cible :

    {
      "id_hopital": 1,
      "nom": "CHU Casablanca",
      "ville": "Casablanca",
      "latitude": 33.589,
      "longitude": -7.600,
      "capacite_totale": 120,
      "lits_occuppees": 85,
      "taux_saturation": 0.7083,
      "niveau_urgence": "niveau_3",
      "specialite_principale": "Traumatologie",
      "temps_moyen_prise_en_charge_min": 25,
      "timestamp": "2025-11-17T18:30:00Z"
    }
    """
    hopitaux = []
    id_counter = 1

    for ville_info in VILLES_MAROC:
        ville = ville_info["ville"]
        lat_base = ville_info["lat"]
        lon_base = ville_info["lon"]

        # 1 ou 2 hôpitaux par ville
        nb_hop_ville = random.choice([1, 2])

        for _ in range(nb_hop_ville):
            capacite = random.randint(CAPACITE_MIN, CAPACITE_MAX)
            lits_occ = random.randint(0, capacite)

            specialite = random.choice(SPECIALITES)

            hop = {
                "id_hopital": id_counter,
                "nom": f"CHU {ville}",
                "ville": ville,
                "latitude": lat_base + random.uniform(-0.02, 0.02),
                "longitude": lon_base + random.uniform(-0.02, 0.02),
                "capacite_totale": capacite,
                # ⚠️ orthographe voulue : lits_occuppees (2 p)
                "lits_occuppees": lits_occ,
                "taux_saturation": round(lits_occ / capacite, 4),
                "niveau_urgence": random.choice(NIVEAUX_URGENCE),
                "specialite_principale": specialite,
                "temps_moyen_prise_en_charge_min": random.randint(10, 60),
                "timestamp": now_iso(),
            }

            hopitaux.append(hop)
            id_counter += 1

    return hopitaux


def mettre_a_jour_hopital(hop: dict) -> dict:
    """
    Met à jour l'état d'un hôpital :
      - variation des lits occupés
      - recalcul du taux de saturation
      - petite variation du temps moyen
      - mise à jour du timestamp
    """
    capacite = hop["capacite_totale"]
    occ = hop["lits_occuppees"]

    variation = random.randint(-3, 3)
    occ = max(0, min(capacite, occ + variation))

    hop["lits_occuppees"] = occ
    hop["taux_saturation"] = round(occ / capacite, 4)

    # petite variation sur le temps moyen de prise en charge
    delta_temps = random.randint(-2, 2)
    hop["temps_moyen_prise_en_charge_min"] = max(
        5, hop["temps_moyen_prise_en_charge_min"] + delta_temps
    )

    hop["timestamp"] = now_iso()
    return hop


def main():
    print("[INFO] Démarrage du producer_hopitaux...")
    print(f"[INFO] Kafka : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[INFO] Topic : {TOPIC_HOPITAUX}")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    hopitaux = generer_liste_hopitaux()
    print(f"[INFO] Nombre d'hôpitaux simulés : {len(hopitaux)}")

    try:
        while True:
            for hop in hopitaux:
                hop = mettre_a_jour_hopital(hop)
                producer.send(TOPIC_HOPITAUX, value=hop)

                print(
                    f"[SEND] H{hop['id_hopital']:02d} | "
                    f"{hop['nom']} ({hop['specialite_principale']}) | "
                    f"{hop['ville']} | sat={hop['taux_saturation']:.2f} | "
                    f"tps={hop['temps_moyen_prise_en_charge_min']} min"
                )

            producer.flush()
            time.sleep(5)

    except KeyboardInterrupt:
        print("\n[INFO] Arrêt du producer_hopitaux (Ctrl+C).")
    finally:
        producer.close()
        print("[INFO] Producer Kafka fermé.")


if __name__ == "__main__":
    main()
