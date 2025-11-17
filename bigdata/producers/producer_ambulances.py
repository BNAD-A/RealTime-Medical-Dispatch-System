import random
import time
import json
from datetime import datetime, timezone
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from kafka import KafkaProducer 

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_AMBULANCES,
    N_AMBULANCES,
    VILLES_MAROC,
)

STATUTS = ["disponible", "en_route", "en_intervention", "hors_service"]


def now_iso() -> str:
    """Retourne l'heure actuelle au format ISO 8601 (UTC)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_AMBULANCES,
    N_AMBULANCES,
    VILLES_MAROC,
)

def init_fleet(n):
    flotte = []
    for i in range(1, n + 1):
        ville_info = random.choice(VILLES_MAROC)

        flotte.append(
            {
                "id_ambulance": i,
                "code_ambulance": f"AMB-{i:03d}",
                "plaque_immatriculation": f"{random.randint(10000, 99999)}-A-{random.randint(1, 9)}",
                "ville": ville_info["ville"],
                "base_lat": ville_info["lat"],
                "base_lon": ville_info["lon"],
                "latitude": ville_info["lat"],
                "longitude": ville_info["lon"],
                "statut": "disponible",
                "vitesse_kmh": 0.0,
                "zone": ville_info["zone"],
                "carburant_pourcent": random.randint(60, 100),
                "actif": True,
                "timestamp": now_iso(),
            }
        )
    return flotte



def update_ambulance(a: dict) -> dict:
    """
    Met à jour l'état d'une ambulance :
    - légère variation autour de sa ville
    - changement de statut
    - changement de vitesse
    - baisse du carburant
    - mise à jour du timestamp
    """

    a["latitude"] = a["base_lat"] + random.uniform(-0.05, 0.05)
    a["longitude"] = a["base_lon"] + random.uniform(-0.05, 0.05)

    a["statut"] = random.choices(
        STATUTS,
        weights=[0.5, 0.2, 0.2, 0.1],
        k=1,
    )[0]

    if a["statut"] == "disponible":
        a["vitesse_kmh"] = random.uniform(0, 30)
    elif a["statut"] == "en_route":
        a["vitesse_kmh"] = random.uniform(30, 80)
    elif a["statut"] == "en_intervention":
        a["vitesse_kmh"] = random.uniform(0, 20)
    else: 
        a["vitesse_kmh"] = 0.0

    a["carburant_pourcent"] = max(0, a["carburant_pourcent"] - random.uniform(0, 0.5))

    a["timestamp"] = now_iso()

    return a


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"[INFO] Connecté à Kafka sur {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[INFO] Topic utilisé : {TOPIC_AMBULANCES}")

    flotte = init_fleet(N_AMBULANCES)
    print(f"[INFO] Flotte initialisée avec {N_AMBULANCES} ambulances (tout le Maroc).")

    try:
        while True:
            for amb in flotte:
                if not amb["actif"]:
                    continue

                amb = update_ambulance(amb)

                message = {
                    "id_ambulance": amb["id_ambulance"],
                    "code_ambulance": amb["code_ambulance"],
                    "plaque_immatriculation": amb["plaque_immatriculation"],
                    "ville": amb["ville"],        # utile pour l'analyse BI
                    "latitude": amb["latitude"],
                    "longitude": amb["longitude"],
                    "statut": amb["statut"],
                    "vitesse_kmh": amb["vitesse_kmh"],
                    "zone": amb["zone"],
                    "carburant_pourcent": int(amb["carburant_pourcent"]),
                    "actif": amb["actif"],
                    "timestamp": amb["timestamp"],
                }

                producer.send(TOPIC_AMBULANCES, value=message)
                print(f"[SEND] {message}")

            time.sleep(2)

    except KeyboardInterrupt:
        print("\n[INFO] Arrêt du simulateur d'ambulances.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()

