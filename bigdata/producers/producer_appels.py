import random
import time
from datetime import datetime, timezone
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from kafka import KafkaProducer
import json

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_APPELS,
    VILLES_MAROC,
)


def now_iso():
    """Retourne le timestamp actuel en ISO UTC."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


MOTIFS_APPEL = [
    "Accident de la route",
    "Malaise",
    "Douleur thoracique",
    "Traumatisme",
    "Détresse respiratoire",
    "Perte de connaissance",
]

SOURCES_APPEL = [
    "patient",
    "témoin",
    "police",
    "famille",
]


def random_position_autour_ville(ville_info, rayon_deg=0.05):
    """
    Génère une position aléatoire autour d'une ville.
    rayon_deg ~ 0.05 ≈ quelques km autour de la ville.
    """
    base_lat = ville_info["lat"]
    base_lon = ville_info["lon"]
    lat = base_lat + random.uniform(-rayon_deg, rayon_deg)
    lon = base_lon + random.uniform(-rayon_deg, rayon_deg)
    return lat, lon


def generate_appel(appel_id_num):
    """Génère un appel d'urgence aléatoire sous forme de dict."""
    ville_info = random.choice(VILLES_MAROC)
    lat, lon = random_position_autour_ville(ville_info)

    motif = random.choice(MOTIFS_APPEL)
    source = random.choice(SOURCES_APPEL)
    gravite = random.randint(1, 3)
    return {
        "id_appel": f"APP-{appel_id_num:05d}",
        "ville": ville_info["ville"],
        "latitude": lat,
        "longitude": lon,
        "motif_appel": motif,
        "gravite": gravite,
        "source_appel": source,
        "statut_appel": "en_attente", 
        "timestamp": now_iso(),
    }


def main():
    print("[INFO] Démarrage du producteur d'appels d'urgence...")
    print(f"[INFO] Serveurs Kafka : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[INFO] Topic utilisé : {TOPIC_APPELS}")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    appel_id = 1

    try:
        while True:
            appel = generate_appel(appel_id)

            producer.send(TOPIC_APPELS, value=appel)
            producer.flush()

            print(f"[SEND] {appel}")

            appel_id += 1

            time.sleep(random.uniform(1.0, 3.0))

    except KeyboardInterrupt:
        print("\n[INFO] Arrêt du producteur d'appels (Ctrl+C).")
    finally:
        producer.close()
        print("[INFO] Producteur fermé.")


if __name__ == "__main__":
    main()
