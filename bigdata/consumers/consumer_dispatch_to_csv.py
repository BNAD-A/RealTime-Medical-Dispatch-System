# consumers/consumer_dispatch_to_csv.py
import os
import sys
import json
import csv
from kafka import KafkaConsumer

# pour pouvoir faire "from config import ..."
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_DISPATCH

# dossier data/raw
DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "raw",
)
os.makedirs(DATA_DIR, exist_ok=True)

CSV_PATH = os.path.join(DATA_DIR, "dispatch_raw.csv")

FIELDNAMES = [
    "id_dispatch",
    "id_appel",
    "motif_appel",
    "latitude_patient",
    "longitude_patient",
    "id_ambulance",
    "id_hopital",
    "distance_km",
    "temps_estime_min",
    "priorite",
    "taux_saturation_hopital",
    "zone_intervention",
    "timestamp",
]


def ensure_header():
    need_header = True
    if os.path.exists(CSV_PATH) and os.path.getsize(CSV_PATH) > 0:
        need_header = False

    if need_header:
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()


def main():
    print("[INFO] Consumer dispatch → CSV démarré")
    print(f"[INFO] Kafka : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[INFO] Topic : {TOPIC_DISPATCH}")
    print(f"[INFO] Fichier CSV : {CSV_PATH}")

    ensure_header()

    consumer = KafkaConsumer(
        TOPIC_DISPATCH,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="dispatch_to_csv",
    )

    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)

        try:
            for msg in consumer:
                d = msg.value

                row = {key: d.get(key) for key in FIELDNAMES}
                writer.writerow(row)
                f.flush()

                print(f"[WRITE] {row['id_dispatch']} | appel={row['id_appel']} | amb={row['id_ambulance']} | hop={row['id_hopital']}")
        except KeyboardInterrupt:
            print("\n[INFO] Arrêt du consumer (Ctrl+C).")
        finally:
            consumer.close()
            print("[INFO] Consumer fermé.")


if __name__ == "__main__":
    main()
