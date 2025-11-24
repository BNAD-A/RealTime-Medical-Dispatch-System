import os
import sys
import json
import csv
from kafka import KafkaConsumer

# Import config.py depuis le dossier parent
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_APPELS

# Chemin du CSV (dans data/raw/)
DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "raw"
)
os.makedirs(DATA_DIR, exist_ok=True)
CSV_PATH = os.path.join(DATA_DIR, "appels_raw.csv")

# Colonnes réelles (fidèles au producer)
CSV_FIELDS = [
    "id_appel",
    "ville",
    "latitude",
    "longitude",
    "motif_appel",
    "gravite",
    "source_appel",
    "statut_appel",
    "timestamp",
]


def ensure_csv_header():
    """Écrit l'entête du CSV si nécessaire."""
    if not os.path.exists(CSV_PATH) or os.path.getsize(CSV_PATH) == 0:
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writeheader()


def main():
    print("[INFO] Consumer 'appels' → CSV démarré")
    print(f"[INFO] Kafka : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[INFO] Topic : {TOPIC_APPELS}")
    print(f"[INFO] Fichier CSV : {CSV_PATH}")

    ensure_csv_header()

    consumer = KafkaConsumer(
        TOPIC_APPELS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="appels_to_csv_group",
    )

    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)

        try:
            for msg in consumer:
                appel = msg.value

                row = {
                    "id_appel": appel.get("id_appel"),
                    "ville": appel.get("ville"),
                    "latitude": appel.get("latitude"),
                    "longitude": appel.get("longitude"),
                    "motif_appel": appel.get("motif_appel"),
                    "gravite": appel.get("gravite"),
                    "source_appel": appel.get("source_appel"),
                    "statut_appel": appel.get("statut_appel"),
                    "timestamp": appel.get("timestamp"),
                }

                writer.writerow(row)
                f.flush()

                print(f"[WRITE] {row['id_appel']} | {row['ville']} | {row['motif_appel']} | gravité={row['gravite']}")

        except KeyboardInterrupt:
            print("\n[INFO] Arrêt du consumer (Ctrl+C).")
        finally:
            consumer.close()
            print("[INFO] Consumer fermé.")


if __name__ == "__main__":
    main()
