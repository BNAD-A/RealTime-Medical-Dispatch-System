import os
import sys
import json
import csv
from kafka import KafkaConsumer

# Import config.py
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_HOPITAUX

# Dossier data/raw
DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "raw"
)
os.makedirs(DATA_DIR, exist_ok=True)

CSV_PATH = os.path.join(DATA_DIR, "hopitaux_raw.csv")

# Colonnes alignées avec ton producer
CSV_FIELDS = [
    "id_hopital",
    "nom",
    "ville",
    "latitude",
    "longitude",
    "capacite_totale",
    "lits_occuppees",
    "taux_saturation",
    "niveau_urgence",
    "specialite_principale",
    "temps_moyen_prise_en_charge_min",
    "timestamp",
]


def ensure_csv_header(path, fieldnames):
    """Écrit l’entête si nécessaire."""
    needs_header = not os.path.exists(path) or os.path.getsize(path) == 0

    if needs_header:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()


def main():
    print("[INFO] Consumer hopitaux → CSV démarré")
    print(f"[INFO] Kafka : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[INFO] Topic : {TOPIC_HOPITAUX}")
    print(f"[INFO] CSV : {CSV_PATH}")

    ensure_csv_header(CSV_PATH, CSV_FIELDS)

    consumer = KafkaConsumer(
        TOPIC_HOPITAUX,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="hopitaux_to_csv_v2"
    )

    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)

        try:
            for msg in consumer:
                hop = msg.value

                row = {
                    "id_hopital": hop.get("id_hopital"),
                    "nom": hop.get("nom"),
                    "ville": hop.get("ville"),
                    "latitude": hop.get("latitude"),
                    "longitude": hop.get("longitude"),
                    "capacite_totale": hop.get("capacite_totale"),
                    "lits_occuppees": hop.get("lits_occuppees"),
                    "taux_saturation": hop.get("taux_saturation"),
                    "niveau_urgence": hop.get("niveau_urgence"),
                    "specialite_principale": hop.get("specialite_principale"),
                    "temps_moyen_prise_en_charge_min": hop.get("temps_moyen_prise_en_charge_min"),
                    "timestamp": hop.get("timestamp"),
                }

                writer.writerow(row)
                f.flush()

                print(f"[WRITE] H{row['id_hopital']} | {row['nom']} | sat={row['taux_saturation']}")

        except KeyboardInterrupt:
            print("\n[INFO] Arrêt du consumer.")
        finally:
            consumer.close()
            print("[INFO] Consumer fermé.")


if __name__ == "__main__":
    main()
