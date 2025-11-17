import json
import csv
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))



from kafka import KafkaConsumer
from config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_AMBULANCES

from kafka import KafkaConsumer

from config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_AMBULANCES


ROOT_DIR = Path(__file__).resolve().parents[2]

AMB_RAW_DIR = ROOT_DIR / "data" / "raw" / "ambulances"
AMB_RAW_DIR.mkdir(parents=True, exist_ok=True)

CSV_PATH = AMB_RAW_DIR / "ambulances_stream.csv"



FIELDNAMES = [
    "id_ambulance",
    "code_ambulance",
    "plaque_immatriculation",
    "ville",
    "latitude",
    "longitude",
    "statut",
    "vitesse_kmh",
    "zone",
    "carburant_pourcent",
    "actif",
    "timestamp",
]


def main():
    print(f"[INFO] Racine projet : {ROOT_DIR}")
    print(f"[INFO] Fichier CSV de sortie : {CSV_PATH}")

    consumer = KafkaConsumer(
        TOPIC_AMBULANCES,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",  # lit depuis le début du topic
        enable_auto_commit=True,
        group_id="ambulances-csv-writer",
    )

    print(f"[INFO] Connecté à Kafka sur {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[INFO] Abonné au topic : {TOPIC_AMBULANCES}")

    file_exists = CSV_PATH.exists() and CSV_PATH.stat().st_size > 0

    with CSV_PATH.open(mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)

        if not file_exists:
            print("[INFO] Fichier CSV vide ou inexistant, écriture de l'en-tête.")
            writer.writeheader()

        print("[INFO] En attente de messages... (Ctrl+C pour arrêter)")

        try:
            for message in consumer:
                data = message.value 

                row = {
                    "id_ambulance": data.get("id_ambulance"),
                    "code_ambulance": data.get("code_ambulance"),
                    "plaque_immatriculation": data.get("plaque_immatriculation"),
                    "ville": data.get("ville"),
                    "latitude": data.get("latitude"),
                    "longitude": data.get("longitude"),
                    "statut": data.get("statut"),
                    "vitesse_kmh": data.get("vitesse_kmh"),
                    "zone": data.get("zone"),
                    "carburant_pourcent": data.get("carburant_pourcent"),
                    "actif": data.get("actif"),
                    "timestamp": data.get("timestamp"),
                }

                writer.writerow(row)
                f.flush() 
                print(f"[WRITE] {row}")

        except KeyboardInterrupt:
            print("\n[INFO] Arrêt du consumer (Ctrl+C).")
        finally:
            consumer.close()
            print("[INFO] Consumer fermé.")


if __name__ == "__main__":
    main()
