import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict

import pandas as pd
from kafka import KafkaProducer, KafkaConsumer

from logic.dispatch_logic import (
    choisir_meilleure_ambulance,
    choisir_meilleur_hopital,
    construire_evenement_dispatch,
)


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_STRUCTURED_DIR = BASE_DIR / "data" / "structured"

AMBULANCES_FILE = DATA_STRUCTURED_DIR / "ambulances_clean.csv"
HOPITAUX_FILE = DATA_STRUCTURED_DIR / "hopitaux_structured.csv"

KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC_APPELS = "appels"
TOPIC_DISPATCH = "dispatch"
GROUP_ID = "dispatch_groupe_FINAL_1"


def charger_ambulances() -> List[Dict]:
    if not AMBULANCES_FILE.exists():
        print(f"[WARN] Fichier ambulances introuvable : {AMBULANCES_FILE}")
        return []
    df = pd.read_csv(AMBULANCES_FILE)
    return df.to_dict(orient="records")


def charger_hopitaux() -> List[Dict]:
    if not HOPITAUX_FILE.exists():
        print(f"[WARN] Fichier hôpitaux introuvable : {HOPITAUX_FILE}")
        return []
    df = pd.read_csv(HOPITAUX_FILE)
    return df.to_dict(orient="records")


def generer_id_dispatch() -> str:
    """
    Génère un identifiant de dispatch du type DYYYYMMDDHHMMSSXXXX
    """
    now = datetime.now(timezone.utc)
    return "D" + now.strftime("%Y%m%d%H%M%S%f")[:18]


def run_service_dispatch() -> None:
    print("========== SERVICE DISPATCH ==========")
    print(f"[INFO] BASE_DIR            : {BASE_DIR}")
    print(f"[INFO] Fichier ambulances  : {AMBULANCES_FILE}")
    print(f"[INFO] Fichier hopitaux    : {HOPITAUX_FILE}")
    print(f"[INFO] Kafka bootstrap     : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[INFO] Topic appels        : {TOPIC_APPELS}")
    print(f"[INFO] Topic dispatch      : {TOPIC_DISPATCH}")
    print("======================================")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    consumer = KafkaConsumer(
        TOPIC_APPELS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=GROUP_ID,
    )

    print("[INFO] Service dispatch en écoute sur le topic 'appels'...")

    try:
        for msg in consumer:
            appel = msg.value

            id_appel = (
                appel.get("id_appel")
                or appel.get("id")
                or appel.get("id_appel_urgences")
            )
            motif = appel.get("motif") or appel.get("motif_appel")
            ville = appel.get("ville_patient") or appel.get("ville")

            print(
                f"\n[APPEL] Réception appel={id_appel} "
                f"ville={ville} motif={motif}"
            )

            ambulances = charger_ambulances()
            hopitaux = charger_hopitaux()

            meilleure_amb = choisir_meilleure_ambulance(appel, ambulances)
            meilleur_hop = choisir_meilleur_hopital(appel, hopitaux)

            if meilleure_amb:
                print(
                    f"[CHOIX] Ambulance={meilleure_amb.get('id_ambulance') or meilleure_amb.get('id')} "
                    f"dist={meilleure_amb.get('distance_ambulance_km'):.2f} km"
                )
            else:
                print("[CHOIX] Aucune ambulance disponible")

            if meilleur_hop:
                print(
                    f"[CHOIX] Hopital={meilleur_hop.get('id_hopital') or meilleur_hop.get('id')} "
                    f"dist={meilleur_hop.get('distance_hopital_km'):.2f} km "
                    f"saturation={meilleur_hop.get('taux_saturation_hopital'):.2f}"
                )
            else:
                print("[CHOIX] Aucun hôpital disponible (ou trop saturé)")

            id_dispatch = generer_id_dispatch()
            event = construire_evenement_dispatch(
                id_dispatch=id_dispatch,
                appel=appel,
                ambulance=meilleure_amb,
                hopital=meilleur_hop,
            )

            producer.send(TOPIC_DISPATCH, event)
            producer.flush()

            print(
                f"[DISPATCH] Envoyé -> {id_dispatch} | "
                f"appel={event.get('id_appel')} | "
                f"amb={event.get('id_ambulance')} | "
                f"hop={event.get('id_hopital')}"
            )

    except KeyboardInterrupt:
        print("\n[INFO] Arrêt du service dispatch (Ctrl+C)")

    finally:
        consumer.close()
        producer.close()
        print("[INFO] Service dispatch arrêté proprement.")


if __name__ == "__main__":
    run_service_dispatch()
