import os, sys
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)



import json
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_AMBULANCES,
    TOPIC_HOPITAUX,
    TOPIC_APPELS,
    TOPIC_DISPATCH,
)
from logic.dispatch_logic import (
    choisir_meilleure_ambulance,
    choisir_meilleur_hopital,
)



def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# petit compteur en mémoire pour générer des id_dispatch uniques
counter_dispatch = 1


def generate_id_dispatch():
    global counter_dispatch
    today = datetime.now().strftime("%Y%m%d")
    dispatch_id = f"D{today}{counter_dispatch:04d}"
    counter_dispatch += 1
    return dispatch_id


def priorite_depuis_gravite(gravite: int) -> str:
    """
    gravite 3 -> 'haute'
    gravite 2 -> 'moyenne'
    gravite 1 -> 'basse'
    """
    if gravite >= 3:
        return "haute"
    elif gravite == 2:
        return "moyenne"
    return "basse"


def main():
    print("[INFO] Démarrage du service de dispatch...")
    print(f"[INFO] Kafka : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[INFO] Topics lus : {TOPIC_AMBULANCES}, {TOPIC_HOPITAUX}, {TOPIC_APPELS}")
    print(f"[INFO] Topic écrit : {TOPIC_DISPATCH}")

    consumer = KafkaConsumer(
        TOPIC_AMBULANCES,
        TOPIC_HOPITAUX,
        TOPIC_APPELS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="service-dispatch",
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # états courants en mémoire
    etat_ambulances = {}  # id_ambulance -> dict
    etat_hopitaux = {}    # id_hopital -> dict

    print("[INFO] Service de dispatch en écoute... (Ctrl+C pour arrêter)")

    try:
        for message in consumer:
            topic = message.topic
            data = message.value

            if topic == TOPIC_AMBULANCES:
                amb_id = data["id_ambulance"]
                etat_ambulances[amb_id] = data

            elif topic == TOPIC_HOPITAUX:
                hop_id = data["id_hopital"]
                etat_hopitaux[hop_id] = data

            elif topic == TOPIC_APPELS:
                appel = data
                print(f"[APPEL] {appel['id_appel']} à {appel['ville']} motif={appel['motif_appel']}")

                # 1) choisir l’ambulance
                meilleure_amb, dist_amb_km = choisir_meilleure_ambulance(etat_ambulances, appel)
                if meilleure_amb is None:
                    print("[WARN] Aucune ambulance disponible pour cet appel.")
                    continue

                # 2) choisir l’hôpital
                meilleur_hop, dist_hop_km, saturation = choisir_meilleur_hopital(etat_hopitaux, appel)

                # on prend comme distance principale la distance patient → hôpital
                distance_km = dist_hop_km if dist_hop_km is not None else dist_amb_km

                # temps estimé très simple : vitesse 40 km/h
                temps_estime_min = None
                if distance_km is not None:
                    temps_estime_min = distance_km / 40.0 * 60.0  # en minutes

                priorite = priorite_depuis_gravite(appel.get("gravite", 1))

                dispatch_event = {
                    "id_dispatch": generate_id_dispatch(),
                    "id_appel": appel["id_appel"],
                    "motif_appel": appel["motif_appel"],
                    "latitude_patient": appel["latitude"],
                    "longitude_patient": appel["longitude"],
                    "id_ambulance": meilleure_amb["id_ambulance"],
                    "id_hopital": meilleur_hop["id_hopital"] if meilleur_hop else None,
                    "distance_km": distance_km,
                    "temps_estime_min": temps_estime_min,
                    "priorite": priorite,
                    "taux_saturation_hopital": saturation,
                    "zone_intervention": meilleure_amb.get("zone"),
                    "timestamp": now_iso(),
                }

                producer.send(TOPIC_DISPATCH, value=dispatch_event)
                producer.flush()

                print(f"[DISPATCH] {dispatch_event}")

    except KeyboardInterrupt:
        print("\n[INFO] Arrêt du service de dispatch (Ctrl+C).")
    finally:
        consumer.close()
        producer.close()
        print("[INFO] Dispatch service fermé.")


if __name__ == "__main__":
    main()
