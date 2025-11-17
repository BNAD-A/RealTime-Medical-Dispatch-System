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
from dispatch_logic import (
    choisir_meilleure_ambulance,
    choisir_meilleur_hopital,
)


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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

    # états courants
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

                # 1) choisir ambulance
                meilleure_amb, dist_amb_km = choisir_meilleure_ambulance(etat_ambulances, appel)

                if meilleure_amb is None:
                    print("[WARN] Aucun ambulance disponible pour cet appel.")
                    continue

                # 2) choisir hôpital en combinant distance + saturation
                meilleur_hop, dist_hop_km, saturation = choisir_meilleur_hopital(etat_hopitaux, appel)

                # 3) temps estimé (très simple : 40 km/h)
                temps_amb_min = dist_amb_km / 40.0 * 60.0 if dist_amb_km is not None else None
                temps_hop_min = dist_hop_km / 40.0 * 60.0 if dist_hop_km is not None else None

                dispatch_event = {
                    # infos appel
                    "id_appel": appel["id_appel"],
                    "ville_appel": appel["ville"],
                    "latitude_appel": appel["latitude"],
                    "longitude_appel": appel["longitude"],
                    "motif_appel": appel["motif_appel"],
                    "gravite": appel["gravite"],
                    "source_appel": appel["source_appel"],

                    # ambulance choisie
                    "id_ambulance": meilleure_amb["id_ambulance"],
                    "code_ambulance": meilleure_amb["code_ambulance"],
                    "ville_ambulance": meilleure_amb["ville"],
                    "distance_ambulance_km": dist_amb_km,
                    "temps_ambulance_estime_min": temps_amb_min,

                    # hôpital choisi (peut être None si Membre 2 n'a pas encore branché hopitaux)
                    "id_hopital": meilleur_hop["id_hopital"] if meilleur_hop else None,
                    "nom_hopital": meilleur_hop["nom_hopital"] if meilleur_hop else None,
                    "ville_hopital": meilleur_hop["ville"] if meilleur_hop else None,
                    "distance_hopital_km": dist_hop_km,
                    "saturation_hopital": saturation,
                    "temps_hopital_estime_min": temps_hop_min,

                    "timestamp_dispatch": now_iso(),
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
