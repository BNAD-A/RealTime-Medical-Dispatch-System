

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
from utils_geo import haversine_km
from dispatch_logic import (
    choisir_meilleure_ambulance,
    choisir_meilleur_hopital,
)


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def choisir_meilleure_ambulance(etat_ambulances, appel):
    """
    Choisit l'ambulance disponible la plus proche du lieu de l'appel.
    etat_ambulances : dict[id_ambulance -> dict]
    appel : dict
    """
    if not etat_ambulances:
        return None, None 

    lat_pat = appel["latitude"]
    lon_pat = appel["longitude"]

    meilleure_amb = None
    meilleure_dist = None

    for amb in etat_ambulances.values():
        if amb.get("statut") not in ("disponible", "en_route"):
            continue  

        dist = haversine_km(lat_pat, lon_pat, amb["latitude"], amb["longitude"])

        if meilleure_dist is None or dist < meilleure_dist:
            meilleure_dist = dist
            meilleure_amb = amb

    return meilleure_amb, meilleure_dist


def choisir_meilleur_hopital(etat_hopitaux, appel):
    """
    V1 simple :
    - si on a des hôpitaux => choisir celui avec le plus de lits disponibles
      (capacite_totale - lits_occupees)
    - sinon => None
    """
    if not etat_hopitaux:
        return None

    meilleur = None
    meilleur_dispo = None

    for hop in etat_hopitaux.values():
        capacite = hop.get("capacite_totale", 0)
        occ = hop.get("lits_occupees", 0)
        dispo = capacite - occ

        if meilleur_dispo is None or dispo > meilleur_dispo:
            meilleur_dispo = dispo
            meilleur = hop

    return meilleur


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

    etat_ambulances = {}  
    etat_hopitaux = {} 

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
                print(f"[APPEL] Reçu appel {appel['id_appel']} à {appel['ville']} motif={appel['motif_appel']}")

                # 1) choisir ambulance
                meilleure_amb, dist_km = choisir_meilleure_ambulance(etat_ambulances, appel)

                if meilleure_amb is None:
                    print("[WARN] Aucun ambulance disponible pour cet appel.")
                    continue

                # 2) choisir hôpital (si dispo)
                meilleur_hop = choisir_meilleur_hopital(etat_hopitaux, appel)

                # 3) estimer le temps d'arrivée 
                if dist_km is not None:
                    temps_estime_min = dist_km / 40.0 * 60.0
                else:
                    temps_estime_min = None

                dispatch_event = {
                    "id_appel": appel["id_appel"],
                    "ville_appel": appel["ville"],
                    "latitude_appel": appel["latitude"],
                    "longitude_appel": appel["longitude"],
                    "motif_appel": appel["motif_appel"],
                    "gravite": appel["gravite"],
                    "source_appel": appel["source_appel"],

                    "id_ambulance": meilleure_amb["id_ambulance"],
                    "code_ambulance": meilleure_amb["code_ambulance"],
                    "ville_ambulance": meilleure_amb["ville"],
                    "distance_km": dist_km,
                    "temps_estime_min": temps_estime_min,

                    "id_hopital": meilleur_hop["id_hopital"] if meilleur_hop else None,
                    "nom_hopital": meilleur_hop["nom_hopital"] if meilleur_hop else None,
                    "ville_hopital": meilleur_hop["ville"] if meilleur_hop else None,

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
