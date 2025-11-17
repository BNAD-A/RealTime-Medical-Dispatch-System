from utils_geo import haversine_km
from typing import Dict


def choisir_meilleure_ambulance(etat_ambulances, appel):
    """
    Choisit l'ambulance disponible la plus proche du lieu de l'appel.
    etat_ambulances : dict[id_ambulance -> dict]
    appel : dict
    """
    if not etat_ambulances:
        return None, None  # pas d'ambulances connues

    lat_pat = appel["latitude"]
    lon_pat = appel["longitude"]

    meilleure_amb = None
    meilleure_dist = None

    for amb in etat_ambulances.values():
        if amb.get("statut") not in ("disponible", "en_route"):
            continue  # on ignore les ambulances en intervention / hors_service

        dist = haversine_km(lat_pat, lon_pat, amb["latitude"], amb["longitude"])

        if meilleure_dist is None or dist < meilleure_dist:
            meilleure_dist = dist
            meilleure_amb = amb

    return meilleure_amb, meilleure_dist


def choisir_meilleur_hopital(
    etat_hopitaux: Dict,
    appel: Dict,
    max_distance_km: float = 50.0,
    w_distance: float = 0.6,
    w_saturation: float = 0.4,
):
    """
    Choisit l'hôpital en combinant :
    - distance hôpital ↔ patient
    - saturation = lits_occupees / capacite_totale

    Score = w_distance * distance_norm + w_saturation * saturation

    Retourne TOUJOURS un triple :
      (hopital_dict or None, distance_km or None, saturation or None)
    """
    if not etat_hopitaux:
        return None, None, None

    lat_pat = appel["latitude"]
    lon_pat = appel["longitude"]

    meilleur_hop = None
    meilleur_score = None
    meilleur_dist = None
    meilleur_sat = None

    for hop in etat_hopitaux.values():
        capacite = hop.get("capacite_totale", 0)
        occ = hop.get("lits_occupees", 0)
        if capacite <= 0:
            continue

        saturation = occ / capacite 

        dist_km = haversine_km(lat_pat, lon_pat, hop["latitude"], hop["longitude"])
        distance_norm = min(dist_km / max_distance_km, 1.0)

        score = w_distance * distance_norm + w_saturation * saturation

        if meilleur_score is None or score < meilleur_score:
            meilleur_score = score
            meilleur_hop = hop
            meilleur_dist = dist_km
            meilleur_sat = saturation

    if meilleur_hop is None:
        return None, None, None

    return meilleur_hop, meilleur_dist, meilleur_sat

