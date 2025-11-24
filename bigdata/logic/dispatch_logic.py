from math import radians, sin, cos, asin, sqrt
from typing import List, Dict, Optional

# --- Paramètres métiers ---
MAX_AMBULANCE_DISTANCE_KM = 30.0   # distance max ambulance → patient
MAX_HOPITAL_SATURATION = 0.95      # saturation max acceptée (95%)
VITESSE_MOYENNE_KM_H = 40.0        # pour estimer le temps vers l'hôpital


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance en km entre 2 points GPS."""
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return 6371.0 * c


def estimate_travel_time_min(distance_km: float) -> float:
    """Temps estimé en minutes à partir d'une distance en km."""
    if distance_km <= 0:
        return 0.0
    return (distance_km / VITESSE_MOYENNE_KM_H) * 60.0


def select_best_ambulance(
    appel: Dict,
    ambulances: List[Dict],
) -> Optional[Dict]:
    """
    Sélectionne la meilleure ambulance :
    - même ville que l'appel si possible
    - sinon même zone
    - statut 'disponible' uniquement
    - distance <= MAX_AMBULANCE_DISTANCE_KM
    """
    lat_p = appel["latitude_patient"]
    lon_p = appel["longitude_patient"]
    ville_p = appel.get("ville_patient")
    zone_p = appel.get("zone_intervention") or appel.get("zone")

    candidates = []

    for amb in ambulances:
        if amb.get("statut") != "disponible":
            continue

        # Filtrer par ville/zone
        score_zone = 0
        if ville_p and amb.get("ville") == ville_p:
            score_zone += 2   # même ville
        if zone_p and amb.get("zone") == zone_p:
            score_zone += 1   # même zone

        lat_a = amb["latitude"]
        lon_a = amb["longitude"]
        dist_km = haversine_km(lat_a, lon_a, lat_p, lon_p)

        # Filtrer sur distance max
        if dist_km > MAX_AMBULANCE_DISTANCE_KM:
            continue

        candidates.append({
            "ambulance": amb,
            "distance_km": dist_km,
            "score_zone": score_zone,
        })

    if not candidates:
        return None  # Aucune ambulance dans le rayon autorisé

    # Trier : 1) meilleur score de zone, 2) distance croissante
    candidates.sort(key=lambda x: (-x["score_zone"], x["distance_km"]))

    best = candidates[0]
    result = best["ambulance"].copy()
    result["distance_ambulance_km"] = best["distance_km"]
    return result


def select_best_hopital(
    appel: Dict,
    hopitaux: List[Dict],
) -> Optional[Dict]:
    """
    Sélectionne le meilleur hôpital :
    - saturation < MAX_HOPITAL_SATURATION
    - même ville si possible, sinon même zone
    - compromis distance / saturation
    """
    lat_p = appel["latitude_patient"]
    lon_p = appel["longitude_patient"]
    ville_p = appel.get("ville_patient")
    zone_p = appel.get("zone_intervention") or appel.get("zone")

    candidates = []

    for hop in hopitaux:
        saturation = hop.get("taux_saturation_hopital")
        if saturation is None:
            continue

        # Refuser les hôpitaux trop saturés
        if saturation >= MAX_HOPITAL_SATURATION:
            continue

        lat_h = hop["latitude"]
        lon_h = hop["longitude"]
        dist_km = haversine_km(lat_h, lon_h, lat_p, lon_p)
        temps_min = estimate_travel_time_min(dist_km)

        score_zone = 0
        if ville_p and hop.get("ville") == ville_p:
            score_zone += 2
        if zone_p and hop.get("zone") == zone_p:
            score_zone += 1

        # Score global : on veut peu de temps ET peu de saturation
        # -> plus petit "score" = meilleur
        score_global = temps_min * 0.7 + saturation * 100 * 0.3

        candidates.append({
            "hopital": hop,
            "distance_km": dist_km,
            "temps_min": temps_min,
            "saturation": saturation,
            "score_zone": score_zone,
            "score_global": score_global,
        })

    if not candidates:
        return None  # aucun hôpital assez peu saturé

    # D'abord favoriser même ville/zone, puis le plus petit score global
    candidates.sort(key=lambda x: (-x["score_zone"], x["score_global"]))

    best = candidates[0]
    result = best["hopital"].copy()
    result["distance_hopital_km"] = best["distance_km"]
    result["temps_hopital_estime_min"] = best["temps_min"]
    result["taux_saturation_hopital"] = best["saturation"]
    return result
