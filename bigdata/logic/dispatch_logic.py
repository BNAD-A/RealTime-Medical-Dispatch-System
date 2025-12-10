from datetime import datetime, timezone
from math import radians, sin, cos, asin, sqrt
from typing import List, Dict, Optional

# --- Paramètres métiers ---
MAX_AMBULANCE_DISTANCE_KM = 40.0   # On cherche une ambulance dans les 40km
MAX_HOPITAL_SATURATION = 0.98      # On évite les hôpitaux pleins à 98%
VITESSE_MOYENNE_KM_H = 40.0        
MAX_DISTANCE_REGIONALE_KM = 80.0   # Au-delà de 80km, on considère que c'est trop loin

# ---------------------------------------------------------------------------
# Utils géo
# ---------------------------------------------------------------------------

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    try:
        lat1, lon1, lat2, lon2 = map(float, [lat1, lon1, lat2, lon2])
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        return 6371.0 * c
    except Exception:
        return 9999.0

def estimate_travel_time_min(distance_km: float) -> float:
    if distance_km <= 0: return 0.0
    return (distance_km / VITESSE_MOYENNE_KM_H) * 60.0

# ---------------------------------------------------------------------------
# Sélection Ambulance
# ---------------------------------------------------------------------------

def choisir_meilleure_ambulance(appel: Dict, ambulances: List[Dict]) -> Optional[Dict]:
    if not ambulances: return None
    
    lat_p = appel.get("latitude_patient", appel.get("latitude"))
    lon_p = appel.get("longitude_patient", appel.get("longitude"))
    if lat_p is None or lon_p is None: return None

    candidates = []
    for amb in ambulances:
        # On ne prend que les disponibles
        if amb.get("statut") != "disponible": continue
        
        lat_a = amb.get("latitude")
        lon_a = amb.get("longitude")
        if lat_a is None or lon_a is None: continue

        dist = haversine_km(lat_a, lon_a, lat_p, lon_p)
        
        # Filtre distance stricte pour l'ambulance
        if dist < MAX_AMBULANCE_DISTANCE_KM:
            amb_copy = amb.copy()
            amb_copy["distance_ambulance_km"] = dist
            candidates.append(amb_copy)

    if not candidates: return None
    
    # On prend la plus proche
    candidates.sort(key=lambda x: x["distance_ambulance_km"])
    return candidates[0]

# ---------------------------------------------------------------------------
# Sélection Hôpital
# ---------------------------------------------------------------------------

def choisir_meilleur_hopital(appel: Dict, hopitaux: List[Dict]) -> Optional[Dict]:
    """
    Logique robuste :
    1. Priorité absolue : Hôpital proche (<80km) ET Non Saturé.
    2. Fallback 1 : Hôpital proche (<80km) MAIS Saturé.
    3. Fallback 2 : L'hôpital le plus proche géographiquement (même si loin).
    """
    if not hopitaux: return None

    lat_p = appel.get("latitude_patient", appel.get("latitude"))
    lon_p = appel.get("longitude_patient", appel.get("longitude"))
    if lat_p is None or lon_p is None: return None

    candidates = []

    for hop in hopitaux:
        lat_h = hop.get("latitude")
        lon_h = hop.get("longitude")
        if lat_h is None or lon_h is None: continue
        
        # Récupération sécurisée saturation
        sat = hop.get("taux_saturation_hopital") or hop.get("taux_saturation") or 0.0
        sat = float(sat)
        
        dist = haversine_km(lat_h, lon_h, lat_p, lon_p)
        temps = estimate_travel_time_min(dist)
        
        # Score penalise la distance et la saturation
        score = dist * (1 + (sat ** 2))

        candidates.append({
            "hopital": hop,
            "dist": dist,
            "sat": sat,
            "temps": temps,
            "score": score
        })

    if not candidates: return None

    # --- NIVEAU 1 : Le choix idéal ---
    ideal = [c for c in candidates if c["dist"] < MAX_DISTANCE_REGIONALE_KM and c["sat"] < MAX_HOPITAL_SATURATION]
    if ideal:
        ideal.sort(key=lambda x: x["score"])
        best = ideal[0]
    
    # --- NIVEAU 2 : Le choix contraint ---
    elif any(c["dist"] < MAX_DISTANCE_REGIONALE_KM for c in candidates):
        proches = [c for c in candidates if c["dist"] < MAX_DISTANCE_REGIONALE_KM]
        proches.sort(key=lambda x: x["sat"]) 
        best = proches[0]
        
    # --- NIVEAU 3 : Le choix "Au secours" ---
    else:
        candidates.sort(key=lambda x: x["dist"])
        best = candidates[0]

    # Construction objet retour
    hop_final = best["hopital"].copy()
    hop_final["distance_hopital_km"] = best["dist"]
    hop_final["temps_hopital_estime_min"] = best["temps"]
    hop_final["taux_saturation_hopital"] = best["sat"]
    
    return hop_final

# ---------------------------------------------------------------------------
# Construction Event Dispatch
# ---------------------------------------------------------------------------

def construire_evenement_dispatch(id_dispatch, appel, ambulance, hopital) -> Dict:
    id_appel = appel.get("id_appel") or appel.get("id")
    
    event = {
        "id_dispatch": id_dispatch,
        "id_appel": id_appel,
        "ville": appel.get("ville_patient") or appel.get("ville"),
        "gravite": appel.get("gravite", 1),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    # Ambulance info
    if ambulance:
        event["id_ambulance"] = ambulance.get("id_ambulance")
        event["distance_ambulance_km"] = round(ambulance.get("distance_ambulance_km", 0), 2)
    else:
        event["id_ambulance"] = "AUCUNE_DISPO"
        event["distance_ambulance_km"] = 0

    # Hopital info
    if hopital:
        event["id_hopital"] = hopital.get("id_hopital")
        event["nom_hopital"] = hopital.get("nom")
        event["distance_hopital_km"] = round(hopital.get("distance_hopital_km", 0), 2)
        event["taux_saturation_hopital"] = hopital.get("taux_saturation_hopital", 0)
        
        # Calcul temps total estimé
        t_amb = (event["distance_ambulance_km"] / 40) * 60
        t_hop = (event["distance_hopital_km"] / 40) * 60
        event["temps_estime_intervention_min"] = round(t_amb + t_hop + 10, 1) # +10min prise en charge
    else:
        event["id_hopital"] = "INCONNU"
        event["nom_hopital"] = "INCONNU"
        event["temps_estime_intervention_min"] = 0

    return event