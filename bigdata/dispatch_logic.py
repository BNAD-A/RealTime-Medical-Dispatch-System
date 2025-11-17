from utils_geo import haversine_km


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
