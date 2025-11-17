import sys
from pathlib import Path
import unittest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from dispatch_logic import (
    choisir_meilleure_ambulance,
    choisir_meilleur_hopital,
)


class TestDispatchLogic(unittest.TestCase):
    def setUp(self):
        self.etat_ambulances = {
            1: {"id_ambulance": 1, "latitude": 34.0, "longitude": -6.8, "statut": "disponible", "ville": "Rabat"},
            2: {"id_ambulance": 2, "latitude": 33.5, "longitude": -7.6, "statut": "disponible", "ville": "Casablanca"},
            3: {"id_ambulance": 3, "latitude": 35.7, "longitude": -5.8, "statut": "hors_service", "ville": "Tanger"},
        }

        self.etat_hopitaux = {
            1: {"id_hopital": 1, "nom_hopital": "CHU Rabat", "ville": "Rabat", "capacite_totale": 100, "lits_occupees": 80},
            2: {"id_hopital": 2, "nom_hopital": "CHU Casa", "ville": "Casablanca", "capacite_totale": 150, "lits_occupees": 50},
        }

    def test_choix_ambulance_la_plus_proche(self):
        appel = {
            "latitude": 34.02,  # proche de Rabat
            "longitude": -6.85,
        }

        amb, dist = choisir_meilleure_ambulance(self.etat_ambulances, appel)

        self.assertIsNotNone(amb)
        self.assertEqual(amb["id_ambulance"], 1)  # Rabat doit être choisie
        self.assertIsNotNone(dist)
        self.assertGreater(dist, 0.0)

    def test_aucune_ambulance_disponible(self):
        etat = {
            1: {"id_ambulance": 1, "latitude": 34.0, "longitude": -6.8, "statut": "hors_service"},
        }
        appel = {"latitude": 34.0, "longitude": -6.8}

        amb, dist = choisir_meilleure_ambulance(etat, appel)
        self.assertIsNone(amb)
        self.assertIsNone(dist)

    def test_choix_hopital_plus_de_lits_dispo(self):
        appel = {"ville": "Rabat"}

        hop = choisir_meilleur_hopital(self.etat_hopitaux, appel)

        self.assertIsNotNone(hop)
        self.assertEqual(hop["id_hopital"], 2)


if __name__ == "__main__":
    unittest.main()
