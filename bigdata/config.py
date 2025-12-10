KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]

TOPIC_AMBULANCES = "ambulances"
TOPIC_HOPITAUX = "hopitaux"
TOPIC_DISPATCH = "dispatch"
TOPIC_APPELS = "appels"

N_AMBULANCES = 50

VILLES_MAROC = [
    {"ville": "Casablanca", "lat": 33.5731, "lon": -7.5898, "zone": "centre"},
    {"ville": "Rabat",      "lat": 34.0209, "lon": -6.8416, "zone": "centre"},
    {"ville": "Fès",        "lat": 34.0331, "lon": -5.0003, "zone": "nord"},
    {"ville": "Marrakech",  "lat": 31.6295, "lon": -7.9811, "zone": "sud"},
    {"ville": "Tanger",     "lat": 35.7595, "lon": -5.8340, "zone": "nord"},
    {"ville": "Agadir",     "lat": 30.4278, "lon": -9.5981, "zone": "sud"},
    {"ville": "Oujda",      "lat": 34.6814, "lon": -1.9086, "zone": "est"},
    {"ville": "Laayoune",   "lat": 27.1253, "lon": -13.1625, "zone": "sud"},
]
