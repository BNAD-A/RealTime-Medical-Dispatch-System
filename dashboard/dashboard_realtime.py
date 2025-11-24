import json
import threading
from math import radians, sin, cos, asin, sqrt
from typing import List, Dict

import pandas as pd
from kafka import KafkaConsumer

from dash import Dash, dcc, html, dash_table
from dash.dependencies import Input, Output
import plotly.express as px

# ===========================
# CONFIG
# ===========================
KAFKA_BOOTSTRAP = ["localhost:9092"]
DISPATCH_TOPIC = "dispatch"

# Adaptation aux chemins de ton projet
HOPITAUX_CSV = r"C:\Users\pc\Projects\Urgences\bigdata\data\structured\hopitaux_structured.csv"
AMBULANCES_CSV = r"C:\Users\pc\Projects\Urgences\bigdata\data\structured\ambulances_clean.csv"

# ===========================
# CHARGEMENT DES DONNÉES DE RÉFÉRENCE
# ===========================
hopitaux_df = pd.read_csv(HOPITAUX_CSV)
ambulances_df = pd.read_csv(AMBULANCES_CSV)

# On prépare des index pour retrouver vite un hôpital/ambulance
hopitaux_index = hopitaux_df.set_index("id_hopital")
ambulances_index = ambulances_df.set_index("id_ambulance")


# ===========================
# BUFFER GLOBAL POUR LES DISPATCHS
# ===========================
dispatch_events: List[Dict] = []
dispatch_lock = threading.Lock()
MAX_EVENTS_STORED = 500  # on limite un peu pour éviter de manger trop de RAM


# ===========================
# FONCTIONS UTILITAIRES
# ===========================
def haversine_km(lat1, lon1, lat2, lon2):
    """Distance en km entre 2 points GPS."""
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return 6371.0 * c


# ===========================
# THREAD KAFKA CONSUMER
# ===========================
def kafka_dispatch_consumer():
    """
    Thread qui écoute le topic 'dispatch' en continu
    et stocke les événements dans dispatch_events.
    """
    print("[DASHBOARD] Démarrage du KafkaConsumer sur le topic 'dispatch'...")
    consumer = KafkaConsumer(
        DISPATCH_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="dashboard-dispatch-group",
    )

    for msg in consumer:
        event = msg.value
        with dispatch_lock:
            dispatch_events.append(event)
            # on ne garde que les N derniers
            if len(dispatch_events) > MAX_EVENTS_STORED:
                dispatch_events.pop(0)


# Lancer le thread Kafka dès le démarrage
consumer_thread = threading.Thread(target=kafka_dispatch_consumer, daemon=True)
consumer_thread.start()


# ===========================
# CRÉATION DE L'APP DASH
# ===========================
app = Dash(__name__)
app.title = "Dashboard Urgences - Temps Réel"

app.layout = html.Div(
    style={"fontFamily": "Arial", "margin": "10px"},
    children=[
        html.H1("🚑 Dashboard Urgences – Temps réel", style={"textAlign": "center"}),

        html.Div(
            style={"display": "flex", "gap": "20px"},
            children=[
                html.Div(
                    style={"flex": "1"},
                    children=[
                        html.H3("Derniers dispatchs"),
                        dash_table.DataTable(
                            id="dispatch-table",
                            columns=[
                                {"name": "Heure", "id": "timestamp"},
                                {"name": "Appel", "id": "id_appel"},
                                {"name": "Ambulance", "id": "id_ambulance"},
                                {"name": "Hôpital", "id": "nom_hopital"},
                                {"name": "Ville hôpital", "id": "ville_hopital"},
                                {"name": "Priorité", "id": "priorite"},
                                {"name": "Distance (km)", "id": "distance_ambulance_km"},
                                {"name": "Temps hôpital (min)", "id": "temps_hopital_estime_min"},
                            ],
                            data=[],
                            page_size=10,
                            style_table={"overflowX": "auto"},
                            style_cell={"fontSize": 12, "padding": "3px"},
                        ),
                    ],
                ),
                html.Div(
                    style={"flex": "1"},
                    children=[
                        html.H3("Dispatchs sur la carte (patient → hôpital)"),
                        dcc.Graph(id="map-dispatch"),
                    ],
                ),
            ],
        ),

        html.Hr(),
        html.Div(
            style={"display": "flex", "gap": "20px"},
            children=[
                html.Div(
                    style={"flex": "1"},
                    children=[
                        html.H3("Saturation des hôpitaux (top 10)"),
                        dcc.Graph(id="bar-saturation"),
                    ],
                ),
                html.Div(
                    style={"flex": "1"},
                    children=[
                        html.H3("Nombre de dispatchs par ville d'hôpital"),
                        dcc.Graph(id="bar-dispatch-par-ville"),
                    ],
                ),
            ],
        ),

        # Interval pour rafraîchir toutes les 2 minutes
        dcc.Interval(id="interval-refresh", interval=120000, n_intervals=0),
    ],
)


# ===========================
# CALLBACK PRINCIPAL
# ===========================
@app.callback(
    [
        Output("dispatch-table", "data"),
        Output("map-dispatch", "figure"),
        Output("bar-saturation", "figure"),
        Output("bar-dispatch-par-ville", "figure"),
    ],
    Input("interval-refresh", "n_intervals"),
)
def update_dashboard(n):
    # Copier les événements en mémoire locale
    with dispatch_lock:
        events = list(dispatch_events)

    if not events:
        # Aucun dispatch encore
        empty_fig = px.scatter_mapbox(
            lat=[], lon=[], zoom=5,
            height=400,
            mapbox_style="open-street-map",
        )
        bar_empty = px.bar()
        return [], empty_fig, bar_empty, bar_empty

    df = pd.DataFrame(events)

    # Assurer les colonnes attendues
    for col in ["id_dispatch", "id_appel", "id_ambulance", "id_hopital",
                "timestamp", "priorite", "distance_ambulance_km",
                "temps_hopital_estime_min"]:
        if col not in df.columns:
            df[col] = None

    # Joindre avec les hôpitaux (pour noms + coords)
    df = df.merge(
        hopitaux_df[["id_hopital", "nom", "ville", "latitude", "longitude", "taux_saturation"]],
        on="id_hopital",
        how="left",
        suffixes=("", "_hop"),
    )

    df.rename(
        columns={
            "nom": "nom_hopital",
            "ville": "ville_hopital",
            "latitude": "latitude_hopital",
            "longitude": "longitude_hopital",
        },
        inplace=True,
    )

    # TABLE : 10 derniers dispatchs
    df_sorted = df.sort_values("timestamp", ascending=False)
    table_cols = [
        "timestamp",
        "id_appel",
        "id_ambulance",
        "nom_hopital",
        "ville_hopital",
        "priorite",
        "distance_ambulance_km",
        "temps_hopital_estime_min",
    ]
    table_data = df_sorted[table_cols].head(10).to_dict("records")

    # CARTE : patients + hôpitaux
    # On suppose que df contient latitude_patient / longitude_patient
    if "latitude_patient" not in df.columns:
        df["latitude_patient"] = None
    if "longitude_patient" not in df.columns:
        df["longitude_patient"] = None

    # Data pour la carte : points patients et hôpitaux
    map_df_patients = df[["id_dispatch", "latitude_patient", "longitude_patient", "priorite"]].dropna()
    map_df_patients = map_df_patients.rename(
        columns={"latitude_patient": "lat", "longitude_patient": "lon"}
    )
    map_df_patients["type"] = "patient"

    map_df_hop = df[["id_dispatch", "nom_hopital", "ville_hopital",
                     "latitude_hopital", "longitude_hopital", "priorite"]].dropna()
    map_df_hop = map_df_hop.rename(
        columns={"latitude_hopital": "lat", "longitude_hopital": "lon"}
    )
    map_df_hop["type"] = "hopital"

    map_df = pd.concat([map_df_patients, map_df_hop], ignore_index=True)

    fig_map = px.scatter_mapbox(
        map_df,
        lat="lat",
        lon="lon",
        color="type",
        hover_name="id_dispatch",
        hover_data={"priorite": True, "type": True},
        zoom=5,
        height=400,
        mapbox_style="open-street-map",
    )

    # BAR 1 : top 10 hôpitaux les plus saturés
    hop_sat = (
        hopitaux_df.sort_values("taux_saturation", ascending=False)
        .head(10)
    )
    fig_bar_sat = px.bar(
        hop_sat,
        x="nom",
        y="taux_saturation",
        title="Top 10 hôpitaux les plus saturés",
    )
    fig_bar_sat.update_layout(xaxis_title="Hôpital", yaxis_title="Taux de saturation")

    # BAR 2 : nombre de dispatchs par ville d'hôpital
    disp_by_city = (
        df.dropna(subset=["ville_hopital"])
        .groupby("ville_hopital")["id_dispatch"]
        .count()
        .reset_index()
        .rename(columns={"id_dispatch": "nb_dispatchs"})
        .sort_values("nb_dispatchs", ascending=False)
        .head(10)
    )
    fig_bar_city = px.bar(
        disp_by_city,
        x="ville_hopital",
        y="nb_dispatchs",
        title="Nombre de dispatchs par ville d'hôpital (top 10)",
    )
    fig_bar_city.update_layout(xaxis_title="Ville", yaxis_title="Nb de dispatchs")

    return table_data, fig_map, fig_bar_sat, fig_bar_city


# ===========================
# MAIN
# ===========================
if __name__ == "__main__":
    # Dashboard en local sur http://127.0.0.1:8050
    app.run(host="0.0.0.0", port=8050, debug=True)
