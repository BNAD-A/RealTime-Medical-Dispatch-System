# 🚀 Guide d’exécution — RESCUE STREAM (Windows + WSL)

Ce document explique étape par étape comment lancer **RESCUE STREAM** sur un environnement **Windows (Kafka + scripts Python)** et **WSL/Ubuntu (Airflow)**.

> ✅ Objectif : générer les flux temps réel (Kafka), produire les CSV, puis exécuter les traitements orchestrés (Airflow).

---

## Pré-requis

### Windows
- Java installé (JDK recommandé)
- Kafka installé (ex: `C:\kafka\kafka_2.13-3.6.0`)
- Python 3.x installé
- `pip` fonctionnel

### WSL (Ubuntu)
- Airflow installé dans un environnement virtuel
- Accès au projet via `/mnt/c/...`

---

## 🧹 0) Nettoyer les logs Kafka (Windows)

Avant de relancer Kafka, il est recommandé de nettoyer les dossiers de logs :

```powershell
Get-ChildItem C:\tmp

Remove-Item -Recurse -Force C:\tmp\kafka-logs
Remove-Item -Recurse -Force C:\tmp\zookeeper

dir C:\tmp
````

---

## 🟦 1) Démarrer Zookeeper (Windows)

Ouvre **une première fenêtre PowerShell** :

```powershell
cd C:\kafka\kafka_2.13-3.6.0
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
```

## 🟩 2) Démarrer Kafka Broker (Windows)

Ouvre **une deuxième fenêtre PowerShell** :

```powershell
cd C:\kafka\kafka_2.13-3.6.0
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

## 🚑 3) Lancer les Producers (Windows)

Dans un terminal PowerShell :

```powershell
cd C:\Users\pc\Projects\Urgences\bigdata

python create_force_ambulances.py
python .\producers\producer_hopitaux.py
python .\producers\producer_appels.py
python .\producers\producer_ambulances.py
```

## 🧠 4) Lancer le service Dispatch (Windows)

```powershell
cd C:\Users\pc\Projects\Urgences\bigdata
python -m services.service_dispatch
```

---

## 📥 5) Lancer les Consumers (Windows)

Dans un terminal PowerShell :

```powershell
cd C:\Users\pc\Projects\Urgences\bigdata

python .\consumers\consumer_hopitaux_to_csv.py
python .\consumers\consumer_appels_to_csv.py
python .\consumers\consumer_ambulances_to_csv.py
python .\consumers\consumer_dispatch_to_csv.py

python prepare_structured_dispatch.py
```

## 🌬 6) Lancer Airflow (WSL/Ubuntu)

Ouvre WSL / Ubuntu.

### 🟧 6.1 Activer l’environnement virtuel

```bash
cd /mnt/c/Users/pc/Projects/Urgences/bigdata
source ~/airflow/venv/bin/activate
```

---

### 🟦 6.2 Démarrer le Scheduler

```bash
airflow scheduler
```

---

### 🟩 6.3 Démarrer le Webserver

Dans un **second terminal WSL** :

```bash
airflow webserver
```

Puis ouvre :

👉 [http://localhost:8080](http://localhost:8080)

---

## 🔁 7) Déclencher les DAGs (optionnel)

Toujours dans WSL :

```bash
airflow dags trigger bigdata_prepare_hopitaux_dag
airflow dags trigger bigdata_prepare_appels_dag
airflow dags trigger bigdata_prepare_dispatch_dag
```

---

> 🚑 **Un flux, une décision, une vie sauvée.**


