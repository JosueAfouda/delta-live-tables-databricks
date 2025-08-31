# Delta Live Tables Project – Yellow Taxi Data Pipeline

[Regarder la Vidéo du projet](https://youtu.be/WpclFybAEy0)

## Introduction

Ce projet démontre la mise en place d’un **pipeline de données moderne** avec **Delta Live Tables (DLT)** dans **Databricks**.
À travers un cas concret basé sur les données **Yellow Taxi de New York**, il illustre comment appliquer les bonnes pratiques Data Engineering en mode **Bronze → Silver → Gold**.

Objectif : Passer **de données brutes (raw)** à des **données agrégées et prêtes pour le reporting**, via un pipeline **scalable, fiable et automatisé**.

---

## Architecture du Pipeline

Le pipeline suit l’architecture **médaillée (Medallion Architecture)** :

* **Bronze Layer (Raw)** : ingestion des données sources brutes avec **Auto Loader** (fichiers Parquet + CSV).
* **Silver Layer (Processed)** : nettoyage, enrichissement, application des règles de qualité et jointure avec des dimensions.
* **Gold Layer (Aggregated)** : agrégation des données pour le reporting et la visualisation (routes populaires, revenus, etc.).

<img width="1026" height="338" alt="pipeline_graph" src="https://github.com/user-attachments/assets/7566dba5-3fef-45bc-8275-d73108e052ec" />

---

## ⚙Technologies utilisées

* **Databricks** : plateforme unifiée pour la Data & AI.
* **Delta Live Tables (DLT)** : framework pour créer des pipelines déclaratifs et fiables.
* **Auto Loader** : ingestion incrémentale depuis des fichiers Parquet/CSV.
* **Delta Lake** : stockage transactionnel avec schéma évolutif.
* **SQL + PySpark** : transformations et enrichissement des données.

---

## Tables créées

### 1. `raw_trips` *(Bronze)*

* Ingestion des fichiers Parquet avec Auto Loader.
* Capture du nom du fichier source via `_metadata.file_path`.
* Stockage incrémental avec **gestion de schéma automatique**.

### 2. `raw_taxi_zones` *(Bronze)*

* Ingestion d’un fichier CSV de lookup (zones de taxis).
* Utilisé pour enrichir les données de trajets.

### 3. `processed_trips` *(Silver)*

* Application de règles de qualité (filtrage des anomalies : distances, montants, horaires).
* Jointure avec la table `taxi_zones` pour enrichissement géographique.
* Extraction de features temporelles (date, heure, jour de la semaine).

### 4. `aggregated_data` *(Gold)*

* Agrégation par date, borough et créneau horaire.
* Calcul des KPIs :

  * Nombre de trajets (`COUNT(*)`)
  * Distance moyenne (`AVG(trip_distance)`)
  * Revenu total (`SUM(total_amount)`)
* Utilisation pour dashboards et analyses stratégiques.

---

## Points clés pédagogiques

1. **Ingestion incrémentale** : grâce à Auto Loader, seules les nouvelles données sont traitées.
2. **Data Quality** : règles simples mais essentielles (pas de trajets avec distance ≤ 0, pas de revenus négatifs, etc.).
3. **Architecture Médaillée** : séparation claire entre raw, clean et aggregated data.
4. **Temporal Features** : ajout d’attributs analytiques utiles (heure, jour de la semaine).
5. **Reproductibilité** : grâce à DLT, le pipeline est **déclaratif, testé et maintenable**.

---

## Mise en avant de mon expertise

En construisant ce projet, j’ai démontré ma capacité à :

* **Mettre en œuvre un pipeline complet** dans Databricks avec Delta Live Tables.
* **Appliquer les best practices Data Engineering** : ingestion incrémentale, architecture médaillée, data quality.
* **Aller de la donnée brute au reporting exploitable**, en passant par toutes les étapes de transformation.
* **Concevoir des pipelines modulaires et maintenables**, facilement compréhensibles pour des équipes Data.
* **Former et vulgariser** : j’ai documenté le projet pour qu’il serve aussi de support pédagogique à des Data Engineers juniors.

Ce projet illustre parfaitement ma capacité à allier **expertise technique** et **pédagogie**, deux qualités essentielles pour des missions de **Data Engineering, Data Architecture ou Lead Technique**.

---

## Conclusion

Ce projet est une **mise en pratique concrète** de Delta Live Tables sur Databricks, montrant comment :

* Construire un pipeline robuste et scalable.
* Nettoyer, enrichir et transformer les données.
* Produire des **tables prêtes à l’analyse** pour les métiers.

Si vous êtes recruteur :
Ce projet illustre ma capacité à **concevoir et déployer des solutions Data Engineering modernes** tout en étant capable de **transmettre ces compétences à des équipes juniors**.
