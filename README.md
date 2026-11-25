# Plateforme Big Data pour un SaaS Multi-Tenant : Analyse des Activités en Temps Réel
> **Projet Écosystème Big Data 2 — 5ème année EMSI (2026–2027)**  
> **Technologies :** Apache Spark 3.5, Apache Kafka (KRaft), MinIO (S3), Delta Lake, GraphFrames & GraphX (Scala).

---

## 1. Vue d'Ensemble de l'Architecture

La plateforme analytique est conçue pour traiter et auditer en continu les événements d'une application SaaS multi-tenant sans surcharger la base transactionnelle PostgreSQL.

```
+---------------------------------------------------------------------------------------+
|                                FLUX DE DONNÉES DE BOUT EN BOUT                        |
+---------------------------------------------------------------------------------------+
|                                                                                       |
|   [producer.py] --(mode kafka 20 msg/s)--> [Kafka Topic: orders (3 partitions)]      |
|                                                       |                               |
|                                                       v                               |
|                                         [Spark Structured Streaming]                  |
|                                                       |                               |
|                     +---------------------------------+-------------------------+     |
|                     |                                 |                         |     |
|                     v                                 v                         v     |
|              [Couche Bronze]                   [Couche Silver]            [Couche Gold] |
|            MinIO: orders_raw/                MinIO: orders_clean/      Delta Lake: kpi/|
|              (JSON Bruts)                   (Parquet enrichi city/date) (Tumbling 10m) |
|                                                                                       |
|   [Graphe SaaS Multi-Tenant]                                                          |
|   Sommets : Users, Roles, Resources | Arêtes : HAS_ROLE, CAN_ACCESS                   |
|   -> PySpark GraphFrames : Détection de fuites inter-tenants & motifs .find()         |
|   -> Scala GraphX : PageRank, Connected Components & Degrés distribués                |
|   -> MinIO Gold : lake/gold/graph_analysis/                                           |
+---------------------------------------------------------------------------------------+
```

---

## 2. Organisation du Référentiel

```text
projet-bigdata-saas/
├── docker-compose.yml              # Cluster distribué : Kafka KRaft, MinIO, Spark Master + 2 Workers
├── .gitignore                      # Exclusion propre des environnements et caches lourds
├── README.md                       # Guide officiel de déploiement et d'évaluation
├── Driver/                         # Espace de travail de l'ingénieur de données
│   ├── producer.py                 # Générateur bi-mode (Mode File 50k & Mode Kafka 20 msg/s)
│   ├── restaurants.json            # Référentiel statique de 200 restaurants
│   ├── orders_history.jsonl        # Données batch historiques (50 000 commandes + anomalies)
│   ├── generate_saas_graph_data.py # Générateur du modèle RBAC multi-tenant
│   ├── graph_vertices.jsonl        # Sommets multi-tenants (Users, Roles, Resources)
│   ├── graph_edges.jsonl           # Arêtes multi-tenants (HAS_ROLE, CAN_ACCESS, anomalies)
│   ├── partie1.ipynb               # Partie 1 : Cluster Standalone, MinIO & Kafka
│   ├── partie2.ipynb               # Partie 2 : Traitement Batch SparkCore (RDD)
│   ├── partie3_sql.ipynb           # Partie 3 : Spark SQL, Benchmark UDF/Natif & Explain Catalyst
│   ├── partie4_streaming.ipynb     # Partie 4 : Structured Streaming depuis Kafka & Watermark
│   ├── partie5_production.ipynb    # Partie 5 : Pipeline Production, Fenêtrage & Delta Lake
│   ├── partie6_graphframes.ipynb   # Partie 6 : Analyse GraphFrames PySpark & Isolation SaaS
│   ├── GraphXAnalysis.scala        # Application Scala GraphX distribuée
│   └── run_graphx.scala            # Script Scala exécutable directement dans spark-shell
```

---

## 3. Prérequis et Versions Compatibles

- **Machine hôte :** macOS ARM64 (Apple Silicon M1/M2/M3/M4) ou Linux x86_64 / Windows WSL2.
- **Docker & Docker Compose :** Docker Desktop avec au moins 3.5 Go de RAM allouée.
- **Java :** OpenJDK 17.
- **Apache Spark :** Version 3.5.0.
- **Scala :** Version 2.12.18 (intégrée dans Spark 3.5.0).
- **Python :** Version 3.11 avec PySpark 3.5.0.

---

## 4. Guide de Démarrage Rapide

### Étape 1 : Démarrer l'infrastructure Docker
```bash
docker compose up -d
```
Vérifiez que tous les services sont actifs :
- **Spark Master UI :** [http://localhost:8080](http://localhost:8080) (affiche 2 Workers actifs).
- **MinIO Console :** [http://localhost:9001](http://localhost:9001) (`admin` / `password123`).
- **MinIO S3 API :** `http://localhost:9000`.
- **Kafka Broker :** `localhost:9092`.

---

### Étape 2 : Préparer les données batch historiques (Parties 2 et 3)

1. **Créer le topic Kafka `orders` (3 partitions imposées) :**
```bash
docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh --create --topic orders --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
```

2. **Générer les 50 000 commandes batch et le référentiel restaurants :**
```bash
cd Driver
python producer.py --mode file --n 50000
cd ..
```
*Génère `orders_history.jsonl` (50 000 commandes + 2% de doublons + 5% de retards) et `restaurants.json` (200 restaurants).*

3. **Charger les données dans MinIO (`lake/bronze/history/`) :**
```bash
python -c "
from minio import Minio
client = Minio('localhost:9000', 'admin', 'password123', secure=False)
if not client.bucket_exists('lake'):
    client.make_bucket('lake')
client.fput_object('lake', 'bronze/history/orders_history.jsonl', 'Driver/orders_history.jsonl')
client.fput_object('lake', 'bronze/history/restaurants.json', 'Driver/restaurants.json')
print('[OK] Fichiers batch charges avec succes dans lake/bronze/history/ sur MinIO')
"
```

---

### Étape 3 : Exécuter les Notebooks sous VS Code

Ouvrez VS Code, sélectionnez le kernel Python de votre environnement (`.venv`) et exécutez les notebooks dans l'ordre pédagogique :

1. **`partie1.ipynb`** :
   - Connexion SparkSession à `spark://localhost:7077`.
   - Affichage de `sc.master`, `sc.defaultParallelism` et test distribué sur les 2 workers.
   - Validation de l'accès au bucket MinIO `lake` et au topic Kafka `orders`.
2. **`partie2.ipynb`** :
   - Manipulation bas niveau SparkCore RDD sur MinIO (`s3a://lake/bronze/history/orders_history.jsonl`).
   - Calcul du CA et du panier moyen par ville en **un seul passage** (`reduceByKey`).
   - Top 5 restaurants et affichage de l'arbre de lignée (`toDebugString`).
3. **`partie3_sql.ipynb`** :
   - Schémas stricts sans inférence (`StructType`).
   - Jointure avec le référentiel restaurants et agrégations par catégorie et ville.
   - **Benchmark réel UDF vs Natif** (`when/otherwise`) démontrant une accélération de ~3x grâce à Catalyst et Tungsten.
   - Écriture en Parquet partitionné par `city` dans `lake/silver/orders_history/`.
   - Analyse du plan d'exécution `explain(True)` (BroadcastHashJoin).

---

### Étape 4 : Exécuter le Streaming Kafka et Delta Lake (Parties 4 et 5)

1. **Lancez le producteur streaming Kafka dans un terminal :**
   ```bash
   cd Driver
   python producer.py --mode kafka --rate 20
   ```
   *Publie 20 commandes par seconde en temps réel dans Kafka.*

2. **Exécutez `partie4_streaming.ipynb` :**
   - Ingestion streaming avec `spark.readStream`.
   - Double sink : affichage console micro-batches et archivage JSON brut dans `lake/bronze/orders_raw/`.
   - Gestion du watermark de 10 minutes (rejet des retards > 10 min).

3. **Exécutez `partie5_production.ipynb` :**
   - Dédoublonnage streaming avec `dropDuplicates(['order_id', 'status'])`.
   - Sink Silver Parquet (`lake/silver/orders_clean/`) partitionné par `city` et `date`.
   - Fenêtrages temporels : Tumbling 10m (CA/Volume), Sliding 10m/5m (Taux d'annulation), Session window 5m (Coursiers).
   - Sink Gold **Delta Lake** transactionnel (`lake/gold/kpi_city_10min/`).
   - Audit Time-Travel ACID avec `DESCRIBE HISTORY`.

---

### Étape 5 : Analyse de Graphes SaaS Multi-Tenant (Partie 6)

1. **Générez le jeu de données RBAC Multi-Tenant :**
   ```bash
   python Driver/generate_saas_graph_data.py
   ```

2. **Exécutez l'analyse PySpark GraphFrames :**
   - Ouvrez et exécutez `partie6_graphframes.ipynb`.
   - Détection de la violation d'isolation inter-tenant (`U_BET_05` vers `R_ALP_Admin`).
   - Recherche de motifs d'accès avec `.find("(u)-[e1]->(r); (r)-[e2]->(res)")`.
   - Calcul des degrés sortants pour repérer l'utilisateur à privilèges excessifs (`U_ALP_10`).
   - Export Parquet Gold dans `lake/gold/graph_analysis/`.

3. **Exécutez l'application Scala GraphX officielle :**
   Lancez l'exécution distribuée dans le conteneur Spark Master :
   ```bash
   docker cp Driver/run_graphx.scala spark-master:/opt/spark/
   docker exec spark-master /opt/spark/bin/spark-shell --master "local[*]" -I /opt/spark/run_graphx.scala
   ```
   *Prouve l'équivalence exacte des scores PageRank et composantes connexes entre GraphFrames (PySpark) et GraphX (Scala).*

---

## 5. Synthèse des Preuves de Conformité

| Composant | Exigence PDF | Preuve Technique Fournie |
|---|---|---|
| **Cluster Spark** | Master + 2 Workers Standalone | UI Master port 8080 (2 Workers connectés) + validation dans `partie1.ipynb` |
| **MinIO Lake** | Bronze, Silver, Gold | `bronze/history/`, `silver/orders_history/`, `gold/kpi_city_10min/` vérifiables via l'API S3 |
| **Kafka KRaft** | Topic `orders` (3 partitions) | `kafka-topics.sh --describe` montre PartitionCount: 3 |
| **Producer.py** | Modes `file` et `kafka` | Deux modes testés avec succès (50 000 batch et 20 msg/s streaming) |
| **Benchmark** | UDF vs Natif | Mesure réelle dans `partie3_sql.ipynb` montrant un gain de ~3x en natif |
| **Delta Lake** | Table Gold ACID | Écriture streaming et affichage du journal de transactions via `DESCRIBE HISTORY` |
| **Graphes SaaS** | Multi-tenant RBAC | Détection d'anomalie inter-tenant + équivalence PageRank GraphFrames / GraphX |
| **Scala GraphX** | Code Scala RDD | `GraphXAnalysis.scala` et script `run_graphx.scala` exécuté avec succès |
