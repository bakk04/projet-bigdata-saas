#!/usr/bin/env python3
"""
Producer de données Big Data pour la plateforme SaaS multi-tenant.
Ce script supporte deux modes imposés par le cahier des charges :
  1. Mode 'file'  : Génère restaurants.json (200 restos) et orders_history.jsonl (50 000 commandes batch).
  2. Mode 'kafka' : Publie en streaming continu dans le topic Kafka 'orders' à la cadence spécifiée (ex: 20 msg/s).

Auteur : Projet Big Data 2 - 5ème année EMSI
"""

import os
import sys
import json
import time
import random
import argparse
from datetime import datetime, timedelta

# Constantes métier
CITIES = ["Casablanca", "Rabat", "Marrakech", "Tanger", "Agadir"]
CATEGORIES = ["pizza", "tacos", "sushi", "marocain", "burger"]
STATUSES = ["CREATED", "PICKED_UP", "DELIVERED", "CANCELLED"]
STATUS_WEIGHTS = [10, 20, 60, 10]  # Répartition réaliste : 60% livrées, 10% annulées, etc.


def generate_restaurants(filepath="restaurants.json", count=200):
    """
    Génère le référentiel statique de 200 restaurants.
    Chaque restaurant a un identifiant unique (R-001 à R-200), une ville, une catégorie et des coordonnées GPS.
    """
    # Fixer la graine pour garantir des restaurants reproductibles
    rng = random.Random(42)
    restaurants = []

    for i in range(1, count + 1):
        city = rng.choice(CITIES)
        category = rng.choice(CATEGORIES)
        # Coordonnées géographiques approximatives au Maroc
        lat = round(rng.uniform(30.0, 35.8), 4)
        lon = round(rng.uniform(-9.5, -5.0), 4)

        rest = {
            "id": f"R-{i:03d}",
            "name": f"Restaurant {i}",
            "city": city,
            "category": category,
            "latitude": lat,
            "longitude": lon
        }
        restaurants.append(rest)

    # Sauvegarde sur disque
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(restaurants, f, indent=2, ensure_ascii=False)

    print(f"[OK] Référentiel restaurants généré : {filepath} ({len(restaurants)} restaurants)")
    return restaurants


def load_or_create_restaurants(filepath="restaurants.json"):
    """Charge le fichier restaurants s'il existe déjà, sinon le génère."""
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                restaurants = json.load(f)
            if len(restaurants) == 200:
                return restaurants
        except Exception:
            pass
    return generate_restaurants(filepath)


def create_order_event(order_seq, restaurants, force_late=False):
    """
    Crée un événement de commande conforme au schéma strict du PDF.
    - order_seq  : numéro d'ordre pour générer order_id unique.
    - force_late : si True, force un retard de 1 à 15 minutes pour simuler un livreur hors réseau.
    """
    rest = random.choice(restaurants)
    status = random.choices(STATUSES, weights=STATUS_WEIGHTS)[0]

    # Gestion de l'horodatage et des retards (5% des cas en moyenne)
    now = datetime.utcnow()
    if force_late:
        delay_minutes = random.randint(1, 15)
        event_time = now - timedelta(minutes=delay_minutes)
    else:
        event_time = now

    # Identifiant coursier avec code ville (ex: L-CAS-07)
    city_code = rest["city"][:3].upper()
    courier_id = f"L-{city_code}-{random.randint(1, 100):02d}"

    # Schéma officiel strict avec "items_count" (et non itemscount)
    order = {
        "order_id": f"ORD-{order_seq:07d}",
        "restaurant_id": rest["id"],
        "customer_id": f"C-{random.randint(1000, 99999):05d}",
        "city": rest["city"],
        "status": status,
        "amount_mad": round(random.uniform(30.0, 500.0), 2),
        "items_count": random.randint(1, 10),
        "distance_km": round(random.uniform(0.5, 15.0), 1),
        "courier_id": courier_id,
        "event_time": event_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    return order


def run_file_mode(n, output_file="orders_history.jsonl"):
    """
    Génère n commandes historiques avec 5% de retards et 2% de doublons stricts.
    Sauvegarde le résultat au format JSON Lines (.jsonl).
    """
    print(f"[MODE FILE] Démarrage de la génération de {n} commandes historiques...")
    restaurants = load_or_create_restaurants("restaurants.json")

    total_events = 0
    duplicate_count = 0
    late_count = 0

    with open(output_file, "w", encoding="utf-8") as f:
        for i in range(1, n + 1):
            is_late = (random.random() < 0.05)
            if is_late:
                late_count += 1

            order = create_order_event(i, restaurants, force_late=is_late)
            f.write(json.dumps(order) + "\n")
            total_events += 1

            # Simulation de 2% de doublons (même order_id et même statut)
            if random.random() < 0.02:
                f.write(json.dumps(order) + "\n")
                total_events += 1
                duplicate_count += 1

    print(f"[OK] Fichier batch généré : {output_file}")
    print(f"     - Commandes distinctes : {n}")
    print(f"     - Événements au total  : {total_events} (dont {duplicate_count} doublons soit ~{duplicate_count/n*100:.1f}%)")
    print(f"     - Événements en retard : {late_count} (~{late_count/n*100:.1f}%)")


def run_kafka_mode(rate=20, topic="orders", bootstrap_servers="localhost:9092"):
    """
    Publie en flux continu dans Kafka à une cadence précise (rate msg/sec).
    Simule 5% d'événements en retard et 2% de doublons en temps réel.
    """
    try:
        from kafka import KafkaProducer
    except ImportError:
        print("[ERREUR] Le module 'kafka-python-ng' n'est pas installé.")
        print("Installez-le avec : pip install kafka-python-ng")
        sys.exit(1)

    print(f"[MODE KAFKA] Connexion au broker Kafka ({bootstrap_servers})...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks=1,
            retries=3
        )
    except Exception as e:
        print(f"[ERREUR] Impossible de se connecter à Kafka : {e}")
        print("Vérifiez que le conteneur kafka-broker est démarré avec 'docker compose up -d'.")
        sys.exit(1)

    restaurants = load_or_create_restaurants("restaurants.json")
    print(f"[MODE KAFKA] Connecté au topic '{topic}' avec un débit de {rate} msg/s.")
    print("Appuyez sur CTRL+C pour arrêter le producteur.\n")

    seq = 1
    delay_between_events = 1.0 / float(rate)
    sent_total = 0
    start_time = time.time()

    try:
        while True:
            cycle_start = time.time()

            is_late = (random.random() < 0.05)
            order = create_order_event(seq, restaurants, force_late=is_late)
            seq += 1

            # Clé de partitionnement basée sur la ville pour une bonne distribution
            key = order["city"].encode("utf-8")
            producer.send(topic, key=key, value=order)
            sent_total += 1

            # 2% de doublons simulés
            if random.random() < 0.02:
                producer.send(topic, key=key, value=order)
                sent_total += 1

            # Log régulier toutes les 50 commandes
            if sent_total % 50 == 0:
                elapsed = time.time() - start_time
                actual_rate = sent_total / elapsed if elapsed > 0 else 0
                print(f" -> {sent_total} messages envoyés | cadence : {actual_rate:.1f} msg/s | dernier: {order['order_id']} ({order['city']})")

            # Régulation de la cadence
            work_time = time.time() - cycle_start
            sleep_time = delay_between_events - work_time
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        print(f"\n[INFO] Arrêt demandé par l'utilisateur (CTRL+C).")
    finally:
        producer.flush()
        producer.close()
        elapsed = time.time() - start_time
        print(f"[OK] Producteur Kafka arrêté. Total messages envoyés : {sent_total} en {elapsed:.1f}s.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Générateur de données pour SaaS Big Data multi-tenant")
    parser.add_argument("--mode", choices=["file", "kafka"], required=True,
                        help="Mode de génération : 'file' pour le batch historique, 'kafka' pour le streaming.")
    parser.add_argument("--n", type=int, default=50000,
                        help="Nombre de commandes en mode file (défaut : 50000).")
    parser.add_argument("--rate", type=int, default=20,
                        help="Débit d'envoi en msg/sec en mode kafka (défaut : 20).")
    parser.add_argument("--topic", type=str, default="orders",
                        help="Topic Kafka cible (défaut : orders).")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092",
                        help="Adresse du broker Kafka (défaut : localhost:9092).")

    args = parser.parse_args()

    if args.mode == "file":
        run_file_mode(args.n)
    elif args.mode == "kafka":
        run_kafka_mode(rate=args.rate, topic=args.topic, bootstrap_servers=args.bootstrap_servers)
