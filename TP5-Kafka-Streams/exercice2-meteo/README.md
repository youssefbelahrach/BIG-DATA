# TP 5 - Exercice 2 : Analyse de données météorologiques

Ce projet est une implémentation de l'Exercice 2 du TP sur le traitement de flux avec Kafka Streams.

## Contexte et Objectif

L'application collecte et analyse des données météorologiques en temps réel provenant de différentes stations. Les mesures brutes sont envoyées dans un topic Kafka, puis l'application Kafka Streams se charge de les filtrer, de les transformer et de les agréger.

---

## Guide d'exécution (via Docker)

Lancez les conteneurs Zookeeper et Kafka via Docker Compose :

```bash
docker-compose up -d
```

Exécutez les commandes suivantes pour créer les topics Kafka requis :

```bash
docker exec -it kafka kafka-topics --create --topic weather-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it kafka kafka-topics --create --topic station-averages --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

Depuis votre IDE, exécutez la méthode main de la classe WeatherDataApp. L'application se connectera au broker Kafka (localhost:9092) et se mettra en écoute.

Ouvrez deux terminaux différents pour simuler le flux.

Terminal 1 : Écoute des moyennes calculées (Consumer)

```bash
docker exec -it kafka kafka-console-consumer --topic station-averages --property print.key=true --property key.separator=" : " --from-beginning --bootstrap-server localhost:9092
```

Terminal 2 : Envoi de mesures (Producer)

```bash
docker exec -it kafka kafka-console-producer --topic weather-data --bootstrap-server localhost:9092
```

Exemple de données à copier/coller dans le Terminal 2 :

Station1, 25.3, 60
Station2, 35.0, 50
Station2, 40.0, 45
Station1, 32.0, 70

---

## Réponses aux questions

#### 1. Pourquoi doit-on regrouper les données par station avant de calculer les moyennes ?

Le regroupement par station permet de séparer les relevés de chaque station météorologique. Sans regroupement, l’application calculerait une moyenne globale qui mélangerait les mesures de toutes les stations. Cette moyenne ne permettrait pas de connaître la situation spécifique de chaque station.

#### 2. Quelle est la différence entre `KStream` et `KTable` ?

`KStream` représente un flux continu d’événements. Chaque message reçu est traité comme un nouvel événement indépendant. Kafka Streams ne remplace pas automatiquement les événements précédents.

`KTable` représente une vue mise à jour en continu, généralement associée à une clé. Lorsqu’une nouvelle valeur est reçue pour une clé donnée, elle met à jour la valeur précédente de cette clé.

#### 3. Pourquoi le résultat d’une agrégation est-il souvent représenté sous forme de `KTable` ?

Une agrégation produit un résultat évolutif qui doit être mis à jour chaque fois qu’un nouveau message arrive.

Une `KTable` est adaptée, car elle représente la dernière valeur calculée pour chaque station. Elle fournit donc une vue actualisée des résultats de l’agrégation.

#### 4. Comment gérer un message mal formé comme `Station1,error,60` ?

Pour éviter qu'une erreur de parsing ne provoque un arrêt brutal de l'application, le traitement doit être encapsulé dans un bloc try-catch.

#### 5. Pourquoi Kafka Streams est adapté à ce type de traitement ?

Kafka Streams est parfaitement adapté car il permet d'effectuer des calculs en continu et de regrouper les données en temps réel. Contrairement à des clusters lourds comme Spark, il s'exécute comme une simple bibliothèque au sein d'une application Java standard, tout en héritant nativement de la haute disponibilité et de la tolérance aux pannes de Kafka.
