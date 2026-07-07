# TP5 — Exercice 3 : Comptage de clics avec Kafka Streams et Spring Boot

## 1. Description de l'architecture

L'application est composée de trois applications indépendantes reliées par deux topics Kafka :

1. **Producteur Web** (`producteur-web`, port **8081**) : application Spring Boot qui affiche une page avec un bouton « Cliquez ici ». Chaque clic envoie dans le topic `clicks` un message avec
   **clé = userId** (ex : `user1` et **valeur = `click`**.
2. **Application Kafka Streams** (`streams-app`) : consomme le topic `clicks` sous forme de `KStream`, groupe les événements (`KGroupedStream`), les compte avec `count()` (résultat sous forme de `KTable`), puis publie chaque mise à jour du comptage dans le topic `click-counts`.
3. **Consommateur REST** (`consommateur-rest`, port **8080**) : consomme `click-counts` avec un `@KafkaListener` et les expose via `GET /clicks/count` (le port 8080 correspond au test `curl` du TP).

## 2. Création des topics Kafka

```bash
kafka-topics.sh --create \
  --topic clicks \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

kafka-topics.sh --create \
  --topic click-counts \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

## 3. Scénario de test complet

1. Démarrer Kafka (le fichier docker-compose.yml).
2. Créer les topics `clicks` et `click-counts`
3. Lancer les trois applications java.
4. Ouvrir `http://localhost:8081` dans un navigateur choisir un utilisateur et cliquer plusieurs fois sur le bouton.

```bash
# Événements de clics bruts
kafka-console-consumer.sh --topic clicks --from-beginning \
  --bootstrap-server localhost:9092 \
  --property print.key=true --property key.separator=" => "

# Résultats du comptage
kafka-console-consumer.sh --topic click-counts --from-beginning \
  --bootstrap-server localhost:9092 \
  --property print.key=true --property key.separator=" => "
```

5. Consulter l'API REST :

```bash
curl http://localhost:8080/clicks/count
```
