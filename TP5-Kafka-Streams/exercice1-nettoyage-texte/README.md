# TP 5 - Exercice 1 : Nettoyage et validation de messages texte

Ce projet est une implémentation de l'Exercice 1 du TP sur le traitement de flux avec Kafka Streams.

## Contexte et Objectif

L'application consomme des messages texte bruts depuis un topic Kafka, les nettoie, puis applique des règles métier pour vérifier leur validité.

Les messages valides sont envoyés vers un topic dédié, tandis que les messages invalides sont redirigés vers un topic de type "Dead Letter".

### Règles de nettoyage

- Suppression des espaces au début et à la fin du message.
- Remplacement des espaces multiples par un seul espace.
- Conversion du message en majuscules.

### Règles de validation (Rejet du message)

Un message est rejeté si :

- Il est vide.
- Il contient uniquement des espaces.
- Il dépasse 100 caractères.
- Il contient un des mots interdits : `HACK`, `SPAM`, ou `XXX`.

---

## Architecture des Topics Kafka

L'application utilise les trois topics suivants :

- **`text-input`** : Topic d'entrée contenant les messages texte bruts.
- **`text-clean`** : Topic de sortie contenant les messages valides après nettoyage.
- **`text-dead-letter`** : Topic contenant les messages invalides ou rejetés.

---
