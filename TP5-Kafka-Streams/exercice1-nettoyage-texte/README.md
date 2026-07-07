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

## Réponses aux questions

#### 1. Quel est le rôle du topic `text-dead-letter` ?

Il sert de zone de quarantaine pour stocker les messages invalides, rejetés ou mal formés qui n'ont pas passé les règles de validation. Cela permet d'isoler les erreurs sans interrompre le traitement du flux principal et d'analyser ultérieurement pourquoi ces messages ont échoué.

#### 2. Pourquoi est-il important de nettoyer les messages avant de les traiter ?

Le nettoyage garantit que les messages ont un format homogène avant leur validation et leur envoi vers les topics de sortie.

#### 3. Pourquoi faut-il convertir le texte en majuscules avant de vérifier les mots interdits ?

La conversion en majuscules permet de vérifier les mots interdits sans tenir compte de la manière dont ils ont été écrits.

#### 4. Comment améliorer l'application pour gérer une liste de mots interdits stockée dans un fichier ou une base de données ?

Au lieu d’écrire les mots interdits directement dans le code Java, on peut stocker la liste dans une source externe.
Avantage : la liste peut être modifiée sans changer la logique principale de l’application.
