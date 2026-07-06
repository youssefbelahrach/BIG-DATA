# TP Spark SQL en Java

## 1. Objectif du TP

Cette solution répond au TP **Spark SQL** consacré à l'analyse d'un système public de location de vélos. L'application Java charge le fichier `bike_sharing.csv` dans un DataFrame Spark, crée la vue temporaire `bike_rentals_view`, puis exécute les requêtes Spark SQL demandées : exploration, agrégations, analyse temporelle et comportement des utilisateurs.

## 2. Technologies utilisées

| Élément | Choix |
|---|---|
| Langage | Java 17 |
| Moteur de traitement | Apache Spark SQL 3.5.1 |
| Gestionnaire de dépendances | Maven |
| Données | CSV (`bike_sharing.csv`) |

## 3. Résultats obtenus

### 3.1 Exploration et requêtes de base

| Indicateur | Résultat |
|---|---:|
| Nombre total de locations | 5 000 |
| Locations de plus de 30 minutes | 1 387 |
| Chiffre d'affaires total | 41 755,70 $ |
| Locations démarrant à `Station A` | 0 |

### 3.2 Nombre de locations par station de départ

| Station de départ | Nombre de locations |
|---|---:|
| Station Hôpital | 447 |
| Station Marina | 443 |
| Station Souk | 436 |
| Station Parc Central | 433 |
| Station Technopark | 430 |
| Station Quartier Administratif | 416 |
| Station Université | 415 |
| Station Centre-Ville | 410 |
| Station Aéroport Bus | 408 |
| Station Corniche | 404 |
| Station Gare | 384 |
| Station Stade | 374 |

La station ayant le plus grand nombre de locations est **Station Hôpital**, avec **447 locations**.

### 3.3 Durée moyenne des locations par station de départ

| Station de départ | Durée moyenne (min) |
|---|---:|
| Station Corniche | 25,81 |
| Station Aéroport Bus | 25,67 |
| Station Souk | 25,59 |
| Station Technopark | 25,20 |
| Station Université | 25,11 |
| Station Gare | 24,92 |
| Station Marina | 24,87 |
| Station Hôpital | 24,51 |
| Station Stade | 24,29 |
| Station Centre-Ville | 24,26 |
| Station Parc Central | 24,10 |
| Station Quartier Administratif | 23,43 |

### 3.4 Analyse temporelle

| Heure de départ | Nombre de locations |
|---:|---:|
| 19 h | 507 |
| 18 h | 503 |
| 17 h | 499 |
| 9 h | 467 |
| 7 h | 452 |
| 8 h | 431 |
| 13 h | 285 |
| 10 h | 261 |
| 12 h | 245 |
| 11 h | 242 |
| 14 h | 242 |
| 16 h | 225 |
| 15 h | 224 |
| 6 h | 90 |
| 20 h | 80 |
| 21 h | 72 |
| 0 h | 27 |
| 1 h | 27 |
| 22 h | 22 |
| 3 h | 21 |
| 5 h | 21 |
| 4 h | 20 |
| 2 h | 19 |
| 23 h | 18 |

- **Heure de pointe : 19 h**, avec **507 locations**.
- Entre **7 h et 12 h inclus**, la station de départ la plus populaire est **Station Technopark**, avec **188 locations**.

### 3.5 Analyse des utilisateurs

| Indicateur | Résultat |
|---|---:|
| Âge moyen | 41,52 ans |
| Genre M | 2 542 locations |
| Genre F | 2 286 locations |
| Genre Autre | 172 locations |

| Tranche d'âge | Nombre de locations |
|---|---:|
| 51+ | 1 567 |
| 18-30 | 1 362 |
| 41-50 | 1 054 |
| 31-40 | 1 017 |

La tranche d'âge qui utilise le plus le service est donc **51+**, avec **1 567 locations**.
