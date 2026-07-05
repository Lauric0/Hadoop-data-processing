# Stock Processing — Analyse des actions du S&P 500 avec PySpark

**Auteur :** Lauric GBOZO

## Description

Ce notebook (`Stock_prosessing_.ipynb`) est un mini-projet réalisé sur **Google Colab** qui met en place un environnement **Apache Spark (PySpark)** pour explorer, nettoyer et analyser le jeu de données [S&P 500 Stocks](https://www.kaggle.com/datasets/andrewmvd/sp-500-stocks) (Kaggle), constitué de trois fichiers :

- `sp500_companies.csv` — informations sur les entreprises (secteur, industrie, capitalisation, etc.)
- `sp500_index.csv` — valeur historique de l'indice S&P 500
- `sp500_stocks.csv` — cours historiques (open/close/high/low/volume) de chaque action du S&P 500

Le notebook combine installation d'environnement, nettoyage de données, feature engineering, agrégations (`groupBy`, `rollup`, `cube`), fonctions de fenêtrage (`Window`), statistiques et détection d'anomalies, avec quelques captures d'écran commentées de la Spark UI.

## Prérequis / Environnement

Le notebook est pensé pour tourner sur **Google Colab** et installe lui-même :

- Java 8 (`openjdk-8-jdk-headless`)
- Apache Spark 3.5.8 (téléchargé depuis les miroirs Apache)
- [`findspark`](https://pypi.org/project/findspark/) pour initialiser Spark dans le notebook
- [`ngrok`](https://ngrok.com/) pour exposer la Spark UI (port `4050`) via un tunnel public
- [`kaggle`](https://pypi.org/project/kaggle/) (CLI) pour télécharger le jeu de données

### Éléments à fournir avant exécution

1. **Token ngrok** : dans la cellule d'authentification, remplacer `YOUR_AUTH_TOKEN` par votre propre auth token ngrok (`ngrok config YOUR_AUTH_TOKEN`).
2. **Identifiants Kaggle** : uploader votre fichier `kaggle.json` (token API Kaggle) lorsque le notebook le demande — il sera copié dans `~/.kaggle/`.

## Structure du notebook

| Section | Contenu |
|---|---|
| **Install Dependencies** | Installation de Java, Spark, `findspark`, création de la `SparkSession` (`CollabApp`), installation de `ngrok` et configuration du tunnel vers la Spark UI, installation de la CLI Kaggle et upload du token |
| **Work — Chargement des données** | Téléchargement et extraction du dataset Kaggle (`sp-500-stocks.zip`), lecture des 3 CSV avec inférence de schéma, puis définition explicite des schémas (`StructType`) pour `companies`, `index` et `stocks` |
| **Nettoyage** | Suppression des lignes contenant des valeurs `NaN`/`None` (`dropna`) sur les trois jeux de données (`companies_clean`, `index_clean`, `stocks_clean`) |
| **Feature engineering** | Calcul du rendement journalier `Daily_return = (Close - Open) / Open`, ajout d'un indicateur binaire `is_gain` (1 si gain, 0 sinon), suppression de la colonne `Adj Close` devenue inutile |
| **Agrégations** | Rendement moyen par symbole (`groupBy("symbol")`), jointure `stocks` ⋈ `companies` puis rendement moyen par secteur, `rollup("year","month","symbol")` sur la clôture, `cube("Sector","Symbol")` sur le rendement journalier |
| **Statistiques** | `describe()` sur `open`, `close`, `volume`, `daily_return`, `is_gain` ; filtrage sur le volume (> 10 000 000) ; filtrage des valeurs aberrantes (`open` et `close` > 0) ; calcul de l'amplitude quotidienne (`range = high - low`) |
| **Agrégations temporelles** | Somme du volume par année/mois (`groupBy("year","month")`) |
| **Fenêtrage (Window)** | Moyenne mobile sur 20 jours (`sma20`) par symbole, triée par date (`Window.partitionBy("symbol").orderBy("Date").rowsBetween(-19, 0)`) |
| **Corrélation & quantiles** | Corrélation de Pearson entre `close` et `volume` (`stat.corr`) ; quantiles approximatifs (50 %, 90 %, 99 %) du rendement journalier (`approxQuantile`) |
| **Détection d'anomalies** | Colonne `anomaly` = 1 si `daily_return > 5 %` |
| **Split & union** | Filtrage indépendant des données de janvier et février, puis réunion (`union`) |
| **Spark UI — commentaires** | Captures d'écran annotées de la Spark UI illustrant l'évaluation paresseuse (lazy evaluation), le DAG des jobs, le *stage skipping* dû au cache, et le parallélisme des tâches |

## Points clés illustrés par le projet

- **Lecture avec/sans inférence de schéma** : comparaison entre lecture automatique (`inferSchema`) et schémas explicites (`StructType`) pour `companies`, `index` et `stocks`.
- **Nettoyage** systématique des valeurs manquantes avant toute analyse.
- **Feature engineering financier** : rendement journalier, indicateur de gain, amplitude quotidienne, moyenne mobile 20 jours.
- **Agrégations multi-niveaux** avec `rollup` (hiérarchie année → mois → symbole) et `cube` (toutes les combinaisons secteur/symbole).
- **Statistiques descriptives et détection d'outliers/anomalies** via `describe`, `corr`, `approxQuantile` et un seuil de rendement.
- **Observation du comportement de Spark** (lazy evaluation, DAG, cache, parallélisme) directement depuis la Spark UI exposée via ngrok.

## Comment exécuter le notebook

1. Ouvrir le notebook dans Google Colab.
2. Exécuter les cellules de la section *Install Dependencies* dans l'ordre (Java → Spark → ngrok → Kaggle).
3. Renseigner votre auth token ngrok et uploader votre `kaggle.json` lorsque demandé.
4. Exécuter les cellules de la section *Work* dans l'ordre : elles dépendent les unes des autres (chaque étape réutilise les DataFrames créés précédemment : `companies_wi` → `companies_clean`, `stocks_wi` → `stocks_clean` → `daily_return` → `is_gain` → `remove_col` → `range` → `partition`, etc.).
5. (Optionnel) Ouvrir le lien ngrok affiché pour suivre l'exécution des jobs Spark en temps réel dans la Spark UI.

## Remarques

- Les identifiants ngrok (`AuthCode`) et Kaggle (`kaggle.json`) sont personnels et ne doivent pas être partagés/committés.
- Le notebook contient des commentaires en français dans certaines cellules de code (ex. jointure secteur/rendement) et des consignes/explications en anglais dans les cellules markdown.
- Les captures d'écran de la Spark UI sont incluses en base64 dans les cellules markdown à titre d'illustration des résultats obtenus lors de l'exécution originale.
