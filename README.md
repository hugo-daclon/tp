# Polytech Nantes - Mini-projet

## Prérequis

L'ensemble de ce TP est à réaliser sur Linux (Debian et Ubuntu par exemple).

## Part 0 - Setup

L'objectif de cette étape est de préparer l'environnement pour la réalisation de ce mini-projet.

La première étape consiste à cloner ce projet :

```bash
git clone https://github.com/polytech-nantes-puddi/tp.git
```

Nous allons ensuite procéder à l'installation des prérequis systèmes :

```bash
sudo apt update
sudo apt install openjdk-11-jdk
sudo apt install python3 python3-venv
python3 -m venv spark-env
```

Il est maintenant nécessaire d'activer l'environnement virtuel Python :

```bash
source spark-env/bin/activate
```

Nous procédons ensuite à l'installation des paquets python nécessaire au TP :

```bash
pip install --upgrade pip
pip install pyspark
```

Note : À chaque fois que vous ouvrirez un nouveau terminal, il sera nécessaire d'activer l'environnement virtuel Python avec la commande : 

```bash
source spark-env/bin/activate
```

## Part 1 - Download files

L'objectif de cette étape est de télécharger les fichiers de données nécessaires à la réalisation de ce mini-projet :


```bash
make part1
```

Cela devrait :

* Télécharger les ZIP de données dans le répertoire `dataset/zip/`
* Extraire les données des ZIP dans le répertoire `dataset/raw/`

Pour information, la documentation sur les données est disponible ici : http://data.gdeltproject.org/documentation/GDELT-Data_Format_Codebook.pdf

## Part 2 - Work count example

1. Exécuter la version wordcount Python :

```bash
python3 part2-wordcount-hamlet-python.py
```

2. Exécuter la version wordcount PySpark (version RDD) :

```bash
spark-submit part2-wordcount-hamlet-pyspark.py
```

3. Exécuter la version wordcount PySpark (version Dataframe) :

```bash
spark-submit part2-wordcount-hamlet-pyspark-2.py
```

4. Comparer les résultats.
5. Adapter les scripts ci-dessus pour filtrer les mots commençant par la lettre `m`.
6. Quelle différence sur votre traitement ?

Quelques documentations utiles :

* https://spark.apache.org/docs/latest/quick-start.html
* https://spark.apache.org/docs/latest/rdd-programming-guide.html

## Part 3 - Processing data

1. Adapter le script `part3.py` pour obtenir le top 10 des pays les plus pertinents dans l'actualité sur l'échantillon de données téléchargé.

Nous considérerons le code pays comme l'identifiant à trois lettres représenté par `Actor1CountryCode` (colonne 7), nous compterons la pertinence d'un événement en fonction de sa colonne `NumMentions` (colonne 31).

Cela revient à l'exercice WordCount où nous comptons les `NumMentions` de chaque événement par pays pour déterminer le Top 10.

Rappel concernant pour l'exécution :

```bash
spark-submit part3.py
```

## Part 4 - Exposing data

1. Convertir les données en parquet :

```bash
spark-submit part4-convert-to-parquet.py
```

2. Adapter le script `part4.py` pour réaliser la même analyse que la partie précédente, mais à partir de la vue temporaire.

Rappel concernant pour l'exécution :

```bash
spark-submit part4.py
```

Quelques documentations utiles :

* https://spark.apache.org/docs/latest/api/python/pyspark.sql.html
* https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
* https://s3.amazonaws.com/assets.datacamp.com/blog_assets/PySpark_SQL_Cheat_Sheet_Python.pdf

## Rendu

L'évaluation se portera sur :

1. Le script et le résultat (copie d'écran) de la partie 2
1. Les analyses de la partie 2
1. Le script et le résultat (copie d'écran) de la partie 3
1. Le script et le résultat (copie d'écran) de la partie 4
1. L'analyse technique de la requête `print(spark.sql("SELECT count(1) FROM gdelt").explain())` présente dans le script de la partie 4
1. Une analyse fonctionnelle des données étudiées GDELT sur une problématique identifiée (bonus)

Quelques idées de sujets d'analyse (vous pouvez choisir votre propre sujet) :

* Comparez les religions, les reportages sont-ils biaisés pour certaines religions (voir le paramètre `tone`).
* Quelle influence a eu un pays par rapport à un autre (en termes de comptage pur).
* Top des organisations (voir le paramètre `Actor1KnownGroupCode`) mentionnées par mois.

Pour votre analyse, les données référentielles sur notre échantillon de données sont disponibles ici :

* https://github.com/carrillo/Gdelt/tree/master/resources/staticTables

Toujours pour votre analyse, voici une page concernant les jointures :

* https://luminousmen.com/post/introduction-to-pyspark-join-types

## Format du rendu attendu

* Scripts et code source
* Compte rendu au format PDF
