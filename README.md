# PROJECTDATAORNIKAR

ProjetDataOrnikar Est un projet python qui répond au test case de la société Ornikar pour Data Engineer. Il y'a principalement des interactions avec le projet "test-data-engineer-090621" sur bigquery 

Il est composé de 3 répertoires principaux à savoir :
- credentials : qui contient le fichier JSON credential pour se connecter à l'entrepôt de donnée
- data : qui contient les données parmi lesquelles on a :
  . Des tables de bigquery les données externe
  . Des données externes , les coordonnées géographiques des départements français
  . Des données tests , qui sont issus des expérimentations de croisements des départements aux meeting_points
  . Des résultats de requêtes , étant des réponses à l'exercice
- scripts : qui contient les scripts au paradigme orienté object dans des packages :
   . Le module connection , pour la connection à bigquery et le requêtage d'Api
   . Le module experimentation qui contient des pistes étudié pour le croisement des départements aux meeting_points , notament l'extraction des tables de bigquery et le test de plusieurs librairies de manipulation de données pour le croisement des données
   . Le module solution qui comprend les réponses aux questions à savoir  : les dates auxquelles les enseignants ayant données le plus de cours au 3ème trimestre de 2020 ont dispensés leur 50ème lesson et le nombre de leçons par départements en fonction des type de partenariats dans un intervalle de temps précis .


## Installation

Pour faire fonctionner ce projet vous avez besoin tout d'abord d'installer python à sa version 12 si vous ne l'avez pas déjà sur votre machine.



Pour les utilisateurs d'Ubuntu

Se rendre dans le terminal et puis lancer les commandes :

```bash
sudo apt update
```

```bash
sudo apt upgrade
```

```bash
sudo apt install python3.12
```
 
Pour les utilisateurs de MAC OS

Se rendre dans le terminal et puis lancer les commandes :

```bash
 brew update
```

```bash
 brew upgrade
```

```bash
brew install python3.12
```
Pour les utilisateurs de windows , se rendre sur le site web de [python](https://www.python.org/downloads/release/python-3120/) et télécharger l installer.

Une fois l installation de python terminé vous pouvez 
- soit vous rendre dans le répertoire de ce projet à la racine et lancer les commandes :
```bash
python3 -m venv .env
```
pour la création d'un environment virtuel

```bash
source ./env/bin/activate
```
ensuite ,pour l'activation de l'environment

```bash
pip install -r requirements.txt
```
ensuite pour l'installation des modules utilisés dans le projet

- soit ouvrir le projet avec l'IDE de votre choix ([Pycharm](https://www.jetbrains.com/pycharm/), [VSCode](https://code.visualstudio.com/), etc) si vous l'avez ou le télécharger et configurez l'interpréteur python 3.12.

## Usage
Pour voir les solutions du projet se rendre dans le répertoire scripts/ et lancer le script main.py soit avec l'IDE ou en ligne de commande

```bash
python3 main.py
```
Il faut se laisser guider par la console interactive.

Faire de même pour les tests dans ou bien les scripts d'experimentation dans experimentation



## Contribution

Les demandes de tirage sont les bienvenues. Pour des changements majeurs, veuillez d'abord ouvrir un problème
pour discuter de ce que vous aimeriez changer.

Veuillez vous assurer de mettre à jour les tests le cas échéant.

