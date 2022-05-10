# 5ESGF-BD-2022
Bienvenue sur le dépôt d'évaluation du cours c#/BigData.

Chaque groupe est invité à créer un Fork de ce dépôt principal muni d'un compte sur Github, travailler sur ce fork au sein du groupe, par le biais de validations, de push sur le server, et de pulls/tirages sur les machines locales des utilisateurs du groupe habilités sur le fork. Une fois le travail effectué et remonté sur le fork, une pull-request sera créée depuis le fork vers le dépôt principal pour fusion et évaluation.

Le fichier de solution "ESGF.Sudoku.Spark.sln" constitue l'environnement de base du travail et s'ouvre dans Visual Studio (attention à bien ouvrir la solution et ne pas rester en "vue de dossier").
En l'état, la solution contient:
- Un projet Sudoku.Shared contenant les classes élémentaires permettant de charger et manipuler des Sudokus, ainsi qu'un interface pour l'implémentation de solvers.
- Un projet de Console Sudoku.Benchmark pour tester des solvers de Sudoku. Il suffit de lui faire référencer un projet cible pour permettre de tester les solvers qu'il contient.
- Un projet d'exemple de solver Sudoku.Z3Solvers qui illustre comment implémenter des solvers de Sudoku à l'aide de la librairie z3, en c# ou en Python via Python.Net. 

Les groupes sont invités à àjouter à la solution leur propre projet de Console référençant d'une part les projets et librairies nécessaires à la résolution de Sudoku, et utilisant d'autre part l'environnement Spark.Net étudié en cours pour une résolution distribuée d'un dataset de Sudokus.