```bash
Projet Big data 2018-2019
Master : SIRAV
```
```````python
### Auteurs :
             OUEFIO Innocent Dieu Benit
             LIPSKI Guillaume
```````
### 1. Introduction
Le but de ce projet est d'analyser les logs d'authentifications aux machines. Ces données
représentent les événements d'authentifications collectés à partir d'ordinateurs individuels
de bureau et serveurs qui sont dotés du système d'exploitation Windows. Chaque événement se trouve sur une ligne distincte sous la forme : "temps, utilisateur_source@domaine, utilisateur_destination@domaine, ordinateur_source, ordinateur_destination, type d'authentificationscation, type de connexion, orientation d'authentificationscation, succès / échec" et représente un événement d'authentificationscation à l'instant donné.<br>
Voici cinq lignes de données à titre d'exemple :

````````bash
1,C625$@DOM1,U147@DOM1,C625,C625,Negotiate,Batch,LogOn,Success
1,C653$@DOM1,SYSTEM@C653,C653,C653,Negotiate,Service,LogOn,Success
1,C660$@DOM1,SYSTEM@C660,C660,C660,Negotiate,Service,LogOn,Success
3,C625$@DOM1,U147@DOM1,C625,C625,Negotiate,Batch,LogOn,Success
4,C600$@DOM1,U147@DOM1,C600,C600,Negotiate,Batch,LogOn,Success
````````
### 2.1 Partie I

### Consigne :
Lien github pour les sources : https://github.com/dieubenit/BigData_Spark	<br>
Lien fichier jar : upload en cours...	<br>
Placer winutils dans C:\\winutils\\bin si nécessaire.(remplacez chemin_vers_winutils par - pour appeler le chemin par default) <br>
Placer le fichier jar dans le même dossier que auth_500000.txt.<br>
Utilisation du programme : <br>
 "java -jar ./mySpark.jar [- OU chemin_vers_winutils] [numero_partie] [fenetre_temp]" <br>
mySpark lance toutes les parties en séquence par default si lancé sans argument ou <br> 
seulement avec winutils ou avec l'argument numero_partie invalide.<br>
exemples :<br>
  java -jar ./mySpark.jar - 1<br>
lance la partie 1 avec winutils par default<br>
  java -jar ./mySpark.jar - 4<br>
lance la partie 4 avec fenetre 60 par default<br>
  java -jar ./mySpark.jar - 4 10<br>
lance la partie 4 avec fenetre 10<br>

#### Question 1. Lire le fichier logs.
`````
df= spark.read()
				.option("header", "true")
				.option("delimiter", ",")
				.option("inferSchema", "true")
				.format("csv")
				.schema(schema)
				.load("auth_500000.txt");
		df.show();

    +-----+--------------------------+-------------------------------+-----------------+----------------------+---------------------+-----------------+----------------------------+------------+
    |temps|utilisateur_sourceAdomaine|utilisateur_destinationAdomaine|ordinateur_source|ordinateur_destination|type_authentification|type_de_connexion|orientation_authentification|succes_echec|
    +-----+--------------------------+-------------------------------+-----------------+----------------------+---------------------+-----------------+----------------------------+------------+
    |    1|      ANONYMOUS LOGON@C586|           ANONYMOUS LOGON@C586|             C586|                  C586|                    ?|          Network|                      LogOff|     Success|
    |    1|                C101$@DOM1|                     C101$@DOM1|             C988|                  C988|                    ?|          Network|                      LogOff|     Success|
    |    1|               C1020$@DOM1|                   SYSTEM@C1020|            C1020|                 C1020|            Negotiate|          Service|                       LogOn|     Success|
    |    1|               C1021$@DOM1|                    C1021$@DOM1|            C1021|                  C625|             Kerberos|          Network|                       LogOn|     Success|
    |    1|               C1035$@DOM1|                    C1035$@DOM1|            C1035|                  C586|             Kerberos|          Network|                       LogOn|     Success|
    |    1|               C1035$@DOM1|                    C1035$@DOM1|             C586|                  C586|                    ?|          Network|                      LogOff|     Success|
    |    1|               C1069$@DOM1|                   SYSTEM@C1069|            C1069|                 C1069|            Negotiate|          Service|                       LogOn|     Success|
    |    1|               C1085$@DOM1|                    C1085$@DOM1|            C1085|                  C612|             Kerberos|          Network|                       LogOn|     Success|
    |    1|               C1085$@DOM1|                    C1085$@DOM1|             C612|                  C612|                    ?|          Network|                      LogOff|     Success|
    |    1|               C1151$@DOM1|                   SYSTEM@C1151|            C1151|                 C1151|            Negotiate|          Service|                       LogOn|     Success|
    |    1|               C1154$@DOM1|                   SYSTEM@C1154|            C1154|                 C1154|            Negotiate|          Service|                       LogOn|     Success|
    |    1|               C1164$@DOM1|                    C1164$@DOM1|             C625|                  C625|                    ?|          Network|                      LogOff|     Success|
    |    1|                C119$@DOM1|                     C119$@DOM1|             C119|                  C528|             Kerberos|          Network|                       LogOn|     Success|
    |    1|               C1218$@DOM1|                    C1218$@DOM1|            C1218|                  C529|             Kerberos|          Network|                       LogOn|     Success|
    |    1|               C1235$@DOM1|                    C1235$@DOM1|             C586|                  C586|                    ?|          Network|                      LogOff|     Success|
    |    1|               C1241$@DOM1|                   SYSTEM@C1241|            C1241|                 C1241|            Negotiate|          Service|                       LogOn|     Success|
    |    1|               C1250$@DOM1|                    C1250$@DOM1|            C1250|                  C586|             Kerberos|          Network|                       LogOn|     Success|
    |    1|               C1314$@DOM1|                    C1314$@DOM1|            C1314|                  C467|             Kerberos|          Network|                       LogOn|     Success|
    |    1|                C144$@DOM1|                    SYSTEM@C144|             C144|                  C144|            Negotiate|          Service|                       LogOn|     Success|
    |    1|               C1444$@DOM1|                    C1444$@DOM1|            C1444|                  C528|             Kerberos|          Network|                       LogOn|     Success|
    +-----+--------------------------+-------------------------------+-----------------+----------------------+---------------------+-----------------+----------------------------+------------+

    +-----+--------------------------+-------------------------------+-----------------+----------------------+---------------------+-----------------+----------------------------+------------+

`````
#### Question 2. Suppression des lignes de logs qui contiennent le symbole ' ?'.
`````
|temps|utilisateur_sourceAdomaine|utilisateur_destinationAdomaine|ordinateur_source|ordinateur_destination|type_authentification|type_de_connexion|orientation_authentification|succes_echec|
+-----+--------------------------+-------------------------------+-----------------+----------------------+---------------------+-----------------+----------------------------+------------+
|    1|               C1020$@DOM1|                   SYSTEM@C1020|            C1020|                 C1020|            Negotiate|          Service|                       LogOn|     Success|
|    1|               C1021$@DOM1|                    C1021$@DOM1|            C1021|                  C625|             Kerberos|          Network|                       LogOn|     Success|
|    1|               C1035$@DOM1|                    C1035$@DOM1|            C1035|                  C586|             Kerberos|          Network|                       LogOn|     Success|
|    1|               C1069$@DOM1|                   SYSTEM@C1069|            C1069|                 C1069|            Negotiate|          Service|                       LogOn|     Success|
|    1|               C1085$@DOM1|                    C1085$@DOM1|            C1085|                  C612|             Kerberos|          Network|                       LogOn|     Success|
|    1|               C1151$@DOM1|                   SYSTEM@C1151|            C1151|                 C1151|            Negotiate|          Service|                       LogOn|     Success|
|    1|               C1154$@DOM1|                   SYSTEM@C1154|            C1154|                 C1154|            Negotiate|          Service|                       LogOn|     Success|
|    1|                C119$@DOM1|                     C119$@DOM1|             C119|                  C528|             Kerberos|          Network|                       LogOn|     Success|
|    1|               C1218$@DOM1|                    C1218$@DOM1|            C1218|                  C529|             Kerberos|          Network|                       LogOn|     Success|
|    1|               C1241$@DOM1|                   SYSTEM@C1241|            C1241|                 C1241|            Negotiate|          Service|                       LogOn|     Success|
|    1|               C1250$@DOM1|                    C1250$@DOM1|            C1250|                  C586|             Kerberos|          Network|                       LogOn|     Success|
|    1|               C1314$@DOM1|                    C1314$@DOM1|            C1314|                  C467|             Kerberos|          Network|                       LogOn|     Success|
|    1|                C144$@DOM1|                    SYSTEM@C144|             C144|                  C144|            Negotiate|          Service|                       LogOn|     Success|
|    1|               C1444$@DOM1|                    C1444$@DOM1|            C1444|                  C528|             Kerberos|          Network|                       LogOn|     Success|
|    1|               C1492$@DOM1|                    C1492$@DOM1|            C1492|                  C467|             Kerberos|          Network|                       LogOn|     Success|
|    1|               C1492$@DOM1|                    C1492$@DOM1|            C1492|                  C528|             Kerberos|          Network|                       LogOn|     Success|
|    1|               C1492$@DOM1|                    C1492$@DOM1|            C1492|                  C586|             Kerberos|          Network|                       LogOn|     Success|
|    1|               C1492$@DOM1|                    C1492$@DOM1|            C1798|                 C1492|             Kerberos|          Network|                       LogOn|     Success|
|    1|               C1504$@DOM1|                      U45@C1504|            C1504|                 C1504|            Negotiate|            Batch|                       LogOn|     Success|
|    1|               C1543$@DOM1|                   SYSTEM@C1543|            C1543|                 C1543|            Negotiate|          Service|                       LogOn|     Success|
+-----+--------------------------+-------------------------------+-----------------+----------------------+---------------------+-----------------+----------------------------+------------+
`````

#### Question 3. Calculons le nombre d'utilisation d'une machine (ordinateur_source) par un utilisateur (utilisateur_source@domaine).

Cf. Question 4.<br>

#### Question 4. Affichons le top 10 des accès les plus fréquents.
`````

Top 10 Utilisateur par Ordinateur
+-----+------------------+
|count|utilisateur_par_pc|
+-----+------------------+
|3837 |C599$@DOM1, C1619 |
|3384 |C585$@DOM1, C585  |
|2929 |C1114$@DOM1, C1115|
|2925 |C743$@DOM1, C743  |
|2725 |C104$@DOM1, C105  |
|2460 |C567$@DOM1, C574  |
|2345 |C123$@DOM1, C527  |
|2002 |C1617$@DOM1, C1618|
|1930 |C538$@DOM1, C539  |
|1907 |U22@DOM1, C506    |
+-----+------------------+
`````
###  2.2 Partie II

### Remarque:
Nous avons trié par odre décroissant sur le poids.

#### Question 1-a)
`````
+------------+-----------+-----+
|Utilisateurs| Connexions|Poids|
+------------+-----------+-----+
|  C585$@DOM1|  C585,C586| 3376|
|  C743$@DOM1|  C743,C586| 2898|
|  C480$@DOM1|  C480,C625| 1670|
|    U19@DOM1|  C229,C229| 1336|
|    U48@DOM1|  C419,C419| 1054|
|  C599$@DOM1| C1619,C101| 1037|
|    U34@DOM1|  C921,C921| 1029|
|  C529$@DOM1|  C529,C529| 1001|
|    U53@DOM1|C1710,C1710|  976|
|  C599$@DOM1| C1619,C553|  917|
+------------+-----------+-----+
`````
#### Question 1-b)
````````
+--------------------------+
|Utilisateurs et Connexions|
+--------------------------+
|               C2962$@DOM1|
|               C3608$@DOM1|
|               C2470$@DOM1|
|               C4248$@DOM1|
|               C2645$@DOM1|
|               C3844$@DOM1|
|               C1800$@DOM1|
|               C2641$@DOM1|
|               C3787$@DOM1|
|               C3572$@DOM1|
+--------------------------+
````````
#### Question 2-a)
`````
+--------------------+-------------+-----+
|        Utilisateurs|   Connexions|Poids|
+--------------------+-------------+-----+
|            U22@DOM1|LogOn,Success| 6342|
|ANONYMOUS LOGON@C586|LogOn,Success| 5906|
|            U66@DOM1|LogOn,Success| 4885|
|          C599$@DOM1|LogOn,Success| 3837|
|          C585$@DOM1|LogOn,Success| 3384|
|         C1114$@DOM1|LogOn,Success| 2929|
|          C743$@DOM1|LogOn,Success| 2925|
|          C104$@DOM1|LogOn,Success| 2725|
|          C567$@DOM1|LogOn,Success| 2460|
|          C123$@DOM1|LogOn,Success| 2345|
+--------------------+-------------+-----+
`````
#### Question 2-b)
````
+-------------------------+
|Utlisateurs et Connexions|
+-------------------------+
|              C4248$@DOM1|
|              C3608$@DOM1|
|               C254$@DOM1|
|              C2645$@DOM1|
|              C3542$@DOM1|
|                C64$@DOM1|
|              C3572$@DOM1|
|              C3787$@DOM1|
|              C1210$@DOM1|
|              C3844$@DOM1|
+-------------------------+
````
#### Question 3-a)
`````
+--------------------+--------------------+-----+
|        Utilisateurs|          Connexions|Poids|
+--------------------+--------------------+-----+
|            U22@DOM1|    U22@DOM1,Success| 6342|
|ANONYMOUS LOGON@C586|ANONYMOUS LOGON@C...| 5906|
|            U66@DOM1|    U66@DOM1,Success| 4885|
|          C599$@DOM1|  C599$@DOM1,Success| 3837|
|          C585$@DOM1|  C585$@DOM1,Success| 3384|
|         C1114$@DOM1| C1114$@DOM1,Success| 2929|
|          C743$@DOM1|  C743$@DOM1,Success| 2925|
|          C104$@DOM1|  C104$@DOM1,Success| 2725|
|          C567$@DOM1|  C567$@DOM1,Success| 2460|
|          C123$@DOM1|  C123$@DOM1,Success| 2345|
+--------------------+--------------------+-----+
`````
#### Question 2-b)
`````
+--------------------------+
|Utilisateurs et Connexions|
+--------------------------+
|               C4248$@DOM1|
|               C3608$@DOM1|
|                C254$@DOM1|
|               C2645$@DOM1|
|               C3542$@DOM1|
|                 C64$@DOM1|
|               C3572$@DOM1|
|               C3787$@DOM1|
|               C3844$@DOM1|
|               C1210$@DOM1|
+--------------------------+
`````
### 2.3 Partie III
##### Objectif :
La troisième partie consiste à généraliser la Partie II. Le but est de calculer pour chaque colonne, la relation entre chaque paires de colonnes du jeu de données.<br>

Exemple de résultat d'éxecution dans le dossier json. <br>
Format : paire_1_2_3<br>
part-00000-7149fac2-e3a0-4e4c-bdc5-6fc548ac6c06-c000.json
part-00001-7149fac2-e3a0-4e4c-bdc5-6fc548ac6c06-c000.json
part-00002-7149fac2-e3a0-4e4c-bdc5-6fc548ac6c06-c000.json
part-00003-7149fac2-e3a0-4e4c-bdc5-6fc548ac6c06-c000.json
part-00004-7149fac2-e3a0-4e4c-bdc5-6fc548ac6c06-c000.json
part-00005-7149fac2-e3a0-4e4c-bdc5-6fc548ac6c06-c000.json
part-00006-7149fac2-e3a0-4e4c-bdc5-6fc548ac6c06-c000.json
part-00007-7149fac2-e3a0-4e4c-bdc5-6fc548ac6c06-c000.json
part-00008-7149fac2-e3a0-4e4c-bdc5-6fc548ac6c06-c000.json
part-00009-7149fac2-e3a0-4e4c-bdc5-6fc548ac6c06-c000.json
part-00010-7149fac2-e3a0-4e4c-bdc5-6fc548ac6c06-c000.json
part-00011-7149fac2-e3a0-4e4c-bdc5-6fc548ac6c06-c000.json
part-00012-7149fac2-e3a0-4e4c-bdc5-6fc548ac6c06-c000.json

### 2.4 Partie IV
Contrairement à la partie III, dans cette partie le temps est pris en considération. En effet, cette partie consiste à calculer la Partie III pour chaque fenêtre (période) temporelle fixée au préalable.<br>

Exemple de résultat d'éxecution dans le dossier json.<br>
Format : paire_0_a_10_1_2_3<br>
