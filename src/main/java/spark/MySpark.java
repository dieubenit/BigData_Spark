package spark;

import java.io.File;

import org.apache.hadoop.record.meta.StructTypeID;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class MySpark {

	private SparkSession spark;
	public static final String winutilsLocation="C:\\winutils\\"; 
	public static final int temps=60; 
	public static final String[] colName= {"temps",
			"utilisateur_sourceAdomaine",
			"utilisateur_destinationAdomaine",
			"ordinateur_source",
			"ordinateur_destination",
			"type_authentification",
			"type_de_connexion",
			"orientation_authentification",
			"succes_echec"}; 
	public static final String MainTab="tab";

	public static void main(String[] args) {

		//filtrage des log
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		//initialisation spark
		System.setProperty("hadoop.home.dir", winutilsLocation);

		//initiation de spark
		MySpark ms= new MySpark();

		//suite partie 1
		//ms.top10UtilOrdi();

		//partie 2
		ms.partie2();

		//partie 3
		

		ms.closeSpark();
	}

	public MySpark(){

		spark = SparkSession
				.builder()
				.config("spark.master", "local")
				.getOrCreate();

		//création du dataframe
		StructType schema = new StructType()
				.add(colName[0],"Integer",true)
				.add(colName[1],"String", true)
				.add(colName[2],"String",true)
				.add(colName[3],"String",true)
				.add(colName[4],"String",true)
				.add(colName[5],"String",true)
				.add(colName[6],"String",true)
				.add(colName[7],"String",true)
				.add(colName[8],"String",true);

		//PARTIE I
		//question 1
		Dataset<Row> df = spark.read()
				.option("header", "true")
				.option("delimiter", ",")
				.option("inferSchema", "true")
				.format("csv")
				.schema(schema)
				.load("auth_500000.txt");
		//df.show();

		//question 2
		df=df.filter(colName[1]+" != '?'");
		df=df.filter(colName[2]+" != '?'");
		df=df.filter(colName[3]+" != '?'");
		df=df.filter(colName[4]+" != '?'");
		df=df.filter(colName[5]+" != '?'");
		df=df.filter(colName[6]+" != '?'");
		df=df.filter(colName[7]+" != '?'");
		df=df.filter(colName[8]+" != '?'");

		//df.show();
		//crée un vue sql sur le dataframe
		df.createOrReplaceTempView(MainTab);
		

	}

	//suite de la partie 1
	private void top10UtilOrdi() {
		//affichage de la table créée question 1+2
		spark.sql("select * from "+MainTab).show();
		//question 3
		Dataset<Row> utilMachine = spark.sql("SELECT count("+colName[1]+", "+colName[3]+") as count,"
				+ " concat("+colName[1]+", ', ' , "+colName[3]+") as utilisateur_par_pc from "+ MainTab
				+" group by utilisateur_par_pc order by count DESC");

		//question 4
		utilMachine.show(10,false);
	}

	private void partie2() {
		//PATTIE II
		//Question 1
		System.out.println("nombre de connexions par utilisateur par paire machine source/destination :\n");
		//(a)
		Dataset<Row> utilConnextionPoids = spark.sql("SELECT utilisateur_sourceAdomaine as utilisateur, concat(ordinateur_source,', ',ordinateur_destination) as connexions, count(utilisateur_sourceAdomaine,(ordinateur_source,ordinateur_destination)) as poids from tab group by utilisateur_sourceAdomaine,connexions");
		utilConnextionPoids.show(10,false);
		
		//(b)
		Dataset<Row> utilEtConnextion=spark.sql("(select utilisateur_sourceAdomaine as utilisateur_et_connexions from tab distinct) UNION (select concat(ordinateur_source,', ',ordinateur_destination) as utilisateur_et_connexions from tab distinct)");
	
		utilEtConnextion.show(10,false);

		//Question 2
		System.out.println("nombre d'authentification avec/sans succes par utilisateur :\n");
		//(a)
		Dataset<Row> utilAthentificationPoids = spark.sql("SELECT utilisateur_sourceAdomaine as utilisateurs, concat(orientation_authentification,', ',succes_echec) as connexions, count(utilisateur_sourceAdomaine,(orientation_authentification,succes_echec)) as poids from tab group by utilisateur_sourceAdomaine,connexions");
		utilAthentificationPoids.show(10,false);
		
		//(b)
		Dataset<Row> utilAthentification=spark.sql("(select utilisateur_sourceAdomaine as utilisateurs_et_connexions from tab distinct) UNION (select concat(ordinateur_source,', ',ordinateur_destination)  from tab distinct) UNION (select concat(orientation_authentification,', ',succes_echec)as utilisateurs_et_connexions from tab distinct)");
		utilAthentification.show(10,false);
		
		//Question 3
		System.out.println("nombre d'authentification avec/sans succes pour chaque machine par utilisateur :\n");
		//(a)
		Dataset<Row> utilAthentificationConPoids = spark.sql("SELECT ordinateur_source as Machine_source, concat(utilisateur_sourceAdomaine,', ',succes_echec) as connexions, count(ordinateur_source,(utilisateur_sourceAdomaine,succes_echec)) as poids from tab group by ordinateur_source,connexions");
		utilAthentificationConPoids.show(10,false);
		
		//(b)
		Dataset<Row> utilAthentificationCon=spark.sql("(select utilisateur_sourceAdomaine as utilisateur_et_connexions from tab distinct) UNION (select concat(utilisateur_sourceAdomaine,', ',succes_echec) as utilisateur_et_connexions from tab distinct)");
		utilAthentificationCon.show(10,false);
		
		System.out.println("Partie II finie");
	}


	private Dataset<Row> relationPaire(String col1,String col2,String nomresultat1,String nomresultat2){
		Dataset<Row> resultat=null;

		return resultat;
	}

	private Dataset<Row> listePaire(String col1,String col2,String nomresultat){
		Dataset<Row> resultat=null;
		resultat=spark.sql("(select "+col1+" as "+nomresultat+" from "+MainTab+" distinct) UNION "
				+ "(select "+col2+" as "+nomresultat+" from "+MainTab+" distinct)");
		return resultat;
	}

	private String combineColumn(String col1,String col2) {
		String res;
		res="concat("+col1+",', ',"+col2+")";
		return res;
	}

	private void autoPaire() {
		for(int i=1;i<9;i++) {
			for(int j=1;j<9;j++) {
				if(j!=i) {
					for(int k=1;k<9;k++) {
						if(k!=i&&k!=j) {
							
						}
					}
				}
			}
		}
	}
	
	private void autoPaireTemps(int fenetreDeTemps,String cheminLog) {
	
	}

	public void closeSpark() {
		spark.close();
	}

}
