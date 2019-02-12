
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

public class Main {

	public static void main(String[] args) {

		//filtrage des log
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		//initialisation spark
		System.setProperty("hadoop.home.dir", "C:\\winutils\\");
		
		SparkSession spark = SparkSession
				.builder()
				.config("spark.master", "local")
				.getOrCreate();
		
		//création du dataframe
		StructType schema = new StructType()
			      .add("temps","Integer",true)
			      .add("utilisateur_sourceAdomaine","String", true)
			      .add("utilisateur_destinationAdomaine","String",true)
			      .add("ordinateur_source","String",true)
			      .add("ordinateur_destination","String",true)
			      .add("type_authentification","String",true)
			      .add("type_de_connexion","String",true)
			      .add("orientation_authentification","String",true)
			      .add("succes_echec","String",true);

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
		df=df.filter("utilisateur_sourceAdomaine != '?'");
		df=df.filter("utilisateur_destinationAdomaine != '?'");
		df=df.filter("ordinateur_source != '?'");
		df=df.filter("ordinateur_destination != '?'");
		df=df.filter("type_authentification != '?'");
		df=df.filter("type_de_connexion != '?'");
		df=df.filter("orientation_authentification != '?'");
		df=df.filter("succes_echec != '?'");

		//df.show();
		//crée un vue sql sur le dataframe
		df.createOrReplaceTempView("tab");

		//question 3
		//Dataset<Row> utilMachine = spark.sql("SELECT count(utilisateur_sourceAdomaine,ordinateur_source) as count, (utilisateur_sourceAdomaine,ordinateur_source) as utilisateurparpc from tab group by (utilisateur_sourceAdomaine,ordinateur_source) order by count DESC");

		//question 4
		//utilMachine.show(10,false);
		
		// PARTIE II
		//Question 1
		//(a)
		//Dataset<Row> utilConnextionPoids = spark.sql("SELECT utilisateur_sourceAdomaine as utilisateur, concat(ordinateur_source,', ',ordinateur_destination) as connexions, count(utilisateur_sourceAdomaine,(ordinateur_source,ordinateur_destination)) as poids from tab group by utilisateur_sourceAdomaine,connexions");
		//utilConnextionPoids.show(10,false);
		
		//(b)
		//Dataset<Row> utilEtConnextion=spark.sql("(select utilisateur_sourceAdomaine as utilisateur_et_connexions from tab distinct) UNION (select concat(ordinateur_source,', ',ordinateur_destination) as utilisateur_et_connexions from tab distinct)");
		
		//Dataset<Row> utilEtConnextion=listePaire("utilisateur_sourceAdomaine",combineColumn("ordinateur_source","ordinateur_destination"),"utilisateur_et_connexions");
		
		//utilEtConnextion.show(20,false);

		//Question 2
		//(a)
		//Dataset<Row> utilAthentificationPoids = spark.sql("SELECT utilisateur_sourceAdomaine as utilisateurs, concat(orientation_authentification,', ',succes_echec) as connexions, count(utilisateur_sourceAdomaine,(orientation_authentification,succes_echec)) as poids from tab group by utilisateur_sourceAdomaine,connexions");
		//utilAthentificationPoids.show(1000,false);
		
		//(b)
		//Dataset<Row> utilAthentification=spark.sql("(select utilisateur_sourceAdomaine as utilisateurs_et_connexions from tab distinct) UNION (select concat(ordinateur_source,', ',ordinateur_destination)  from tab distinct) UNION (select concat(orientation_authentification,', ',succes_echec)as utilisateurs_et_connexions from tab distinct)");
		//utilAthentification.show(10,false);
		
		//Question 3
		//(a)
		//Dataset<Row> utilAthentificationConPoids = spark.sql("SELECT ordinateur_source as Machine_source, concat(utilisateur_sourceAdomaine,', ',succes_echec) as connexions, count(ordinateur_source,(utilisateur_sourceAdomaine,succes_echec)) as poids from tab group by ordinateur_source,connexions");
		//utilAthentificationConPoids.show(2000,false);
		
		//(b)
		//Dataset<Row> utilAthentificationCon=spark.sql("(select utilisateur_sourceAdomaine as utilisateur_et_connexions from tab distinct) UNION (select concat(utilisateur_sourceAdomaine,', ',succes_echec) as utilisateur_et_connexions from tab distinct)");
		//utilAthentificationCon.show(200,false);
		
		spark.close();
	}
	
	
	
	private Dataset<Row> relationPaire(String col1,String col2,String nomresultat1,String nomresultat2){
		Dataset<Row> resultat=null;
		
		return resultat;
	}
	
	private Dataset<Row> listePaire(String col1,String col2,String nomresultat){
		Dataset<Row> resultat=null;
		//resultat=spark.sql("(select "+col1+" as "+nomresultat+" from tab distinct) UNION (select "+col2+" as "+nomresultat+" from tab distinct)");

		return resultat;
	}
	
	private String combineColumn(String col1,String col2) {
		String res;
		res="concat("+col1+",', ',"+col2+")";
		return res;
	}
	
}


