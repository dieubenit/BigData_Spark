
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
		System.setProperty("hadoop.home.dir", "C:\\winutil\\");
		
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
		Dataset<Row> utilConnextionPoids = spark.sql("SELECT utilisateur_sourceAdomaine as utilisateur, (ordinateur_source,ordinateur_destination) as connexions, count(utilisateur_sourceAdomaine,(ordinateur_source,ordinateur_destination)) as poids from tab group by utilisateur_sourceAdomaine,(ordinateur_source,ordinateur_destination)");

		//utilConnextionPoids.show(10,false);
		//(b) TODO
		Dataset<Row> utilEtConnextion=spark.sql("(SELECT utilisateur_sourceAdomaine as utilisateur_et_connexions from tab)UNION(SELECT ordinateur_source + ordinateur_destination as utilisateur_et_connexions from tab)");

		utilConnextionPoids.show(100,false);
		spark.close();
	}
	
	
}


