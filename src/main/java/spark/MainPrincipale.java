
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
import org.apache.spark.sql.catalyst.expressions.aggregate.Count;
import org.apache.spark.sql.types.StructType;

public class MainPrincipale {

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
		//df.groupBy("utilisateur_sourceAdomaine","ordinateur_source" ).count().orderBy(org.apache.spark.sql.functions.col("count").desc()).show(10);

		//question 4
		//utilMachine.show(10,false);
		
		// PARTIE II
		
		//Question 1-a
		System.out.println("Question 1-a");
        Dataset<Row> Connexion1  = Partie2.DFselect (df, "utilisateur_sourceAdomaine","ordinateur_source","ordinateur_destination");              
        Dataset<Row> Connexion2  = Partie2.GroupConnexion(Connexion1, "utilisateur_sourceAdomaine", "ordinateur_source","ordinateur_destination", "Connexion");
        //Connexion2.show(10);
        Connexion2.toDF("Utilisateurs","Connexions","Poids").show(10);
		
      //Question 1-b
        System.out.println("Question 1-b");
        Dataset<Row> Util_Connex = Partie2.ColUnion(Connexion2, "utilisateur_sourceAdomaine", "Connexion", "utilisateurs_et_connexions");
       // Util_Connex.show(100);
       Util_Connex.toDF("Utilisateurs et Connexions").show(10);
		
      //Question 2-a    
        System.out.println("Question 2-a");
        Dataset<Row> UtiLog1 = Partie2.DFselect (df, "utilisateur_sourceAdomaine","orientation_authentification","succes_echec");
        Dataset<Row> UtiLog2 = Partie2.GroupConnexion(UtiLog1, "utilisateur_sourceAdomaine", "orientation_authentification","succes_echec", "Connexion2");
        //UtiLog2.show(10);
        UtiLog2.toDF("Utilisateurs","Connexions","Poids").show(10);
        
        //Question 2-b    
        System.out.println("Question 2-b");
        Dataset<Row> UtiLog = Partie2.ColUnion(UtiLog2, "utilisateur_sourceAdomaine", "Connexion2","utilisateurs_et_connexions");
        UtiLog.toDF("Utlisateurs et Connexions").show(10);
		
        //Question 3-a
        System.out.println("Question 3-a");
        Dataset<Row> User_ConnexPoid2  = Partie2.GroupConnexion(df, "utilisateur_sourceAdomaine", "utilisateur_sourceAdomaine","succes_echec", "Connexions");
        //User_ConnexPoid2.show(10);
        User_ConnexPoid2.toDF("Utilisateurs","Connexions","Poids").show(10);
        
      //Question 3-b
        System.out.println("Question 3-b");
        Dataset<Row> User_Connex2 = Partie2.ColUnion(User_ConnexPoid2, "utilisateur_sourceAdomaine", "Connexions", "succes_echec");
        User_Connex2.toDF("Utilisateurs et Connexions").show(10);

		
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


