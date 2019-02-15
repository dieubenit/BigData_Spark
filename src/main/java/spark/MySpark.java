package spark;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

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

/**
 * 
 * @author Guillaume Lipski
 * @author Innocent Ouefio
 *
 */
public class MySpark {

	private SparkSession spark;
	private Dataset<Row> df;
	public static final String jsonDestinationFolder=".\\json\\"; 
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
		
		//gestion des 1ers arguments
		String winutils ="C:\\winutils\\",tmp;
		int temps=60;
		if(args.length>=1) {
			tmp=args[0];
			if(!tmp.equals("-")) {
				winutils=tmp;
			}
		}

		//initialisation spark
		System.setProperty("hadoop.home.dir", winutils);

		MySpark ms= new MySpark();
		Dataset<Row> df=ms.getDF();

		//sélection de la partie du tp a traité
		if(args.length>=2) {
			switch (args[1]) {
			case "1":
				ms.top10UtilOrdi();
				break;
			case "2":
				ms.partie2();
				break;
			case "3":
				autoPaire(df);
				break;
			case "4":
				if(args.length>=3) {
					try{
						temps=Integer.parseInt(args[2]);
						if(temps<0) {
							temps=60;
						}
					}catch (Exception e) {
						System.out.println("Ereur, format attendus : mySpark [Numero_partie] [fenetre_temps]");
					}
				}
				autoPaireTemps(df,temps);
				break;
			default :
				ms.top10UtilOrdi();
				ms.partie2();
				autoPaire(df);
				autoPaireTemps(df,temps);
				break;
			}
		}else {
			ms.top10UtilOrdi();
			ms.partie2();
			autoPaire(df);
			autoPaireTemps(df,temps);
		}
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
		df= spark.read()
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
		System.out.println("Table générée :");
		spark.sql("select * from "+MainTab).show();
		//question 3
		Dataset<Row> utilMachine = spark.sql("SELECT count("+colName[1]+", "+colName[3]+") as count,"
				+ " concat("+colName[1]+", ', ' , "+colName[3]+") as utilisateur_par_pc from "+ MainTab
				+" group by utilisateur_par_pc order by count DESC");

		//question 4
		System.out.println("Top 10 Utilisateur par Ordinateur");
		utilMachine.show(10,false);

	}

	private void partie2() {
		//PATTIE II
		//Question 1
		System.out.println("nombre de connexions par utilisateur par paire machine source/destination :\n");
		//(a)
		Dataset<Row> utilConnextionPoids = this.relationPaire(df,colName[1],colName[3],colName[4] ,"utilisateur", "connexions_sur_machines"); 
		//spark.sql("SELECT utilisateur_sourceAdomaine as utilisateur, concat(ordinateur_source,', ',ordinateur_destination) as connexions, count(utilisateur_sourceAdomaine,(ordinateur_source,ordinateur_destination)) as poids from tab group by utilisateur_sourceAdomaine,connexions");
		utilConnextionPoids.show(10,false);
		//(b)

		Dataset<Row> utilEtConnextion=this.listePaire(df,colName[1],colName[3],colName[4],"utilisateur_et_connexions_sur_machines");
		//spark.sql("(select utilisateur_sourceAdomaine as utilisateur_et_connexions from tab distinct) UNION (select concat(ordinateur_source,', ',ordinateur_destination) as utilisateur_et_connexions from tab distinct)");
		utilEtConnextion.show(10,false);


		//Question 2
		System.out.println("nombre d'authentification avec/sans succes par utilisateur :\n");
		//(a)
		Dataset<Row> utilAthentificationPoids = this.relationPaire(df,colName[1],colName[7],colName[8] ,"utilisateur", "authentification_succes"); 
		//spark.sql("SELECT utilisateur_sourceAdomaine as utilisateurs, concat(orientation_authentification,', ',succes_echec) as connexions, count(utilisateur_sourceAdomaine,(orientation_authentification,succes_echec)) as poids from tab group by utilisateur_sourceAdomaine,connexions");
		utilAthentificationPoids.show(10,false);

		//(b)
		Dataset<Row> utilAthentification=this.listePaire(df,colName[1],colName[7],colName[8],"utilisateur_et_authentification_succes");
		//spark.sql("(select utilisateur_sourceAdomaine as utilisateurs_et_connexions from tab distinct) UNION (select concat(ordinateur_source,', ',ordinateur_destination)  from tab distinct) UNION (select concat(orientation_authentification,', ',succes_echec)as utilisateurs_et_connexions from tab distinct)");
		utilAthentification.show(10,false);

		//Question 3
		System.out.println("nombre d'authentification avec/sans succes pour chaque machine par utilisateur + succes :\n");
		//(a)
		Dataset<Row> utilAthentificationConPoids = this.relationPaire(df,colName[3],colName[1],colName[8] ,"machine_source", "utilisateur_succes");
		//spark.sql("SELECT ordinateur_source as Machine_source, concat(utilisateur_sourceAdomaine,', ',succes_echec) as connexions, count(ordinateur_source,(utilisateur_sourceAdomaine,succes_echec)) as poids from tab group by ordinateur_source,connexions");
		utilAthentificationConPoids.show(10,false);

		//(b)
		Dataset<Row> utilAthentificationCon= this.listePaire(df,colName[3],colName[1],colName[8] ,"machine_source_et_utilisateur_succes");
		//spark.sql("(select ordinateur_source as utilisateur_et_connexions from tab distinct) UNION (select concat(utilisateur_sourceAdomaine,', ',succes_echec) as utilisateur_et_connexions from tab distinct)");
		utilAthentificationCon.show(10,false);

		System.out.println("Partie II finie");
	}


	public static Dataset<Row> relationPaire(Dataset<Row> df,String col1,String col2,String col3,String nomresultat1,String nomresultat2){
		Dataset<Row> resultat=null;
		resultat = df.withColumn(nomresultat1,df.col(col1))
				.withColumn(nomresultat2, concat(df.col(col2),
						lit(", "), df.col(col3)))
				.groupBy(col1,nomresultat2).count().withColumnRenamed("count", "poids")
				.orderBy(org.apache.spark.sql.functions.col("poids").desc());
		return resultat;
	}

	public static Dataset<Row> listePaire(Dataset<Row> df,String col1,String col2,String col3,String nomresultat){
		Dataset<Row> resultat=null;
		resultat = df.select(col1).distinct().union(df.withColumn(nomresultat, concat(df.col(col2),lit(", "), df.col(col3))).select(nomresultat).distinct()).toDF(nomresultat);
		return resultat;
	}

	public static void autoPaire(Dataset<Row> df) {
		Dataset<Row> resultat=null;
		int length=df.schema().length();
		String[] column=df.columns();
		for(int i=1;i<length;i++) {
			for(int j=1;j<length;j++) {
				if(j!=i) {
					for(int k=j+1;k<length;k++) {
						if(k!=i&&k!=j) {
							System.out.println("paire : ("+i+"("+j+","+k+"))");
							resultat=relationPaire(df,column[i],column[j],column[k],column[i],column[j]+"AND"+column[k]);
							resultat.write().json(jsonDestinationFolder+"paire"+i+"_"+j+"_"+k+"");
						}
					}
				}
			}
		}
	}

	public static void autoPaireTemps(Dataset<Row> df,int fenetreDeTemps) {
		Dataset<Row> resultat=null,temp=null;
		int length=df.schema().length();
		String[] column=df.columns();
		int t=0;
		temp=df.filter(column[0]+">"+t+" AND "+column[0]+"<="+fenetreDeTemps);
		while(temp.count()>0) {
			for(int i=1;i<length;i++) {
				for(int j=1;j<length;j++) {
					if(j!=i) {
						for(int k=j+1;k<length;k++) {
							if(k!=i&&k!=j) {
								System.out.println("paire "+t+" a "+(t+fenetreDeTemps)+" : ("+i+"("+j+","+k+"))");
								resultat=relationPaire(df,column[i],column[j],column[k],column[i],column[j]+"AND"+column[k]);
								resultat.write().json(jsonDestinationFolder+"paire_"+t+"_a_"+(t+fenetreDeTemps)+"_"+i+"_"+j+"_"+k+"");
							}
						}
					}
				}
			}
			t+=fenetreDeTemps;
			temp=df.filter(column[0]+">"+t+" AND "+column[0]+"<="+(t+fenetreDeTemps));
		}
	}


	private Dataset<Row> getDF() {
		// TODO Auto-generated method stub
		return df;
	}


	public void closeSpark() {
		spark.close();
	}

}
