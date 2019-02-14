package spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public class Partie2 extends Main{
	
	public static Dataset<Row> DFselect (Dataset<Row> ds, String col1, String col2, String col3) {
		
		Dataset<Row> selected= ds.select(ds.col(col1),
	        		            ds.col(col2),
	        		            ds.col(col3));
		return selected;

	}
	
	public static Dataset<Row> GroupConnexion (Dataset<Row> ds, String col1, String col2, String col3,String Connexion) {
		Dataset<Row> GroupConnexion = ds.withColumn(Connexion, concat(ds.col(col2),
				                 lit(","), ds.col(col3)))
				                 .groupBy(col1,Connexion).count()
				                 .orderBy(org.apache.spark.sql.functions.col("count").desc());
		return GroupConnexion;

	}

	public static Dataset<Row> ColUnion(Dataset<Row> df, String col1,
			String col2, String name) {
		Dataset<Row> Union = df.select(col1).union(df.select(col2)).distinct().toDF(name);
		return Union;
	}

}
