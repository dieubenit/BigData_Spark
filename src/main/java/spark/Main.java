
package spark;

import java.io.File;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		System.setProperty("hadoop.home.dir", "C:\\winutil\\");
		
		SparkSession spark = SparkSession
				.builder()
				.config("spark.master", "local")
				.getOrCreate();
		File tmpDir = new File("auth_500000.txt");

		Dataset<Row> df = spark.read()
				  .option("header", "true")
				  .option("delimiter", ",")
				  .option("inferSchema", "true")
				  .format("csv")
				  .load("auth_500000.txt");

		df.show();
		
		System.out.println("coucou");
		spark.stop();
	}

}
