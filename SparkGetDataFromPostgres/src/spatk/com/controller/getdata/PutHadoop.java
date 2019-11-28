package spatk.com.controller.getdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PutHadoop {
	
	public static void getItToHDFS(Dataset<Row> jdbcDF1) {
		jdbcDF1.show();
		//jdbcDF1.write().format("com.databricks.spark.avro").option("header", "true").save("hdfs://192.168.11.130:9000/2012/jan/new.avro");
		
	}

}
