package spatk.com.controller.getdata;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
// $example off:schema_merging$
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
// $example on:basic_parquet_example$
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.sql.Encoders;
// $example on:schema_merging$
// $example on:json_dataset$
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.getMinMax.time.GetMinMaxTime;
import com.google.protobuf.ServiceException;

public class GetDataFromPostgresql {
	static Configuration connect = null;
	static public String maxTime;
	static public String minTime;
	private static BufferedReader readMax;

	public static void main(String[] SparkToPostgresql) throws ClassNotFoundException, MasterNotRunningException,
			ZooKeeperConnectionException, ServiceException, IOException {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		GetMinMaxTime min = new GetMinMaxTime();
		getlastRowTime max = new getlastRowTime();

		connect = HBaseConfiguration.create();
		connect.set("hbase.zookeeper.quorum",
				"IP");
		connect.set("hbase.zookeeper.property.clientPort", "2181");
		connect.set("hbase.rpc.timeout", "120000000");
		connect.set("hbase.client.scanner.timeout.period", "120000000");
		HBaseAdmin.checkHBaseAvailable(connect);

		try {
			Timer t = new Timer();
			t.schedule(new TimerTask() {

				@Override
				public void run() {

					try {
						maxTime = readMaxTimeFromFile();
						minTime = readMinTimeFromFile();

						if (maxTime == null&&minTime == null) {
							maxTime = max.getCall("dbname", "username", "pass",
									"Query");
							writeMaxTimeInToFile(maxTime);
							minTime = min.getMinTime("dbname", "username", "pass",
									"Query");;
							writeMinTimeInToFile(minTime);
							getData(maxTime,minTime);
						}
						if(maxTime != null&&minTime != null){
							String max2time=readMaxTimeFromFile();
					             writeMinTimeInToFile(max2time);
						       minTime=readMinTimeFromFile();
						      String max2Time = max.("dbname", "username", "pass",
									"Query");
								writeMaxTimeInToFile(max2Time);
								String max3time=readMaxTimeFromFile();
								getData(max3time,max2Time);
							System.out.println("Swap Time" + max3time+ ""+max2Time);
						}

					} catch (ClassNotFoundException e) {

						//e.printStackTrace();
					} catch (IOException e) {

						//e.printStackTrace();
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

				private void getData(String data, String mintimedata) throws ClassNotFoundException, ParseException {
					System.out.println("spark function" + "This is data value  " + data + " this is mintimedata value"
							+ mintimedata);
					SparkSession spark = SparkSession.builder().master("local[*]")
							.appName("Java Spark SQL data sources example")
							.config("spark.some.config.option", "some-value").getOrCreate();

					Dataset<Row> jdbcDF1 = spark.read().format("jdbc")
							.option("url", "jdbc:postgresql://ip:5432/dbname")
							.option("dbtable",
									"(query) as tmp")
							.option("user", "username").option("password", "pass").load()
							.filter("updated_on > 1571210606");

					// jdbcDF1.show();
					PutHbase puts = new PutHbase();
					jdbcDF1.show();
					try {
					jdbcDF1.write().format("com.databricks.spark.avro").option("header", "true").save("hdfs://ip:9000/new.avro");
					System.out.println("done with import hdfs");
					}catch(Exception e)
					{
						e.printStackTrace();
					}
					//	PutHadoop.getItToHDFS( jdbcDF1);
//					jdbcDF1.foreach(k -> {
//						try {
//
//							System.out.println("getting data");
////							puts.getItToHbase(read data), connect);
//
//						
//
//						} catch (Exception e) {
//							e.printStackTrace();
//						}
//					});

				}

			}, 0, 6000);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	



	public static String readMaxTimeFromFile() throws IOException {
		readMax = new BufferedReader(new FileReader("MaxTime.txt"));
		maxTime = readMax.readLine();
		readMax.close();
		System.out.println("this is max readValue"+maxTime);
		return maxTime;

	}

	public static void writeMaxTimeInToFile(String timemax) throws IOException {
		FileOutputStream fos = new FileOutputStream("MaxTime.txt");
		OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fos);
		BufferedWriter br = new BufferedWriter(outputStreamWriter);
		br.write(timemax);
		System.out.println("this is max WriteValue"+timemax);
		br.close();

	}

	public static void writeMinTimeInToFile(String timemin) throws IOException {
		FileOutputStream fos = new FileOutputStream("MinTime.txt");
		OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fos);
		BufferedWriter br = new BufferedWriter(outputStreamWriter);
		br.write(timemin);
		System.out.println("this is min WriteValue"+timemin);
		br.close();

	}

	public static String readMinTimeFromFile() throws IOException {
		BufferedReader readMinTime = new BufferedReader(new FileReader("MinTime.txt"));
		minTime = readMinTime.readLine();
		readMinTime.close();
		System.out.println("this is min readValue"+minTime);
		return minTime;

	}

}
