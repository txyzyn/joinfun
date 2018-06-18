package com.txy.dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataFrame {

	public static void main(String[] args) {
		/*
		 * spark1.x
		 */
		/*SparkConf conf = new SparkConf().setMaster("local").setAppName("test");
		JavaSparkContext sc = new JavaSparkContext(conf);
		HiveContext sqlContext = new HiveContext(sc);
		DataFrame jdbcDF = sqlContext.read().format("jdbc")
				//.option("url", "jdbc:oracle://master:1521/txy")
				.option("url", "jdbc:mysql://master:3306/txy")
				.option("dbtable", "(select * from mysql_date_test) T")
				.option("user", "root")
				.option("password", "Txy0904")
				// .option("driver","oracle.jdbc.driver.OracleDriver")
				.option("driver", "com.mysql.jdbc.Driver").load();*/
		/*
		 * spark2.x
		 */
		SparkSession spark = SparkSession
				  .builder()
				  .appName("Java Spark Hive Example")
				  .enableHiveSupport()
				  .getOrCreate();
		Dataset<Row> jdbcDF = spark.read()
				  .format("jdbc")
				  .option("url", "jdbc:mysql://master:3306/txy")
				  .option("dbtable", "(select * from stu_info) T")
				  .option("user", "root")
				  .option("password", "Txy0904")
				  .load();
		jdbcDF.show();
		Dataset<Row> hiveDF = spark.sql("select * from ods_ord.oracle");
		hiveDF.show();
		/*Tuple2<String, String>[] dtype = jdbcDF.dtypes();
		for (Tuple2<String, String> t : dtype) {
			System.out.println(t._1 + "====" + t._2);
		}*/
	}
}
