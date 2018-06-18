package com.txy.dataframe;

import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.api.java.JavaRDD;

public class Spark_Conf {
	private static String appName = "JavaWordCountDemo";
	private static String master = "spark://192.168.31.128:7077";
	private static JavaSparkContext sc;
	public static void main(String[] args) {
		// 初始化Spark
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(master).set("yarn.resourcemanager.address", "192.168.31.128:8030");
		sc = new JavaSparkContext(conf);
		JavaRDD<String> distFile = sc.textFile("/test/spark_test.txt");
		
		JavaRDD<String> data_60 = distFile.map(s->{
			String s_new = null;
			if(s.contains("0")) 
				s_new = s;
			return s_new;
		});
		HashMap<String,String> map = new HashMap<>();
		//加到内存
		data_60.persist(StorageLevel.MEMORY_ONLY());
		data_60.reduce((a,b)->map.put(a, b));
		System.out.println(map);
	}
}
