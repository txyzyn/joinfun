package com.dataframe.txy;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class DataFrame {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        Dataset<Row> df = spark.read().json("examples/src/main/resources/people.json");
        df.createOrReplaceTempView("people");
        spark.sql("SELECT * FROM global_temp.people").show();
        Tuple2<String, String>[] dtypes = df.dtypes();
        for(Tuple2 t:dtypes){
            if(t._2().equals("String")){

            }
        }
    }
}
