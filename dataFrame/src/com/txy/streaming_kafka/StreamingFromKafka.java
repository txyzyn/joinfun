package com.txy.streaming_kafka;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;


public class StreamingFromKafka {
	private static JavaStreamingContext ssc;
	private static final Pattern SPACE = Pattern.compile(" ");
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("sparkstreaming").setMaster("yarn-client");
		ssc = new JavaStreamingContext(conf,  Durations.seconds(2));
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "master:9092"); //broker
		kafkaParams.put("key.deserializer", StringDeserializer.class); //密钥的反序列化器类
		kafkaParams.put("value.deserializer", StringDeserializer.class);//
		kafkaParams.put("group.id", "test-consumer-group"); //多个消费者消费一个topic是分发而不是复制
		kafkaParams.put("auto.offset.reset", "latest"); //取最新的offset
		kafkaParams.put("enable.auto.commit", false); //offset不自动提交

		Collection<String> topics = Arrays.asList("NEW", "STU_INFO");//多topic设置

		// Import dependencies and create kafka params as in Create Direct Stream above

		/*OffsetRange[] offsetRanges = {
		  // topic, partition, inclusive starting offset, exclusive ending offset
		  OffsetRange.create("test", 0, 0, 100),
		  OffsetRange.create("test", 1, 0, 100)
		};*/
		
		JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
				ssc,
		        LocationStrategies.PreferConsistent(),
		        ConsumerStrategies.Subscribe(topics, kafkaParams));
		
		//messages.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
		
		// Get the lines, split them into words, count the words and print
	    JavaDStream<String> lines = messages.map(ConsumerRecord::value);
	    lines.map(s->{
	    	System.out.println(s);
	    	return s;
	    });
	    JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
	    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
	        .reduceByKey((i1, i2) -> i1 + i2);
	    wordCounts.print();

	    // Start the computation
	    ssc.start();
	    try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		/*JavaRDD<ConsumerRecord<String, String>> rdd = KafkaUtils.createRDD(
				  ssc,
				  kafkaParams,
				  offsetRanges,
				  LocationStrategies.PreferConsistent()
				);*/
/*		stream.foreachRDD(rdd -> {
			  OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
			  rdd.foreachPartition(consumerRecords -> {
			    OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
			    System.out.println(
			      o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
			  });
			});
		// The details depend on your data store, but the general idea looks like this

		// begin from the the offsets committed to the database
		Map<TopicPartition, Long> fromOffsets = new HashMap<>();
		for (resultSet : selectOffsetsFromYourDatabase)
		  fromOffsets.put(new TopicPartition(resultSet.string("topic"), resultSet.int("partition")), resultSet.long("offset"));
		}

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
		  streamingContext,
		  LocationStrategies.PreferConsistent(),
		  ConsumerStrategies.<String, String>Assign(fromOffsets.keySet(), kafkaParams, fromOffsets)
		);

		stream.foreachRDD(rdd -> {
		  OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
		  
		  Object results = yourCalculation(rdd);

		  // begin your transaction

		  // update results
		  // update offsets where the end of existing offsets matches the beginning of this batch of offsets
		  // assert that offsets were updated correctly

		  // end your transaction
		});*/
	}
}
