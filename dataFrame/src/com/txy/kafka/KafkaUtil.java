
package com.txy.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * Package: com.uaes.kafka.util ClassName: KafkaUtil Description: kafka工具类
 * CreateDate: 2018/3/14 19:03 UpdateDate: 2018/3/14 19:03 UpdateRemark: The
 * modified content Version: 1.0
 */

public class KafkaUtil {
	// private static final Logger logger =
	// LoggerFactory.getLogger(KafkaUtil.class);

	/**
	 * 获取自动提交偏移量方式的消费者
	 * "share.kafka.xchanger.cn:9012,share.kafka.xchanger.cn:9013,share.kafka.xchanger.cn:9014"
	 * Producer即生产者，向Kafka集群发送消息，在发送消息之前，会对消息进行分类，即Topic，上图展示了两个producer发送了分类为topic1的消息，另外一个发送了topic2的消息。
	 * Topic即主题，通过对消息指定主题可以将消息分类，消费者可以只关注自己需要的Topic中的消息
	 * Consumer即消费者，消费者通过与kafka集群建立长连接的方式，不断地从集群中拉取消息，然后可以对这些消息进行处理。
	 * 从上图中就可以看出同一个Topic下的消费者和生产者的数量并不是对应的。
	 * 
	 * @param broker_list
	 * @param groupId
	 * @param topic
	 * @return
	 */

	public static KafkaConsumer<String, String> getConsumer(String broker_list, String groupId, String topic) {
		Properties props = new Properties();
		props.put("bootstrap.servers", broker_list);
		props.put("group.id", groupId);
		props.put("enable.auto.commit", "true");
		props.put("auto.offset.reset", "earliest");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("request.timeout.ms", "60000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topic));
		return consumer;
	}

	/**
	 * 获取自动提交偏移量方式的消费者,使用SASL权限控制
	 */

	public static KafkaConsumer<String, String> getConsumer(String broker_list, String groupId, String topic,
			String slas) {
		Properties props = new Properties();
		props.put("bootstrap.servers", broker_list);
		props.put("group.id", groupId);
		props.put("enable.auto.commit", "true");
		props.put("auto.offset.reset", "earliest");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		// props.put("request.timeout.ms", "60000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		// src/main/resources/kafka_client_jaas.conf
		/// usr/uaes/test/kafka_client_jaas.conf
		System.setProperty("java.security.auth.login.config", slas); // 环境变量添加，需要输入配置文件的路径
		props.put("security.protocol", "SASL_PLAINTEXT");
		props.put("sasl.mechanism", "PLAIN");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topic));
		return consumer;
	}

	/**
	 * 初始化kafka生产者
	 */

	public static KafkaProducer<String, String> getProducer(String broker_list) {
		Properties props = new Properties();
		props.put("bootstrap.servers", broker_list);// 服务器ip:端口号，集群用逗号分隔
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		return new KafkaProducer<>(props);
	}

	/**
	 * 初始化kafka生产者,使用SASL权限控制
	 */

	public static KafkaProducer<String, String> getProducer(String broker_list, String sals) {
		Properties props = new Properties();
		props.put("bootstrap.servers", broker_list);// 服务器ip:端口号，集群用逗号分隔
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		// src/main/resources/kafka_client_jaas.conf
		/// usr/uaes/test/
		System.setProperty("java.security.auth.login.config", sals); // 环境变量添加，需要输入配置文件的路径
		props.put("security.protocol", "SASL_PLAINTEXT");
		props.put("sasl.mechanism", "PLAIN");

		return new KafkaProducer<>(props);
	}

}
