
package com.txy.kafka;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.txy.kafka.KafkaUtil;

/**
 * Package: com.uaes.kafka.Gw ClassName: ProducerGw Description:
 * 消费长城demo项目kafka的topic数据 CreateDate: 2018/3/14 19:21 UpdateDate: 2018/3/14
 * 19:21 UpdateRemark: The modified content Version: 1.0
 */

public class ProducerGw {
	private static final Logger logger = LoggerFactory.getLogger(ProducerGw.class);

	public static void main(String[] args) {
		try {
			// KafkaProducer<String, String> producer =
			// KafkaUtil.getProducer("139.196.79.212:9092");
			KafkaProducer<String, String> producer = KafkaUtil.getProducer("localhost:9092");
			for (int i = 1; i < 50; i++) {
				producer.send(new ProducerRecord<String, String>("test", "message_test_" + i)).get();
				System.out.println("发送消息:message_test_" + i);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 异步发送消息
	 */

	public static void sendAsync(KafkaProducer producer, final String topic, final String value) {

		producer.send(new ProducerRecord<String, String>(topic, value), new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if (metadata != null) {
					logger.info("向kafka写入消息成功 {}", value);
				} else {
					logger.error("向kafka写入消息失败 {}", value);
					logger.error(exception.getMessage());
				}
			}
		});
	}

	/**
	 * 同步发送消息
	 */

	public static void sendSync(KafkaProducer producer, final String topic, final String value) {
		try {
			RecordMetadata recordMetadata = (RecordMetadata) producer
					.send(new ProducerRecord<String, String>(topic, value)).get();
			logger.info("同步发送成功");
			logger.info("offset: " + recordMetadata.offset() + " ;partition: " + recordMetadata.partition());
		} catch (InterruptedException | ExecutionException e) {
			logger.error("同步发送失败");
			e.printStackTrace();
		}
	}
}
