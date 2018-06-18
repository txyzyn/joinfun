
package com.txy.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import com.txy.kafka.KafkaUtil;

/**
 * Package: com.uaes.kafka.Gw ClassName: ConsumerGw Description:
 * 消费长城demo项目kafka的topic数据 CreateDate: 2018/3/14 19:21 UpdateDate: 2018/3/14
 * 19:21 UpdateRemark: The modified content Version: 1.0
 */

public class ConsumerGw {
	public static void main(String[] args) {
		KafkaConsumer<String, String> consumer = KafkaUtil.getConsumer("192.168.31.128:9092", "test-consumer-group",
				"NEW");

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records)
				System.out.println(record.value());

		}
	}
}
