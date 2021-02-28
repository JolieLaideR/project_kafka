package com.example.Kafka.producer;

import java.util.Properties;

import javax.security.auth.callback.Callback;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FirstAppProducer {

	private static String topicName = "first-app";

	public static void main(String[] args) {
		
		final Logger logger = LoggerFactory.getLogger(FirstAppProducer.class);
		
		String bootstrapServers = "127.0.0.1:9092";

		// KafkaProducer에 필요한 설정
		Properties conf = new Properties();
		conf.setProperty("bootstrap.servers", "kafka-broker01:9092,kafka=broker02:9092,kafka-broker03:9092");
		conf.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		conf.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		// 카프카 클러스터에 메시지를 송신(produce)하는 객체 생성
//		Producer<Integer, String> producer = new KafkaProducer<>(conf);
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(conf);

//		int key;
//		String value;
//		for (int i = 1; i <= 100; i++) {
//			key = i;
//			value = String.valueOf(i);

			// 송신 메시지 생성
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("second_topic", "hello world");

			// 메시지를 송신하고 Ack를 받았을 때 실행할 작업(Callback) 등록
			producer.send(record);
//					, new Callback() {
				
//				 @Override
//				public void onCompletion(RecordMetadata metadata, Exception e) {
//					 if (e == null) {
//							// the record was successfully sent
//							logger.info("Received new metadata. \n"
//								+ "Topic:" + metadata.topic() + "\n"
//								+ "Partition:" + metadata.partition() + "\n"
//								+ "Offset:" + metadata.offset() + "\n"
//								+ "TimeStamp:" + metadata.timestamp());
//						} else {
//							logger.error("Error while producing", e);
//						}
//					if (metadata != null) {
//						// 송신에 성공한 경우 처리
//						String infoString = String.format("Success partition:%d, offset:%d", metadata.partition(),
//								metadata.offset());
//						System.out.println(infoString);
//					} else {
//						// 송신에 실패한 경우 처리
//						String infoString = String.format("Failed:%s", e.getMessage());
//						System.err.println(infoString);
//					}
//				}
//			});
//		}
		// flush data
				producer.flush();

				// flush and close producer
				producer.close();
		// KafkaProducer를 닫고 종료
//		producer.close();

	}

}
