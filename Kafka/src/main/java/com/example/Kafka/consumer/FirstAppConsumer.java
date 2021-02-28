package com.example.Kafka.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class FirstAppConsumer {
	
	private static String topicName = "first-app";
	private static final String FIN_MESSAGE = "exit";
	
	public static void main (String[] args) {
		
		String bootstrapServers = "127.0.0.1:9092";
		
		//KafkaConsumer에 필요한 설정
		Properties conf = new Properties();
		conf.setProperty("bootstrap.servers", "kafka-broker01:9092,kafka=broker02:9092,kafka-broker03:9092");
		conf.setProperty("group.id", "FirstAppConsumerGroup");
		conf.setProperty("enable.auto.commit", "false");
		conf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
		conf.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		//카프카 클러스터에서 메시지를 수신(Consume)하는 객체 생성
//		Consumer<Integer, String> consumer = new KafkaConsumer<>(conf);
		 KafkaConsumer<String, String> consumer = new KafkaConsumer<>(conf);
		
		//구독(subscribe)하는 Topic 등록
		consumer.subscribe(Collections.singletonList(topicName));
		
        String message = null;
        try {
            
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100000));

                for (ConsumerRecord<String, String> record : records) {
                    message = record.value();
                    System.out.println(message);
                }
          
//            while (!StringUtils.equals(message, FIN_MESSAGE));
        } catch(Exception e) {
            // exception
        } finally {
		
//		for(int count = 0; count < 300; count++) {
//			//메시지를 수신하여 콘솔에 표시
//			ConsumerRecords<Integer, String> records = consumer.poll(1);
//			for(Consumer<Integer, String> record: records) {
//				String msgString = String.format("key:%d, value:%s", record.key(), record.value());
//				System.out.println(msgString);
//				
//				//처리가 완료된 메시지의 오프셋을 커밋
//				TopicPartition tp = new TopicPartition(record.topic(), record.partitions());
//				OffsetAndMetadata oam = new OffsetAndMetadata(record.offset() + 1);
//				Map<TopicPartition, OffsetAndMetadata> commitInfo = Collections.singletonMap(tp, oam);
//				consumer.commitSync(commitInfo);
//				
//			}
//			try {
//				Thread.sleep(1000);
//			}catch (InterruptedException ex) {
//				ex.printStackTrace();
//			}
//		}
		//KafkaConsumer를 닫고 종료
		consumer.close();
        }
	}

}
