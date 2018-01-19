package com.sapient;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import kafka.producer.Producer;
import kafka.producer.ProducerConfig;

public class KafkaClosePriceConsumer {

	private final static String TOPIC = "closeprice";
	private final static String BOOTSTRAP_SERVERS = "localhost:9092";

	private static KafkaConsumer<String, String> createConsumer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return new KafkaConsumer<>(props);
	}

	public static void main(String[] args) throws Exception{
		KafkaConsumer<String, String> consumer = KafkaClosePriceConsumer.createConsumer();

		// Kafka Consumer subscribes list of topics here.
		consumer.subscribe(Arrays.asList(TOPIC));

		int i = 0;

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records)

				// print the offset,key and value for the consumer records.
				System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
		}

	}
}
