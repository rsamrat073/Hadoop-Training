package com.sapient;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import kafka.producer.Producer;
import kafka.producer.ProducerConfig;

public class KafkaClosePriceProducer {

	private final static String TOPIC = "closeprice";
	private final static String BOOTSTRAP_SERVERS = "localhost:9092";

	private static KafkaProducer<String, String> createProducer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
		 props.put("acks", "all");
		 props.put("retries", 0);
		 props.put("batch.size", 16384);
		 props.put("buffer.memory", 33554432);
		props.put("key.serializer", StringSerializer.class.getName());
		props.put("value.serializer", StringSerializer.class.getName());
		return new KafkaProducer<>(props);
	}
	
	public static void main(String[] args) throws Exception{
		KafkaProducer<String, String> produce = KafkaClosePriceProducer.createProducer();
		ProducerRecord<String, String> p=new ProducerRecord<String, String>(TOPIC,"SAMRAT","ROY");
		//while(true){
			
			produce.send(p);
		//}
		
	}

}
