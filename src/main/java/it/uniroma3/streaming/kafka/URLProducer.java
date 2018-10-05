package it.uniroma3.streaming.kafka;

import static it.uniroma3.properties.PropertiesReader.KAFKA_BOOTSRAP_SERVERS;
import static it.uniroma3.properties.PropertiesReader.KAFKA_KEY_SERIALIZER;
import static it.uniroma3.properties.PropertiesReader.KAFKA_TOPIC;
import static it.uniroma3.properties.PropertiesReader.KAFKA_VALUE_SERIALIZER;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import it.uniroma3.properties.PropertiesReader;

public class URLProducer {
	private KafkaProducer<Long, String> innerProducer;
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();
	
	public URLProducer() throws FileNotFoundException,
								IOException,
								InterruptedException,
								ClassNotFoundException {
		
		Map<String, Object> params = new HashMap<>();
		params.put("key.serializer", Class.forName(propsReader.getProperty(KAFKA_KEY_SERIALIZER)));
		params.put("value.serializer", Class.forName(propsReader.getProperty(KAFKA_VALUE_SERIALIZER)));
		params.put("bootstrap.servers", propsReader.getProperty(KAFKA_BOOTSRAP_SERVERS));
		
		this.innerProducer = new KafkaProducer<>(params);
	}
	
	public void produceMessage(String message) throws FileNotFoundException, IOException {
		this.innerProducer.send(new ProducerRecord<Long, String>(propsReader.getProperty(KAFKA_TOPIC), message));
	}
	
	public void close() {
		this.innerProducer.close();
	}

}
