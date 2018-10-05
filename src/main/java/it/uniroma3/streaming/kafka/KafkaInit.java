package it.uniroma3.streaming.kafka;

import static it.uniroma3.properties.PropertiesReader.KAFKA_BOOTSRAP_SERVERS;
import static it.uniroma3.properties.PropertiesReader.KAFKA_KEY_DESERIALIZER;
import static it.uniroma3.properties.PropertiesReader.KAFKA_TOPIC;
import static it.uniroma3.properties.PropertiesReader.KAFKA_VALUE_DESERIALIZER;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import it.uniroma3.batch.utils.SparkLoader;
import it.uniroma3.properties.PropertiesReader;

public class KafkaInit {
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();
	private static KafkaInit instance;

	public void init() throws InterruptedException, FileNotFoundException, IOException, ClassNotFoundException {
		String topic = propsReader.getProperty(KAFKA_TOPIC);

		JavaStreamingContext jssc = new JavaStreamingContext(SparkLoader.getInstance().getContext(), Durations.seconds(2));

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", propsReader.getProperty(KAFKA_BOOTSRAP_SERVERS));
		kafkaParams.put("key.deserializer", Class.forName(propsReader.getProperty(KAFKA_KEY_DESERIALIZER)));
		kafkaParams.put("value.deserializer", Class.forName(propsReader.getProperty(KAFKA_VALUE_DESERIALIZER)));
		kafkaParams.put("group.id", topic);

		// Create direct kafka stream with brokers and topics
		JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
				jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.Subscribe(Arrays.asList(new String[] {topic}), kafkaParams));


		// Get the lines, split them into words, count the words and print
		StreamAnalyzer.getInstance().attach(messages).run();

		// Start the computation
		jssc.start();
		jssc.awaitTermination();
	}

	public static KafkaInit getInstance() {
		return (instance==null) ? (instance = new KafkaInit()) : instance;
	}

}
