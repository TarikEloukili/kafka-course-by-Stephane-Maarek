package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.logging.Logger;

public class ProducerDemo {

    private static final Logger log = Logger.getLogger(ProducerDemo.class.getName());

    public static void main(String[] args) {
        log.info("I am kafka producer");

        // create producer properties
        Properties properties = new Properties();

        //connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("demo_java", "hello world");

        // send data
        producer.send(record);

        // flush and close producer
        //tell producer to block until sending all data -- synchronous
        producer.flush();
        // btw .close() also use flush behind the scenes
        producer.close();

    }
}
