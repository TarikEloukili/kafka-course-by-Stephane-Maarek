package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;

public class ConsumerDemoCooperative {

    private static final Logger log = Logger.getLogger(ConsumerDemoCooperative.class.getName());

    public static void main(String[] args) {
        log.info("I am kafka consumer");

        String groupId = "my-kafka-application";
        String topic = "demo_java";

        // create consumer properties
        Properties properties = new Properties();

        //connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // create consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());


        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //get reference to the main thread
        final Thread mainThread = Thread.currentThread();

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("Detecting a shutdown, let's exit by calling consumer.wakeup() ...");
                consumer.wakeup();

                //join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        }); 

        try{
            // subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));
            // poll for data
            while(true){

                ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(1000));

                for(ConsumerRecord<String, String> record : records){
                    log.info("key: " + record.key() + ", value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shutdown");
        } catch (Exception e) {
            log.info("unexpected exception");
        }finally{
            consumer.close();
            log.info("Consumer closed");
        }


    }
}
