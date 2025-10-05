package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.logging.Logger;

public class ProducerDemoKeys {

    private static final Logger log = Logger.getLogger(ProducerDemoKeys.class.getName());

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

        for(int j=0;j<10;j++){
            // send data
            for(int i=0;i<10;i++){

                String topic = "demo_java";
                String key = "key "+i;
                String value = "Hello World "+i;

                //create producer record
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                // send data
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // execute every time a recod is successfully sent or an exception is thrown
                        if(e==null){
                            //the record wa successfully sent
                            log.info("key: " + key + " | Partition: " + recordMetadata.partition());
                        }else{
                            log.log(java.util.logging.Level.SEVERE, "error while producing", e);
                        }
                    }
                });
            }

            try{
                Thread.sleep(500);
            }catch (InterruptedException e){
                e.printStackTrace();
            }

        }







        // flush and close producer
        //tell producer to block until sending all data -- synchronous
        producer.flush();
        // btw .close() also use flush behind the scenes
        producer.close();

    }
}
