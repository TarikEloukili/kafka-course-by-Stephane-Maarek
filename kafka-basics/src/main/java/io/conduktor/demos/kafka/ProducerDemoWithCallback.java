package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.logging.Logger;

public class ProducerDemoWithCallback {

    private static final Logger log = Logger.getLogger(ProducerDemoWithCallback.class.getName());

    public static void main(String[] args) {
        log.info("I am kafka producer");

        // create producer properties
        Properties properties = new Properties();

        //connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400");

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);



        // send data
        for(int j =0; j<10;j++){
            for(int i=0;i<30;i++){
                //create producer record
                ProducerRecord<String, String> record = new ProducerRecord<>("demo_java", "tarik" + i);

                // send data
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // execute every time a recod is successfully sent or an exception is thrown
                        if(e==null){
                            //the record wa successfully sent
                            log.info("Received new metadata \n" +
                                    "Topics: " + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "TimeStamp: " + recordMetadata.timestamp() + "\n");
                        }else{
                            log.log(java.util.logging.Level.SEVERE, "error while producing", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
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
