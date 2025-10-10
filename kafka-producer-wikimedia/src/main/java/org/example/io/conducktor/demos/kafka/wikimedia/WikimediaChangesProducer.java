package org.example.io.conducktor.demos.kafka.wikimedia;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.launchdarkly.eventsource.EventSource;
import okhttp3.Headers;
import java.net.URI;
import java.time.Duration;

public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {

        String bootstrapServer = "127.0.0.1:9092";

        // create producer properties
        Properties properties = new Properties();

        // connect to localhost
        properties.setProperty("bootstrap.servers", bootstrapServer);

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchange";

        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        Headers headers = new Headers.Builder()
                .add("User-Agent", "Tarik-DE-Pipeline/1.0 (tarik@quantalgo.org)")
                .add("Accept", "text/event-stream")
                .build();


        EventSource eventSource = new EventSource.Builder(
                new WikimediaChangeHandler(producer, topic),
                URI.create(url))
                .headers(headers)
                .reconnectTime(Duration.ofSeconds(3))   // polite reconnect/backoff
                .build();



        // start the producer in another thread:
        eventSource.start();

        // we produce for 10-min and block program until then
        TimeUnit.MINUTES.sleep(10);

    }
}
