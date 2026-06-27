package com.springboot.kafka_wikimedia_producer;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.HttpConnectStrategy;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;

@Service
public class WikimediaChangesProducer {

    @Value("${spring.kafka.topic.name}")
    private String topicName;

    private static final Logger logger = LoggerFactory.getLogger(WikimediaChangesProducer.class);

    private KafkaTemplate<String,String> kafkaTemplate;

    public WikimediaChangesProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /*public void sendMessage() throws InterruptedException {
        String topic = "Wikimedia_recentchange";

        //To read real time stream data from Wikimedia , we use event source
        //BackgroundEventHandler eventHandler =new WikimediaChangesHandler(kafkaTemplate,topic);
       // String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        //EventSource.Builder builder = new EventSource.Builder(eventHandler,url);


        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        BackgroundEventHandler eventHandler = new WikimediaChangesHandler(kafkaTemplate, topic);
        BackgroundEventSource.Builder builder = new BackgroundEventSource.Builder(eventHandler,new EventSource.Builder(URI.create(url)));
        BackgroundEventSource eventSource = builder.build();
        eventSource.start();

        Thread.sleep(10 * 60 * 1000); // 10 minutes
    }*/

    public void sendMessage() throws InterruptedException {
        //String topic = "Wikimedia_recentchange";
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        BackgroundEventHandler eventHandler = new WikimediaChangesHandler(kafkaTemplate, topicName);

        EventSource.Builder eventSourceBuilder = new EventSource.Builder(
                HttpConnectStrategy.http(URI.create(url))
                        .header("User-Agent", "kafka-wikimedia-producer/1.0 (your-email@example.com)")
        );

        BackgroundEventSource.Builder builder = new BackgroundEventSource.Builder(eventHandler, eventSourceBuilder);
        BackgroundEventSource eventSource = builder.build();
        eventSource.start();

        Thread.sleep(10 * 60 * 1000);
    }
}
