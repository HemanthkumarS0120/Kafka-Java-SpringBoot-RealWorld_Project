package com.springboot.kafka_wikimedia_producer;


import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

public class WikimediaChangesHandler implements BackgroundEventHandler {

    private static Logger logger = LoggerFactory.getLogger(WikimediaChangesHandler.class);

    private KafkaTemplate<String,String> kafkaTemplate;
    private String topic;

    public WikimediaChangesHandler(KafkaTemplate<String, String> kafkaTemplate, String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {
        logger.info("Connection opened");
    }

    @Override
    public void onClosed() throws Exception {
        logger.info("Connection closed");
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
            logger.info(String.format("Event Data -> %s",messageEvent.getData()));
            kafkaTemplate.send(topic,messageEvent.getData());
    }

    @Override
    public void onComment(String s) throws Exception {

    }

    @Override
    public void onError(Throwable throwable) {
        logger.error("Error in stream", throwable);
    }
}
