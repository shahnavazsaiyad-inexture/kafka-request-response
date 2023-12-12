package com.inexture.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.BatchConsumerAwareMessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.GenericMessageListenerContainer;
import org.springframework.kafka.requestreply.CorrelationKey;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.util.Assert;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiPredicate;

public class AcknowledgeReplyingKafkaTemplate<K, V, R> extends ReplyingKafkaTemplate<K, V, R> {

    private Map<String, RequestReplyFuture<K, V, R>> acknowledgementFutures = new HashMap<>();

    public AcknowledgeReplyingKafkaTemplate(ProducerFactory<K, V> producerFactory, GenericMessageListenerContainer<K, R> replyContainer) {
        super(producerFactory, replyContainer);
    }

    @Override
    public void onMessage(List<ConsumerRecord<K, R>> data) {
        List<ConsumerRecord<K,R>> completed = new ArrayList<>();
        data.forEach(consumerRecord -> {
            if(consumerRecord.topic().equals("acknowledgement-topic")){
                R value = consumerRecord.value();
                String key = value.toString();
                System.out.println(key);
                acknowledgementFutures.get(key).complete(consumerRecord);
            }
            else{
                completed.add(consumerRecord);
            }
        });
        if(!completed.isEmpty()){
            super.onMessage(data);
        }
    }

    public RequestReplyFuture<K, V, R> sendAndReceive(ProducerRecord<K, V> record, String uuid) {
        RequestReplyFuture<K, V, R> requestReplyFuture = super.sendAndReceive(record);

        RequestReplyFuture<K, V, R> acknowledgementFuture = new RequestReplyFuture<>();
        acknowledgementFutures.put(uuid, acknowledgementFuture);
        return requestReplyFuture;
    }

    public boolean isAcknowledgementReceived(String uuid) throws ExecutionException, InterruptedException, TimeoutException {
        acknowledgementFutures.get(uuid).get(5, TimeUnit.SECONDS);
        return  true;
    }
}
