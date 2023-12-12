package com.inexture.service;

import com.inexture.config.AcknowledgeReplyingKafkaTemplate;
import com.inexture.dto.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.requestreply.*;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class KafkaClientService {

    @Autowired
    private AcknowledgeReplyingKafkaTemplate<String, Message, Message> acknowledgeReplyingKafkaTemplate;

    int key = 1;

    public Message sendAndReceiveMessage(String content, Integer partition) {
        Message request = new Message();
        request.setId(++key);
        request.setContent(content);

        String uuid = UUID.randomUUID().toString();
        try {
            RequestReplyFuture<String, Message, Message> requestReplyFuture = acknowledgeReplyingKafkaTemplate.sendAndReceive(new ProducerRecord<>("my-request-topic", partition, uuid,  request), uuid);

            SendResult<String, Message> sendFuture = requestReplyFuture.getSendFuture().get(10, TimeUnit.SECONDS);

            acknowledgeReplyingKafkaTemplate.isAcknowledgementReceived(uuid);
            ConsumerRecord<String, Message> consumerRecord = requestReplyFuture.get(20, TimeUnit.SECONDS);
            return consumerRecord.value();

        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error sending and receiving message", e);
        } catch (TimeoutException e) {
            acknowledgeReplyingKafkaTemplate.send("my-request-topic",partition, uuid, null);
            throw new KafkaReplyTimeoutException("Consumer is not available");
        }
    }
}
