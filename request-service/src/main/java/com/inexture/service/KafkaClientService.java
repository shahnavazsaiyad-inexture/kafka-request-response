package com.inexture.service;

import com.inexture.dto.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.requestreply.ReplyingKafkaOperations;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Service
public class KafkaClientService {

    @Autowired
    private ReplyingKafkaTemplate<String, Message, Message> replyingKafkaTemplate;

    int key = 1;

    public Message sendAndReceiveMessage(String content, Integer partition) {
        Message request = new Message();
        request.setId(++key);
        request.setContent(content);

        try {
            RequestReplyFuture<String, Message, Message> requestReplyFuture = replyingKafkaTemplate.sendAndReceive(new ProducerRecord<>("my-request-topic", partition, UUID.randomUUID().toString(),  request));

            ConsumerRecord<String, Message> consumerRecord = requestReplyFuture.get();
            return consumerRecord.value();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error sending and receiving message", e);
        }
    }
}
