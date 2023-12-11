package com.inexture.config;

import com.inexture.dto.KafkaPartitions;
import com.inexture.dto.Message;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.messaging.handler.annotation.SendTo;

@Configuration
@EnableKafka
public class KafkaConfiguration {

//    @KafkaListener(id = "reply-listener", topics = "my-request-topic")
//    @SendTo("my-reply-topic2")
//    public Message listenAndReply(Message in){
//
//        Message message = new Message();
//        message.setContent(in.getContent());
//        return  message;
//    }
//
//    @KafkaListener(topics = "my-request-topic2")
//    @SendTo("my-reply-topic1")
//    public Message welcomeListener(Message in){
//        System.out.println("Received in welcome");
//        Message message = new Message();
//        message.setContent("Welcome "+in.getContent());
//        return  message;
//    }

    @KafkaListener(topicPartitions = @TopicPartition(topic = "my-request-topic", partitions = {KafkaPartitions.WELCOME}))
    @SendTo("my-reply-topic")
    public Message welcomeListener(Message in){
        System.out.println("Received in welcome");
        Message message = new Message();
        message.setId(in.getId());
        message.setContent("Welcome "+in.getContent());
        return  message;
    }


    @KafkaListener(topicPartitions = @TopicPartition(topic = "my-request-topic", partitions = {KafkaPartitions.GREET}))
    @SendTo("my-reply-topic")
    public Message greetListener(Message in){
        System.out.println("Received in Greet");
        Message message = new Message();
        message.setId(in.getId());
        message.setContent("Hello "+in.getContent());
        return  message;
    }
}
