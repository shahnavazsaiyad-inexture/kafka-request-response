package com.inexture.config;

import com.inexture.dto.KafkaPartitions;
import com.inexture.dto.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.messaging.handler.annotation.Payload;
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
    public Message welcomeListener(ConsumerRecord<String, Message> record){
        System.out.println("Received in welcome "+ record.value());
        if(record.value() != null){
            try {
                Thread.sleep(15000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Message message = new Message();
            message.setId(record.value().getId());
            message.setContent("Welcome "+record.value().getContent());
            return  message;

        }
        return null;
    }

    @KafkaListener(topicPartitions = @TopicPartition(topic = "my-request-topic", partitions = {KafkaPartitions.WELCOME}))
    @SendTo("acknowledgement-topic")
    public Message acknowledge(ConsumerRecord<String, Message> record){
        System.out.println("Received in acknowledge "+ record.key());
        if(record.value() != null){
            Message message = new Message();
            message.setContent(record.key());
            return  message;
        }
        return null;
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
