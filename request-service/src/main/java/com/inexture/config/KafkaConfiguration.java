package com.inexture.config;

import com.inexture.dto.Message;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.requestreply.AggregatingReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

@Configuration
@EnableKafka
public class KafkaConfiguration {

    @Autowired
    private Environment environment;

    @Bean
    public ConsumerFactory<String, Message> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("spring.kafka.bootstrap-servers"));
        config.put(ConsumerConfig.GROUP_ID_CONFIG, environment.getProperty("spring.kafka.consumer.group-id"));
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "com.inexture.dto");

        return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(), new JsonDeserializer<>());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Message> kafkaListenerContainerFactory(ConsumerFactory<String, Message> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, Message> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        return factory;
    }
    @Bean
    public ConcurrentMessageListenerContainer<String, Message> repliesContainer(ConcurrentKafkaListenerContainerFactory<String, Message> containerFactory) {
        ConcurrentMessageListenerContainer<String, Message> repliesContainer = containerFactory.createContainer("my-reply-topic", "acknowledgement-topic");
        repliesContainer.getContainerProperties().setGroupId("replies-group");
        repliesContainer.setAutoStartup(false);
        return repliesContainer;
    }

    @Bean
    public ProducerFactory<String, Message> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("spring.kafka.bootstrap-servers"));
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, Message> kafkaTemplate(ProducerFactory<String, Message> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public AcknowledgeReplyingKafkaTemplate<String, Message, Message> replyingKafkaTemplate(ProducerFactory<String, Message> producerFactory,
                                                                                 ConcurrentMessageListenerContainer<String, Message> replyContainer) {
        AcknowledgeReplyingKafkaTemplate<String, Message, Message> kafkaTemplate = new AcknowledgeReplyingKafkaTemplate<>(producerFactory, replyContainer);
        kafkaTemplate.setDefaultReplyTimeout(Duration.ofSeconds(20));
        return kafkaTemplate;
    }

    @Bean
    public NewTopic myRequestTopic() {
        return TopicBuilder.name("my-request-topic")
                .partitions(2)
                .replicas(1)
                .build();
    }

}
