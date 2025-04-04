package eu.dissco.annotationprocessingservice.configuration;

import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.properties.KafkaConsumerProperties;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

@Profile({Profiles.KAFKA_MAS, Profiles.KAFKA_AUTO})
@Configuration
@AllArgsConstructor
public class KafkaConsumerConfiguration {

  private final KafkaConsumerProperties properties;

  @Bean
  public ConsumerFactory<String, String> consumerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getHost());
    props.put(
        ConsumerConfig.GROUP_ID_CONFIG, properties.getGroup());
    props.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class);
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, properties.getBatchSize());
    return new DefaultKafkaConsumerFactory<>(props);
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String>
  kafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, String> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setBatchListener(true);
    factory.setConsumerFactory(consumerFactory());
    return factory;
  }

}
