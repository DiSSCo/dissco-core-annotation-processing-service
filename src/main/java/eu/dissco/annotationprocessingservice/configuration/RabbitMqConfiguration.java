package eu.dissco.annotationprocessingservice.configuration;

import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.component.MessageCompressionComponent;
import eu.dissco.annotationprocessingservice.properties.RabbitMqProperties;
import lombok.AllArgsConstructor;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile(Profiles.RABBIT_MQ_AUTO)
@AllArgsConstructor
public class RabbitMqConfiguration {

  private final MessageCompressionComponent compressedMessageConverter;
  private final RabbitMqProperties rabbitMqProperties;

  @Bean
  public SimpleRabbitListenerContainerFactory consumerBatchContainerFactory(
      ConnectionFactory connectionFactory) {
    SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
    factory.setConnectionFactory(connectionFactory);
    factory.setBatchListener(true);
    factory.setBatchSize(rabbitMqProperties.getBatchSize());
    factory.setConsumerBatchEnabled(true);
    factory.setMessageConverter(compressedMessageConverter);
    return factory;
  }

  @Bean(name = "batchTemplate")
  public RabbitTemplate compressedTemplate(ConnectionFactory connectionFactory,
      MessageCompressionComponent compressedMessageConverter) {
    var rabbitTemplate = new RabbitTemplate(connectionFactory);
    rabbitTemplate.setMessageConverter(compressedMessageConverter);
    return rabbitTemplate;
  }
}
