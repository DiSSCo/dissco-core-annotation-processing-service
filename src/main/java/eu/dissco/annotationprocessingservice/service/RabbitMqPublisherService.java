package eu.dissco.annotationprocessingservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.properties.RabbitMqProperties;
import eu.dissco.annotationprocessingservice.schema.Annotation;
import eu.dissco.annotationprocessingservice.schema.CreateUpdateTombstoneEvent;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@AllArgsConstructor
public class RabbitMqPublisherService {

  private final ObjectMapper mapper;
  private final RabbitTemplate rabbitTemplate;
  private final RabbitMqProperties rabbitMqProperties;
  private final ProvenanceService provenanceService;

  public void publishCreateEvent(Annotation annotation) throws JsonProcessingException {
    var event = provenanceService.generateCreateEvent(annotation);
    publishMessage(event);
  }

  public void publishUpdateEvent(Annotation currentAnnotation, Annotation annotation)
      throws JsonProcessingException {
    var event = provenanceService.generateUpdateEvent(annotation, currentAnnotation);
    publishMessage(event);
  }

  public void publishTombstoneEvent(Annotation tombstoneAnnotation, Annotation currentAnnotation)
      throws JsonProcessingException {
    var event = provenanceService.generateTombstoneEvent(tombstoneAnnotation, currentAnnotation);
    publishMessage(event);
  }

  private void publishMessage(CreateUpdateTombstoneEvent event) throws JsonProcessingException {
    rabbitTemplate.convertSendAndReceive(
        rabbitMqProperties.getCreateUpdateTombstone().getExchangeName(),
        rabbitMqProperties.getCreateUpdateTombstone().getRoutingKeyName(),
        mapper.writeValueAsString(event));
  }

  public void deadLetterRaw(String message) {
    rabbitTemplate.convertSendAndReceive(
        rabbitMqProperties.getAutoAcceptedAnnotation().getDlqExchangeName(),
        rabbitMqProperties.getAutoAcceptedAnnotation().getDlqRoutingKeyName(), message);
  }
}
