package eu.dissco.annotationprocessingservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.domain.AutoAcceptedAnnotation;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import java.util.List;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Profile(Profiles.RABBITMQ_AUTO)
@AllArgsConstructor
@Slf4j
public class RabbitMqConsumerService {

  private final ObjectMapper mapper;
  private final ProcessingAutoAcceptedService autoAcceptedService;
  private final RabbitMqPublisherService publisherService;

  @RabbitListener(queues = "${rabbitmq.auto-accepted-annotation.queue-name:auto-accepted-annotation-queue}",
      containerFactory = "consumerBatchContainerFactory")
  public void getAutoAcceptedMessages(@Payload List<String> messages)
      throws DataBaseException, FailedProcessingException {
    var events = messages.stream().map(message -> {
      try {
        return mapper.readValue(message, AutoAcceptedAnnotation.class);
      } catch (JsonProcessingException e) {
        log.error("Failed to parse event message", e);
        publisherService.deadLetterRaw(message);
        return null;
      }
    }).filter(Objects::nonNull).toList();
    autoAcceptedService.handleMessage(events);
  }

}
