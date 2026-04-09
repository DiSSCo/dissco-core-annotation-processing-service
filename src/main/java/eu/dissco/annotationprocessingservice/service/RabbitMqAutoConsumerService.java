package eu.dissco.annotationprocessingservice.service;

import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.domain.AutoAcceptedAnnotation;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

@Service
@Profile(Profiles.RABBIT_MQ_AUTO)
@AllArgsConstructor
@Slf4j
public class RabbitMqAutoConsumerService {

  private final JsonMapper mapper;
  private final ProcessingAutoAcceptedService autoAcceptedService;

  @RabbitListener(queues = "${rabbitmq.auto-accepted-annotation.queue-name:auto-accepted-annotation-queue}",
      containerFactory = "consumerBatchContainerFactory")
  public void getAutoAcceptedMessages(@Payload List<String> messages)
      throws DataBaseException, FailedProcessingException {
    var events = messages.stream()
        .map(message -> mapper.readValue(message, AutoAcceptedAnnotation.class))
        .filter(Objects::nonNull).collect(Collectors.toSet());
    autoAcceptedService.handleMessage(events);
  }

}
