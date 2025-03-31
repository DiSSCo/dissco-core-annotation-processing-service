package eu.dissco.annotationprocessingservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.domain.AutoAcceptedAnnotation;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Profile(Profiles.KAFKA_AUTO)
@AllArgsConstructor
@Slf4j
public class KafkaAutoConsumerService {

  private final ObjectMapper mapper;
  private final ProcessingAutoAcceptedService autoAcceptedService;


  @KafkaListener(topics = "${kafka.consumer.topic}")
  public void getAutoAcceptedMessages(@Payload List<String> messages)
      throws DataBaseException, FailedProcessingException {
    var events = messages.stream().map(message -> {
      try {
        return mapper.readValue(message, AutoAcceptedAnnotation.class);
      } catch (JsonProcessingException e) {
        log.error("Failed to parse event message", e);
        return null;
      }
    }).filter(Objects::nonNull).toList();
    autoAcceptedService.handleMessage(events);
  }

}
