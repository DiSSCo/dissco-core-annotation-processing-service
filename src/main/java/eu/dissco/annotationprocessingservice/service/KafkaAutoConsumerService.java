package eu.dissco.annotationprocessingservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.domain.AutoAcceptedAnnotation;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import java.io.IOException;
import lombok.AllArgsConstructor;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Profile(Profiles.KAFKA_AUTO)
@AllArgsConstructor
public class KafkaAutoConsumerService {

  private final ObjectMapper mapper;
  private final ProcessingAutoAcceptedService autoAcceptedService;


  @KafkaListener(topics = "${kafka.consumer.topic}")
  public void getAutoAcceptedMessages(@Payload String message)
      throws IOException, DataBaseException, FailedProcessingException {
    var event = mapper.readValue(message, AutoAcceptedAnnotation.class);
    autoAcceptedService.handleMessage(event);
  }

}
