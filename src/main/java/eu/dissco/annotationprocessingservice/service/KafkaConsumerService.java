package eu.dissco.annotationprocessingservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import eu.dissco.annotationprocessingservice.exception.BatchingException;
import eu.dissco.annotationprocessingservice.exception.ConflictException;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@Profile(Profiles.KAFKA)
@AllArgsConstructor
public class KafkaConsumerService {

  private final ObjectMapper mapper;
  private final ProcessingKafkaService service;

  @KafkaListener(topics = "${kafka.consumer.topic}")
  public void getMessages(@Payload String message)
      throws IOException, DataBaseException, FailedProcessingException, AnnotationValidationException, ConflictException, BatchingException {
    var event = mapper.readValue(message, AnnotationEvent.class);
    service.handleMessage(event);
  }

}
