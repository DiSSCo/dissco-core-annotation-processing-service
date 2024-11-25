package eu.dissco.annotationprocessingservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import eu.dissco.annotationprocessingservice.exception.BatchingException;
import eu.dissco.annotationprocessingservice.exception.ConflictException;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingEvent;
import java.io.IOException;
import lombok.AllArgsConstructor;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Profile(Profiles.KAFKA_MAS)
@AllArgsConstructor
public class KafkaMasConsumerService {

  private final ObjectMapper mapper;
  private final ProcessingKafkaService service;

  @KafkaListener(topics = "${kafka.consumer.topic}")
  public void getMessages(@Payload String message)
      throws IOException, DataBaseException, FailedProcessingException, AnnotationValidationException, ConflictException, BatchingException {
    var event = mapper.readValue(message, AnnotationProcessingEvent.class);
    service.handleMessage(event);
  }

}
