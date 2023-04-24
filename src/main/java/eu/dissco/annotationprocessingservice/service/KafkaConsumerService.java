package eu.dissco.annotationprocessingservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import javax.xml.transform.TransformerException;
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
  private final ProcessingService service;

  @KafkaListener(topics = "${kafka.consumer.topic}")
  public void getMessages(@Payload String message)
      throws JsonProcessingException, TransformerException, DataBaseException, FailedProcessingException {
    var event = mapper.readValue(message, AnnotationEvent.class);
    service.handleMessage(event);
  }

}
