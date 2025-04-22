package eu.dissco.annotationprocessingservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.domain.ProcessedAnnotationBatch;
import eu.dissco.annotationprocessingservice.properties.KafkaConsumerProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaPublisherService {

  private final ObjectMapper mapper;
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final KafkaConsumerProperties consumerProperties;


  public void publishBatchAnnotation(ProcessedAnnotationBatch annotationEvent)
      throws JsonProcessingException {
    kafkaTemplate.send(consumerProperties.getTopic(), mapper.writeValueAsString(annotationEvent));
  }

}
