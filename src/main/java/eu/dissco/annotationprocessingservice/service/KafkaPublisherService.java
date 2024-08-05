package eu.dissco.annotationprocessingservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.domain.ProcessedAnnotationBatch;
import eu.dissco.annotationprocessingservice.properties.KafkaConsumerProperties;
import eu.dissco.annotationprocessingservice.schema.Annotation;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaPublisherService {

  private final ObjectMapper mapper;
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final KafkaConsumerProperties consumerProperties;
  private final ProvenanceService provenanceService;

  public void publishCreateEvent(Annotation annotation) throws JsonProcessingException {
    var event = provenanceService.generateCreateEvent(annotation);
    kafkaTemplate.send("createUpdateDeleteTopic", mapper.writeValueAsString(event));
  }

  public void publishUpdateEvent(Annotation currentAnnotation, Annotation annotation)
      throws JsonProcessingException {
    var event = provenanceService.generateUpdateEvent(annotation, currentAnnotation);
    kafkaTemplate.send("createUpdateDeleteTopic", mapper.writeValueAsString(event));
  }

  public void publishBatchAnnotation(ProcessedAnnotationBatch annotationEvent)
      throws JsonProcessingException {
      kafkaTemplate.send(consumerProperties.getTopic(), mapper.writeValueAsString(annotationEvent));
  }
}
