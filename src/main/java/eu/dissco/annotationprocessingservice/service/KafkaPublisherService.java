package eu.dissco.annotationprocessingservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.diff.JsonDiff;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.domain.CreateUpdateDeleteEvent;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.properties.KafkaConsumerProperties;
import java.time.Instant;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaPublisherService {

  private static final String SUBJECT_TYPE = "Annotation";

  private final ObjectMapper mapper;
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final KafkaConsumerProperties consumerProperties;

  public void publishCreateEvent(Annotation annotation) throws JsonProcessingException {
    var event = new CreateUpdateDeleteEvent(UUID.randomUUID(),
        "create",
        "annotation-processing-service",
        annotation.getOdsId(),
        SUBJECT_TYPE,
        Instant.now(),
        mapper.valueToTree(annotation),
        null,
        "Annotation newly created");
    kafkaTemplate.send("createUpdateDeleteTopic", mapper.writeValueAsString(event));
  }

  public void publishUpdateEvent(Annotation currentAnnotation, Annotation annotation)
      throws JsonProcessingException {
    var jsonPatch = createJsonPatch(currentAnnotation, annotation);
    var event = new CreateUpdateDeleteEvent(UUID.randomUUID(),
        "update",
        "annotation-processing-service",
        annotation.getOdsId(),
        SUBJECT_TYPE,
        Instant.now(),
        mapper.valueToTree(annotation), jsonPatch, "Annotation has been updated");
    kafkaTemplate.send("createUpdateDeleteTopic", mapper.writeValueAsString(event));
  }

  public void publishBatchAnnotation(AnnotationEvent annotationEvent)
      throws JsonProcessingException {
    kafkaTemplate.send(consumerProperties.getTopic(), mapper.writeValueAsString(annotationEvent));
  }

  private JsonNode createJsonPatch(Annotation currentAnnotation, Annotation annotation) {
    return JsonDiff.asJson(mapper.valueToTree(currentAnnotation), mapper.valueToTree(annotation));
  }
}
