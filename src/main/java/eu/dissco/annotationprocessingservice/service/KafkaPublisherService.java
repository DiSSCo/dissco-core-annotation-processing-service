package eu.dissco.annotationprocessingservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.diff.JsonDiff;
import eu.dissco.annotationprocessingservice.domain.AnnotationRecord;
import eu.dissco.annotationprocessingservice.domain.CreateUpdateDeleteEvent;
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

  public void publishCreateEvent(AnnotationRecord annotationRecord) {
    var event = new CreateUpdateDeleteEvent(UUID.randomUUID(),
        "create",
        "annotation-processing-service",
        annotationRecord.id(),
        SUBJECT_TYPE,
        Instant.now(),
        mapper.valueToTree(annotationRecord),
        null,
        "Annotation newly created");
    try {
      kafkaTemplate.send("createUpdateDeleteTopic", mapper.writeValueAsString(event));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public void publishUpdateEvent(AnnotationRecord currentAnnotationRecord,
      AnnotationRecord annotationRecord) {
    var jsonPatch = createJsonPatch(currentAnnotationRecord, annotationRecord);
    var event = new CreateUpdateDeleteEvent(UUID.randomUUID(),
        "update",
        "annotation-processing-service",
        annotationRecord.id(),
        SUBJECT_TYPE,
        Instant.now(),
        mapper.valueToTree(annotationRecord),
        jsonPatch,
        "Annotation has been updated");
    try {
      kafkaTemplate.send("createUpdateDeleteTopic", mapper.writeValueAsString(event));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private JsonNode createJsonPatch(AnnotationRecord currentAnnotationRecord,
      AnnotationRecord annotationRecord) {
    return JsonDiff.asJson(mapper.valueToTree(currentAnnotationRecord.annotation()),
        mapper.valueToTree(annotationRecord.annotation()));
  }
}
