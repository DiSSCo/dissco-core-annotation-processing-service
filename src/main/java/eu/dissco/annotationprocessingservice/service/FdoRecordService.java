package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.domain.FdoProfileAttributes.ACCESS_RESTRICTED;
import static eu.dissco.annotationprocessingservice.domain.FdoProfileAttributes.ANNOTATION_TOPIC;
import static eu.dissco.annotationprocessingservice.domain.FdoProfileAttributes.DIGITAL_OBJECT_TYPE;
import static eu.dissco.annotationprocessingservice.domain.FdoProfileAttributes.FDO_PROFILE;
import static eu.dissco.annotationprocessingservice.domain.FdoProfileAttributes.ISSUED_FOR_AGENT;
import static eu.dissco.annotationprocessingservice.domain.FdoProfileAttributes.LINKED_OBJECT_URL;
import static eu.dissco.annotationprocessingservice.domain.FdoProfileAttributes.REPLACE_OR_APPEND;
import static eu.dissco.annotationprocessingservice.domain.FdoProfileAttributes.SUBJECT_ID;
import static eu.dissco.annotationprocessingservice.domain.FdoProfileAttributes.TYPE;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import eu.dissco.annotationprocessingservice.domain.Annotation;
import eu.dissco.annotationprocessingservice.domain.AnnotationRecord;
import java.util.List;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class FdoRecordService {

  private final ObjectMapper mapper;

  public List<JsonNode> buildPostHandleRequest(Annotation annotation) {
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    var attributes = generateAttributes(annotation);
    data.put(TYPE.getAttribute(), TYPE.getDefaultValue());
    data.set("attributes", attributes);
    request.set("data", data);
    return List.of(request);
  }

  public List<JsonNode> buildPatchDeleteHandleRequest(AnnotationRecord annotation) {
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    var attributes = generateAttributes(annotation.annotation());
    data.put(TYPE.getAttribute(), TYPE.getDefaultValue());
    data.set("attributes", attributes);
    data.put("id", annotation.id());
    request.set("data", data);
    return List.of(request);
  }

  private JsonNode generateAttributes(Annotation annotation) {
    var attributes = mapper.createObjectNode();
    attributes.put(FDO_PROFILE.getAttribute(), FDO_PROFILE.getDefaultValue());
    attributes.put(DIGITAL_OBJECT_TYPE.getAttribute(), DIGITAL_OBJECT_TYPE.getDefaultValue());
    attributes.put(ISSUED_FOR_AGENT.getAttribute(), ISSUED_FOR_AGENT.getDefaultValue());
    var targetId = annotation.target().get("id");
    if (targetId != null) {
      attributes.put(SUBJECT_ID.getAttribute(), targetId.asText());
    } else {
      throw new IllegalStateException("Missing mandatory attribute for PID record: target id");
    }
    attributes.put(ANNOTATION_TOPIC.getAttribute(), annotation.motivation());
    attributes.put(REPLACE_OR_APPEND.getAttribute(), "append");
    attributes.put(ACCESS_RESTRICTED.getAttribute(), false);
    var generatorId = annotation.generator().get("id");
    if (generatorId != null) {
      attributes.put(LINKED_OBJECT_URL.getAttribute(), generatorId.asText());
    }
    return attributes;
  }

  public JsonNode buildRollbackCreationRequest(AnnotationRecord annotationRecord) {
    var dataNode = List.of(mapper.createObjectNode().put("id", annotationRecord.id()));
    ArrayNode dataArray = mapper.valueToTree(dataNode);
    return mapper.createObjectNode().set("data", dataArray);
  }

  public boolean handleNeedsUpdate(Annotation currentAnnotation, Annotation newAnnotation) {
    return (
        !Objects.equals(currentAnnotation.target().get("id"), (newAnnotation.target().get("id"))) ||
            !currentAnnotation.motivation().equals(newAnnotation.motivation()));
  }

}
