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

  private static final String ATTRIBUTES = "attributes";
  private static final String DATA = "data";
  private static final String ID = "id";

  public List<JsonNode> buildPostHandleRequest(Annotation annotation) {
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    var attributes = generateAttributes(annotation);
    data.put(TYPE.getAttribute(), TYPE.getDefaultValue());
    data.set(ATTRIBUTES, attributes);
    request.set(DATA, data);
    return List.of(request);
  }

  public List<JsonNode> buildPatchRollbackHandleRequest(Annotation annotation, String handle) {
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    var attributes = generateAttributes(annotation);
    data.put(TYPE.getAttribute(), TYPE.getDefaultValue());
    data.set(ATTRIBUTES, attributes);
    data.put(ID, handle);
    request.set(DATA, data);
    return List.of(request);
  }

  public List<JsonNode> buildArchiveHandleRequest(String id){
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    var attributes = mapper.createObjectNode();
    attributes.put("tombstoneText", "This annotation was archived");
    data.put(ID, id);
    data.set(ATTRIBUTES, attributes);
    request.set(DATA, data);
    return List.of(request);
  }

  private JsonNode generateAttributes(Annotation annotation) {
    var attributes = mapper.createObjectNode();
    attributes.put(FDO_PROFILE.getAttribute(), FDO_PROFILE.getDefaultValue());
    attributes.put(DIGITAL_OBJECT_TYPE.getAttribute(), DIGITAL_OBJECT_TYPE.getDefaultValue());
    attributes.put(ISSUED_FOR_AGENT.getAttribute(), ISSUED_FOR_AGENT.getDefaultValue());
    var targetId = annotation.target().get(ID);
    if (targetId != null) {
      attributes.put(SUBJECT_ID.getAttribute(), targetId.asText());
    } else {
      throw new IllegalStateException("Missing mandatory attribute for PID record: target id");
    }
    attributes.put(ANNOTATION_TOPIC.getAttribute(), annotation.motivation());
    attributes.put(REPLACE_OR_APPEND.getAttribute(), "append");
    attributes.put(ACCESS_RESTRICTED.getAttribute(), false);
    var generatorId = annotation.generator().get(ID);
    if (generatorId != null) {
      attributes.put(LINKED_OBJECT_URL.getAttribute(), generatorId.asText());
    }
    return attributes;
  }

  public JsonNode buildRollbackCreationRequest(AnnotationRecord annotationRecord) {
    var dataNode = List.of(mapper.createObjectNode().put(ID, annotationRecord.id()));
    ArrayNode dataArray = mapper.valueToTree(dataNode);
    return mapper.createObjectNode().set(DATA, dataArray);
  }

  public boolean handleNeedsUpdate(Annotation currentAnnotation, Annotation newAnnotation) {
    return (
        !Objects.equals(currentAnnotation.target().get(ID), (newAnnotation.target().get(ID))) ||
            !currentAnnotation.motivation().equals(newAnnotation.motivation()));
  }

}
