package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.domain.FdoProfileAttributes.ANNOTATION_HASH;
import static eu.dissco.annotationprocessingservice.domain.FdoProfileAttributes.ISSUED_FOR_AGENT;
import static eu.dissco.annotationprocessingservice.domain.FdoProfileAttributes.MOTIVATION;
import static eu.dissco.annotationprocessingservice.domain.FdoProfileAttributes.TARGET_PID;
import static eu.dissco.annotationprocessingservice.domain.FdoProfileAttributes.TARGET_TYPE;
import static eu.dissco.annotationprocessingservice.domain.FdoProfileAttributes.TYPE;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import eu.dissco.annotationprocessingservice.domain.HashedAnnotation;
import eu.dissco.annotationprocessingservice.domain.HashedAnnotationRequest;
import eu.dissco.annotationprocessingservice.properties.FdoProperties;
import eu.dissco.annotationprocessingservice.schema.Annotation;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class FdoRecordService {

  private static final String ATTRIBUTES = "attributes";
  private static final String DATA = "data";
  private static final String ID = "id";
  private final ObjectMapper mapper;
  private final FdoProperties fdoProperties;

  public List<JsonNode> buildPostHandleRequest(AnnotationProcessingRequest annotationRequest) {
    return List.of(buildSinglePostHandleRequest(annotationRequest, null));
  }

  public List<JsonNode> buildPostHandleRequest(List<HashedAnnotationRequest> annotations) {
    List<JsonNode> requestBody = new ArrayList<>();
    for (var annotation : annotations) {
      requestBody.add(buildSinglePostHandleRequest(annotation.annotation(), annotation.hash()));
    }
    return requestBody;
  }

  private JsonNode buildSinglePostHandleRequest(AnnotationProcessingRequest annotation,
      UUID annotationHash) {
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    var attributes = generateAttributes(annotation.getOaHasTarget().getId(),
        annotation.getOaHasTarget().getOdsType(), annotation.getOaMotivation().value(),
        annotationHash);
    data.put(TYPE.getAttribute(), fdoProperties.getType());
    data.set(ATTRIBUTES, attributes);
    request.set(DATA, data);
    return request;
  }

  public List<JsonNode> buildPatchRollbackHandleRequest(Annotation annotation) {
    return List.of(buildSinglePatchRollbackHandleRequest(annotation, null));
  }

  public List<JsonNode> buildPatchRollbackHandleRequest(List<HashedAnnotation> annotations) {
    List<JsonNode> requestBody = new ArrayList<>();
    for (var annotation : annotations) {
      requestBody.add(
          buildSinglePatchRollbackHandleRequest(annotation.annotation(), annotation.hash()));
    }
    return requestBody;
  }

  private JsonNode buildSinglePatchRollbackHandleRequest(Annotation annotation,
      UUID annotationHash) {
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    var attributes = generateAttributes(annotation.getOaHasTarget().getId(),
        annotation.getOaHasTarget().getOdsType(),
        annotation.getOaMotivation().value(), annotationHash);
    data.put(TYPE.getAttribute(), fdoProperties.getType());
    data.set(ATTRIBUTES, attributes);
    data.put(ID, annotation.getId());
    request.set(DATA, data);
    return request;
  }

  public JsonNode buildArchiveHandleRequest(String id) {
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    var attributes = mapper.createObjectNode();
    attributes.put("tombstoneText", "This annotation was archived");
    data.put(ID, id);
    data.set(ATTRIBUTES, attributes);
    request.set(DATA, data);
    return request;
  }

  private JsonNode generateAttributes(String targetPid, String targetType, String motivation,
      UUID annotationHash) {
    var attributes = mapper.createObjectNode();
    attributes.put(ISSUED_FOR_AGENT.getAttribute(), fdoProperties.getIssuedForAgent());
    attributes.put(TARGET_PID.getAttribute(), targetPid);
    attributes.put(TARGET_TYPE.getAttribute(), targetType);
    attributes.put(MOTIVATION.getAttribute(), motivation);
    if (annotationHash != null) {
      attributes.put(ANNOTATION_HASH.getAttribute(), annotationHash.toString());
    }
    return attributes;
  }

  public JsonNode buildRollbackCreationRequest(Annotation annotation) {
    var dataNode = List.of(mapper.createObjectNode().put(ID, annotation.getId()));
    ArrayNode dataArray = mapper.valueToTree(dataNode);
    return mapper.createObjectNode().set(DATA, dataArray);
  }

  public JsonNode buildRollbackCreationRequest(List<String> idList) {
    var dataNode = idList.stream()
        .map(handle -> mapper.createObjectNode().put("id", handle))
        .toList();
    ArrayNode dataArray = mapper.valueToTree(dataNode);
    return mapper.createObjectNode().set("data", dataArray);
  }

  public boolean handleNeedsUpdate(
      Annotation currentAnnotation, Annotation newAnnotation) {
    return (
        !Objects.equals(currentAnnotation.getOaHasTarget().getId(),
            (newAnnotation.getOaHasTarget().getId())) ||
            !currentAnnotation.getOaMotivation().equals(newAnnotation.getOaMotivation()));
  }

}
