package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.domain.FdoProfileAttributes.DIGITAL_OBJECT_TYPE;
import static eu.dissco.annotationprocessingservice.domain.FdoProfileAttributes.FDO_PROFILE;
import static eu.dissco.annotationprocessingservice.domain.FdoProfileAttributes.ISSUED_FOR_AGENT;
import static eu.dissco.annotationprocessingservice.domain.FdoProfileAttributes.TYPE;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import eu.dissco.annotationprocessingservice.domain.HashedAnnotation;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import java.util.ArrayList;
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
    return List.of(buildSinglePostHandleRequest(annotation));
  }

  public List<JsonNode> buildPostHandleRequest(List<HashedAnnotation> annotations) {
    List<JsonNode> requestBody = new ArrayList<>();
    for (var annotation : annotations) {
      requestBody.add(buildSinglePostHandleRequest(annotation.annotation()));
    }
    return requestBody;
  }

  private JsonNode buildSinglePostHandleRequest(Annotation annotation) {
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    var attributes = generateAttributes(annotation);
    data.put(TYPE.getAttribute(), "handle");
    data.set(ATTRIBUTES, attributes);
    request.set(DATA, data);
    return request;
  }

  public List<JsonNode> buildPatchRollbackHandleRequest(Annotation annotation) {
    return List.of(buildSinglePatchRollbackHandleRequest(annotation));
  }

  public List<JsonNode> buildPatchRollbackHandleRequest(List<Annotation> annotations) {
    List<JsonNode> requestBody = new ArrayList<>();
    for (var annotation : annotations) {
      requestBody.add(buildSinglePatchRollbackHandleRequest(annotation));
    }
    return requestBody;
  }

  private JsonNode buildSinglePatchRollbackHandleRequest(Annotation annotation) {
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    var attributes = generateAttributes(annotation);
    data.put(TYPE.getAttribute(), "handle");
    data.set(ATTRIBUTES, attributes);
    data.put(ID, annotation.getOdsId());
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

  private JsonNode generateAttributes(Annotation annotation) {
    var attributes = mapper.createObjectNode();
    attributes.put(FDO_PROFILE.getAttribute(), FDO_PROFILE.getDefaultValue());
    attributes.put(DIGITAL_OBJECT_TYPE.getAttribute(), DIGITAL_OBJECT_TYPE.getDefaultValue());
    attributes.put(ISSUED_FOR_AGENT.getAttribute(), ISSUED_FOR_AGENT.getDefaultValue());
    return attributes;
  }

  public JsonNode buildRollbackCreationRequest(Annotation annotation) {
    var dataNode = List.of(mapper.createObjectNode().put(ID, annotation.getOdsId()));
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
        !Objects.equals(currentAnnotation.getOaTarget().getOdsId(), (newAnnotation.getOaTarget().getOdsId())) ||
            !currentAnnotation.getOaMotivation().equals(newAnnotation.getOaMotivation()));
  }

}
