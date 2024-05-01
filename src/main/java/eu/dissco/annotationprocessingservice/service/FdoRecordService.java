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
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class FdoRecordService {

  private final ObjectMapper mapper;

  private static final String ATTRIBUTES = "attributes";
  private static final String DATA = "data";
  private static final String ID = "id";
  private static final String FDO_TYPE = "https://hdl.handle.net/21.T11148/cf458ca9ee1d44a5608f";
  private static final String ISSUED_FOR_AGENT_ROR = "https://ror.org/0566bfb96";

  public List<JsonNode> buildPostHandleRequest(Annotation annotation) {
    return List.of(buildSinglePostHandleRequest(annotation, null));
  }

  public List<JsonNode> buildPostHandleRequest(List<HashedAnnotation> annotations) {
    List<JsonNode> requestBody = new ArrayList<>();
    for (var annotation : annotations) {
      requestBody.add(buildSinglePostHandleRequest(annotation.annotation(), annotation.hash()));
    }
    return requestBody;
  }

  private JsonNode buildSinglePostHandleRequest(Annotation annotation, UUID annotationHash) {
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    var attributes = generateAttributes(annotation, annotationHash);
    data.put(TYPE.getAttribute(), FDO_TYPE);
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
      requestBody.add(buildSinglePatchRollbackHandleRequest(annotation.annotation(), annotation.hash()));
    }
    return requestBody;
  }

  private JsonNode buildSinglePatchRollbackHandleRequest(Annotation annotation, UUID annotationHash) {
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    var attributes = generateAttributes(annotation, annotationHash);
    data.put(TYPE.getAttribute(), FDO_TYPE);
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

  private JsonNode generateAttributes(Annotation annotation, UUID annotationHash) {
    var attributes = mapper.createObjectNode();
    attributes.put(ISSUED_FOR_AGENT.getAttribute(), ISSUED_FOR_AGENT_ROR);
    attributes.put(TARGET_PID.getAttribute(), annotation.getOaTarget().getOdsId());
    attributes.put(TARGET_TYPE.getAttribute(), annotation.getOaTarget().getOdsType().toString());
    attributes.put(MOTIVATION.getAttribute(), annotation.getOaMotivation().toString());
    if (annotationHash!= null) {
      attributes.put(ANNOTATION_HASH.getAttribute(), annotationHash.toString());
    }
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
