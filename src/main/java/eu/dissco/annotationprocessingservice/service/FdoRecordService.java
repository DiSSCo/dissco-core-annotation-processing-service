package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.domain.FdoProfileAttributes.ANNOTATION_HASH;
import static eu.dissco.annotationprocessingservice.domain.FdoProfileAttributes.TARGET_PID;
import static eu.dissco.annotationprocessingservice.domain.FdoProfileAttributes.TARGET_TYPE;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class FdoRecordService {

  private static final String ATTRIBUTES = "attributes";
  private static final String TYPE = "type";
  private static final String DATA = "data";
  private static final String ID = "id";
  @Qualifier("objectMapper")
  private final ObjectMapper mapper;
  private final FdoProperties fdoProperties;

  public List<JsonNode> buildPostHandleRequest(List<AnnotationProcessingRequest> annotationRequest) {
    return annotationRequest.stream()
        .map(a -> buildSinglePostHandleRequest(a, null))
        .toList();
  }

  public List<JsonNode> buildPostHandleRequestHash(List<HashedAnnotationRequest> annotations) {
    return
        annotations.stream()
            .map(ha -> buildSinglePostHandleRequest(ha.annotation(), ha.hash()))
            .toList();
  }

  private JsonNode buildSinglePostHandleRequest(AnnotationProcessingRequest annotation,
      UUID annotationHash) {
    return mapper.createObjectNode()
        .set("data", mapper.createObjectNode()
            .put(TYPE, fdoProperties.getType())
            .set(ATTRIBUTES, generateAttributes(annotation.getOaHasTarget().getId(),
                annotation.getOaHasTarget().getOdsFdoType(),
                annotationHash)));
  }

  public List<JsonNode> buildPatchHandleRequest(Annotation annotation) {
    return List.of(buildSinglePatchHandleRequest(annotation, null));
  }

  public List<JsonNode> buildPatchHandleRequest(List<HashedAnnotation> annotations) {
    List<JsonNode> requestBody = new ArrayList<>();
    for (var annotation : annotations) {
      requestBody.add(
          buildSinglePatchHandleRequest(annotation.annotation(), annotation.hash()));
    }
    return requestBody;
  }

  private JsonNode buildSinglePatchHandleRequest(Annotation annotation,
      UUID annotationHash) {
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    var attributes = generateAttributes(annotation.getOaHasTarget().getId(),
        annotation.getOaHasTarget().getOdsFdoType(),
        annotationHash);
    data.put(TYPE, fdoProperties.getType());
    data.set(ATTRIBUTES, attributes);
    data.put(ID, annotation.getId());
    request.set(DATA, data);
    return request;
  }

  public JsonNode buildTombstoneHandleRequest(String id) {
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    var attributes = mapper.createObjectNode()
        .put("tombstoneText", "This annotation was archived");
    data.put(ID, id);
    data.set(ATTRIBUTES, attributes);
    request.set(DATA, data);
    return request;
  }

  private JsonNode generateAttributes(String targetPid, String targetType,
      UUID annotationHash) {
    var attributes = mapper.createObjectNode()
        .put(TARGET_PID.getAttribute(), targetPid)
        .put(TARGET_TYPE.getAttribute(), targetType);
    if (annotationHash != null) {
      attributes.put(ANNOTATION_HASH.getAttribute(), annotationHash.toString());
    }
    return attributes;
  }

  public boolean handleNeedsUpdate(
      Annotation currentAnnotation, Annotation newAnnotation) {
    return (
        !Objects.equals(currentAnnotation.getOaHasTarget().getId(),
            (newAnnotation.getOaHasTarget().getId())) ||
            !currentAnnotation.getOaMotivation().equals(newAnnotation.getOaMotivation()));
  }
}
