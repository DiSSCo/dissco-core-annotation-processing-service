package eu.dissco.annotationprocessingservice.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.UnsupportedOperationException;
import eu.dissco.annotationprocessingservice.repository.MasJobRecordRepository;
import java.lang.reflect.Array;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class MasJobRecordService {

  private final MasJobRecordRepository repository;
  private final Environment environment;
  private final ObjectMapper mapper;

  public void verifyMasJobId(AnnotationEvent event) throws FailedProcessingException {
    if (environment.matchesProfiles(Profiles.KAFKA) && event.jobId() == null) {
      log.error("Missing MAS Job ID for event {}", event);
      throw new FailedProcessingException();
    }
    if (!environment.matchesProfiles(Profiles.KAFKA)){
      throw new UnsupportedOperationException();
    }
  }

  public void markMasJobRecordAsComplete(String jobId, List<String> annotationIds,
      boolean isBatchResult) {
    var newAnnotationNode = buildAnnotationNode(annotationIds);
    if (!isBatchResult){
      repository.markMasJobRecordAsComplete(jobId, newAnnotationNode);
    } else {
      var existingAnnotations = repository.getMasJobRecordAnnotations(jobId);
      if (existingAnnotations.isArray()){
        var annotationArray = (ArrayNode) existingAnnotations;
        annotationArray.addAll(newAnnotationNode);
        repository.markMasJobRecordAsComplete(jobId, annotationArray);
      } else {
        log.warn("Unexpected result from mas job record: {}", existingAnnotations);
      }
    }
  }

  public void markEmptyMasJobRecordAsComplete(String jobId, boolean isBatchResult){
    if (!isBatchResult){
      repository.markMasJobRecordAsComplete(jobId, mapper.createObjectNode());
    }
  }

  private ArrayNode buildAnnotationNode(List<String> annotationIds) {
    var listNode = mapper.createArrayNode();
    annotationIds.forEach(listNode::add);
    return listNode;
  }

  public void markMasJobRecordAsFailed(String jobId, boolean isBatchResult) {
    if (!isBatchResult){
      repository.markMasJobRecordAsFailed(jobId);
    }
  }

  public boolean getBatchingRequest(String jobId){
    return repository.getBatchingRequested(jobId);
  }

}
