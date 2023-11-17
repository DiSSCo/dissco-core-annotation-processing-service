package eu.dissco.annotationprocessingservice.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.UnsupportedOperationException;
import eu.dissco.annotationprocessingservice.repository.MasJobRecordRepository;
import java.util.List;
import java.util.UUID;
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

  public void markMasJobRecordAsComplete(UUID jobId, List<String> annotationIds) {
    var annotationNode = buildAnnotationNode(annotationIds);
    repository.markMasJobRecordAsComplete(jobId, annotationNode);
  }

  private JsonNode buildAnnotationNode(List<String> annotationIds) {
    var listNode = mapper.createArrayNode();
    for (var annotationId : annotationIds) {
      var annotationNode = mapper.createObjectNode();
      annotationNode.put("annotationId", annotationId);
      listNode.add(annotationNode);
    }
    return listNode;
  }

  public void markMasJobRecordAsFailed(UUID jobId) {
    repository.markMasJobRecordAsFailed(jobId);
  }

}
