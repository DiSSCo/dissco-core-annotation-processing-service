package eu.dissco.annotationprocessingservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.database.jooq.enums.ErrorCode;
import eu.dissco.annotationprocessingservice.domain.MasJobRecord;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.UnsupportedOperationException;
import eu.dissco.annotationprocessingservice.repository.MasJobRecordRepository;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingEvent;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class MasJobRecordService {

  private final MasJobRecordRepository repository;
  private final Environment environment;
  @Qualifier("objectMapper")
  private final ObjectMapper mapper;

  public void verifyMasJobId(AnnotationProcessingEvent event) throws FailedProcessingException {
    if (environment.matchesProfiles(Profiles.RABBIT_MQ_MAS, Profiles.RABBIT_MQ_AUTO)
        && event.getJobId() == null) {
      log.error("Missing MAS Job ID for event {}", event);
      throw new FailedProcessingException();
    }
    if (!environment.matchesProfiles(Profiles.RABBIT_MQ_MAS, Profiles.RABBIT_MQ_AUTO)) {
      throw new UnsupportedOperationException();
    }
  }

  public void markMasJobRecordAsComplete(String jobId, List<String> annotationIds,
      boolean isBatchResult) {
    if (isBatchResult) {
      return;
    }
    var newAnnotationNode = buildAnnotationNode(annotationIds);
    repository.markMasJobRecordAsComplete(jobId, newAnnotationNode);
  }

  public void markEmptyMasJobRecordAsComplete(String jobId, boolean isBatchResult) {
    if (!isBatchResult) {
      repository.markMasJobRecordAsComplete(jobId, mapper.createObjectNode());
    }
  }

  private ArrayNode buildAnnotationNode(List<String> annotationIds) {
    var listNode = mapper.createArrayNode();
    annotationIds.forEach(listNode::add);
    return listNode;
  }

  public void markMasJobRecordAsFailed(String jobId, boolean isBatchResult, ErrorCode errorCode,
      String errorMessage) {
    if (!isBatchResult) {
      repository.markMasJobRecordAsFailed(jobId, errorCode, errorMessage);
    }
  }

  public MasJobRecord getMasJobRecord(String jobId) {
    return repository.getMasJobRecord(jobId);
  }

}
