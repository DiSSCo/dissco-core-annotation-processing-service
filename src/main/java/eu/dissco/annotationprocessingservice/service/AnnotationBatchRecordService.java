package eu.dissco.annotationprocessingservice.service;

import eu.dissco.annotationprocessingservice.domain.AnnotationBatchRecord;
import eu.dissco.annotationprocessingservice.domain.AnnotationProcessingEvent;
import eu.dissco.annotationprocessingservice.repository.AnnotationBatchRecordRepository;
import eu.dissco.annotationprocessingservice.schema.Annotation;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class AnnotationBatchRecordService {

  private final AnnotationBatchRecordRepository repository;

  public Optional<Map<String, UUID>> mintBatchIds(List<Annotation> newAnnotations,
      boolean batchingRequested, AnnotationProcessingEvent event) {
    Optional<Map<String, UUID>> batchIds;
    if (batchingRequested && event.batchId() == null) {
      batchIds = Optional.of(newAnnotations.stream().collect(Collectors.toMap(
          Annotation::getId,
          value -> UUID.randomUUID()
      )));
      createNewAnnotationBatchRecord(batchIds.get(), newAnnotations);
    } else if (event.batchId() != null) {
      batchIds = Optional.of(newAnnotations.stream().collect(Collectors.toMap(
          Annotation::getId,
          value -> event.batchId()
      )));
    } else {
      batchIds = Optional.empty();
    }
    return batchIds;
  }

  public void mintBatchId(Annotation annotation) {
    var batchId = UUID.randomUUID();
    annotation.setOdsBatchID(batchId);
    createNewAnnotationBatchRecord(Map.of(annotation.getId(), batchId), List.of(annotation));
    log.info("Batch record {} has been created for hashedAnnotation", batchId);
  }

  private void createNewAnnotationBatchRecord(Map<String, UUID> batchIds,
      List<Annotation> annotations) {
    var batchRecords = new ArrayList<AnnotationBatchRecord>();
    for (var annotation : annotations) {
      batchRecords.add(new AnnotationBatchRecord(
          batchIds.get(annotation.getId()),
          annotation.getDctermsCreator().getId(),
          annotation.getId(),
          Instant.now(),
          annotation.getOdsJobID()
      ));
    }
    repository.createAnnotationBatchRecord(batchRecords);
  }

  public void updateAnnotationBatchRecord(UUID batchId, long batchIdCount) {
    repository.updateAnnotationBatchRecord(batchId, batchIdCount);
  }

  public void rollbackAnnotationBatchRecord(Optional<Map<String, UUID>> batchIds) {
    batchIds.ifPresent(stringUUIDMap -> repository.rollbackAnnotationBatchRecord(
        new HashSet<>(stringUUIDMap.values())));
  }

}
