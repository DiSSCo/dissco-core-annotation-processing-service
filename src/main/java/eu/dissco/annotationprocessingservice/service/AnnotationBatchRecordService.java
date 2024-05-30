package eu.dissco.annotationprocessingservice.service;

import eu.dissco.annotationprocessingservice.domain.AnnotationBatchRecord;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.domain.HashedAnnotation;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.repository.AnnotationBatchRecordRepository;
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
      boolean batchingRequested, AnnotationEvent event) {
    Optional<Map<String, UUID>> batchIds;
    if (batchingRequested && event.batchId() == null) {
      batchIds = Optional.of(newAnnotations.stream().collect(Collectors.toMap(
          Annotation::getOdsId,
          value -> UUID.randomUUID()
      )));
      createNewAnnotationBatchRecord(batchIds.get(), newAnnotations);
    } else {
      batchIds = Optional.empty();
    }
    return batchIds;
  }

  public void mintBatchId(Annotation annotation) {
    var batchId = UUID.randomUUID();
    annotation.setOdsBatchId(batchId);
    createNewAnnotationBatchRecord(Map.of(annotation.getOdsId(), batchId), List.of(annotation));
  }

  private void createNewAnnotationBatchRecord(Map<String, UUID> batchIds,
      List<Annotation> annotations) {
    var batchRecords = new ArrayList<AnnotationBatchRecord>();
    for (var annotation : annotations) {
      batchRecords.add(new AnnotationBatchRecord(
          batchIds.get(annotation.getOdsId()),
          annotation.getOaCreator().getOdsId(),
          annotation.getOdsId(),
          Instant.now(),
          annotation.getOdsJobId()
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
