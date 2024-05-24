package eu.dissco.annotationprocessingservice.service;

import eu.dissco.annotationprocessingservice.domain.AnnotationBatchRecord;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
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
    Optional<Map<String, UUID>> batchIds =
        batchingRequested && event.batchId() == null ? Optional.of(newAnnotations.stream().collect(Collectors.toMap(
            Annotation::getOdsId,
            value -> UUID.randomUUID()
        ))) : Optional.empty();
    batchIds.ifPresent(
        stringUUIDMap -> createNewAnnotationBatchRecord(stringUUIDMap, newAnnotations));
    return batchIds;
  }

  private void createNewAnnotationBatchRecord(Map<String, UUID> batchIds,
      List<Annotation> annotations) {
    var batchRecords = new ArrayList<AnnotationBatchRecord>();
    for (var annotation : annotations) {
      batchRecords.add(new AnnotationBatchRecord(
          batchIds.get(annotation.getOdsId()),
          annotation.getOaCreator().getOdsId(),
          annotation.getAsGenerator().getOdsId(),
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

  public void rollbackAnnotationBatchRecord(Optional<Map<String, UUID>> batchIds,
      boolean isBatchResult) {
    if (batchIds.isPresent() && !isBatchResult) {
      repository.rollbackAnnotationBatchRecord(new HashSet<>(batchIds.get().values()));
    }
  }

}
