package eu.dissco.annotationprocessingservice.service;

import eu.dissco.annotationprocessingservice.domain.AnnotationBatchRecord;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.domain.MasJobRecord;
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

  public void createNewAnnotationBatchRecord(Optional<Map<String, UUID>> batchIds,
      List<Annotation> annotations,
      boolean isBatchResult) {
    if (isBatchResult || batchIds.isEmpty()) {
      return;
    }
    var batchRecords = new ArrayList<AnnotationBatchRecord>();
    for (var annotation : annotations) {
      batchRecords.add(new AnnotationBatchRecord(
          batchIds.get().get(annotation.getOdsId()),
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
    /*batchIdCount.entrySet().stream().filter(e -> e.getValue() > 0)
        .forEach(e -> repository.updateAnnotationBatchRecord(e.getKey(), e.getValue()));

     */
  }

  public void rollbackAnnotationBatchRecord(Optional<Map<String, UUID>> batchIds,
      boolean isBatchResult) {
    if (batchIds.isPresent() && !isBatchResult) {
      repository.rollbackAnnotationBatchRecord(new HashSet<>(batchIds.get().values()));
    }
  }

  public Optional<Map<String, UUID>> getBatchId(MasJobRecord masJobRecord, List<Annotation> newAnnotations, AnnotationEvent event) {
    // If no batching requested and not batch result
    if (!masJobRecord.batchingRequested() && event.batchId() == null) {
      return Optional.empty();
    }
    if (event.batchId() != null) {
      /*return Map.of(
        todo resolve
      )*/
    }
    return Optional.of(newAnnotations.stream().collect(Collectors.toMap(
        Annotation::getOdsId,
        value -> UUID.randomUUID()
    )));
  }

}
