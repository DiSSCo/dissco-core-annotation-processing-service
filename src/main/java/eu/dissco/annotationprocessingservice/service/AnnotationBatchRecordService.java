package eu.dissco.annotationprocessingservice.service;

import eu.dissco.annotationprocessingservice.domain.AnnotationBatchRecord;
import eu.dissco.annotationprocessingservice.domain.HashedAnnotation;
import eu.dissco.annotationprocessingservice.domain.MasJobRecord;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.repository.AnnotationBatchRecordRepository;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class AnnotationBatchRecordService {

  private AnnotationBatchRecordRepository repository;

  public void createNewAnnotationBatchRecord(Optional<UUID> batchId, Annotation annotation,
      boolean isBatchResult, MasJobRecord mjr) {
    if (isBatchResult || batchId.isEmpty()) {
      return;
    }
    var batchRecord = new AnnotationBatchRecord(
        batchId.get(),
        annotation.getOaCreator().getOdsId(),
        annotation.getAsGenerator().getOdsId(),
        annotation.getOdsId(),
        Instant.now(),
        mjr.jobId()
    );
    repository.createAnnotationBatchRecord(batchRecord);
  }

  public void updateAnnotationBatchRecord(UUID batchId, Long annotationCount) {
    repository.updateAnnotationBatchRecord(batchId, annotationCount);
  }

  public void rollbackAnnotationBatchRecord(Optional<UUID> batchId, boolean isBatchResult) {
    if (batchId.isPresent() && !isBatchResult) {
      repository.rollbackAnnotationBatchRecord(batchId.get());
    }
  }

  public Optional<UUID> getBatchId(MasJobRecord masJobRecord, boolean isBatchResult, List<HashedAnnotation> newAnnotations) {
    // !( A or B) => (!A and !B), if no batching requested and not batch result
    if (!(masJobRecord.batchingRequested() || isBatchResult)) {
      return Optional.empty();
    }
    if (isBatchResult) {
      var batchIds = repository.getBatchIdFromMasJobId(masJobRecord.jobId());
    }
    return Optional.of(UUID.randomUUID());
  }

}
