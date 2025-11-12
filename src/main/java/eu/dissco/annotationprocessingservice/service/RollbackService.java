package eu.dissco.annotationprocessingservice.service;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import eu.dissco.annotationprocessingservice.domain.HashedAnnotation;
import eu.dissco.annotationprocessingservice.domain.UpdatedAnnotation;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import eu.dissco.annotationprocessingservice.repository.AnnotationBatchRecordRepository;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.schema.Annotation;
import eu.dissco.annotationprocessingservice.web.HandleComponent;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.exception.DataAccessException;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class RollbackService {

  private final FdoRecordService fdoRecordService;
  private final HandleComponent handleComponent;
  private final ElasticSearchRepository elasticRepository;
  private final AnnotationRepository repository;
  private final AnnotationBatchRecordRepository batchRecordRepository;

  /* New Annotation Rollback */

  public void rollbackNewAnnotationsHash(List<HashedAnnotation> hashedAnnotations,
      boolean elasticRollback, boolean repositoryRollback, Optional<Map<String, UUID>> batchIds) {
    rollbackNewAnnotations(
        hashedAnnotations.stream().map(HashedAnnotation::annotation).toList(), elasticRollback,
        repositoryRollback);
    rollbackBatchIds(batchIds);
  }

  public void rollbackNewAnnotations(List<Annotation> annotations,
      boolean elasticRollback, boolean repositoryRollback) {
    var idList = annotations.stream().map(Annotation::getId).toList();
    log.info("Rolling back for annotations: {}", idList);
    rollbackHandleCreation(idList);
    if (elasticRollback) {
      try {
        elasticRepository.archiveAnnotations(idList);
      } catch (IOException e) {
        log.error("Critical error: unable to archive annotations in elastic", e);
      }
    }
    if (repositoryRollback) {
      try {
        repository.rollbackAnnotations(idList);
      } catch (DataAccessException e) {
        log.error("Critical error: unable to rollback annotations in database", e);
      }
    }
  }

  private void rollbackHandleCreation(List<String> idList) {
    try {
      handleComponent.rollbackHandleCreation(idList);
    } catch (PidCreationException e) {
      log.error("Unable to rollback PID creation for annotations {}", idList, e);
    }
  }

  /* Rollback Updates */
  public void rollbackUpdatedAnnotations(Set<UpdatedAnnotation> updatedAnnotations,
      boolean elasticRollback, boolean repositoryRollback) {
    var currentAnnotationsHashed = updatedAnnotations.stream()
        .map(UpdatedAnnotation::hashedCurrentAnnotation).toList();
    var idList = getIdListFromUpdates(updatedAnnotations);
    filterUpdatesAndRollbackHandleUpdateRecord(updatedAnnotations);
    if (elasticRollback) {
      try {
        elasticRepository.indexAnnotations(
            currentAnnotationsHashed.stream().map(HashedAnnotation::annotation).toList());
      } catch (IOException | ElasticsearchException e) {
        log.error("Fatal exception, unable to rollback update for: {}", idList, e);
      }
    }
    if (repositoryRollback) {
      try {
        repository.createAnnotationRecordsHashed(currentAnnotationsHashed, false);
      } catch (DataAccessException e) {
        log.error("Fatal database exception. Unable to rollback annotations to original state");
      }
    }
  }

  private static List<String> getIdListFromUpdates(Set<UpdatedAnnotation> updatedAnnotations) {
    return updatedAnnotations.stream()
        .map(UpdatedAnnotation::hashedCurrentAnnotation)
        .map(HashedAnnotation::annotation)
        .map(Annotation::getId).toList();
  }

  public void rollbackUpdatedAnnotation(Annotation currentAnnotation, Annotation annotation,
      boolean elasticRollback, boolean repositoryRollback) {
    filterUpdatesAndRollbackHandleUpdateRecord(currentAnnotation, annotation);
    if (elasticRollback) {
      try {
        elasticRepository.indexAnnotation(currentAnnotation);
      } catch (IOException | ElasticsearchException e) {
        log.error("Fatal elastic exception, unable to rollback update for: {}", annotation, e);
      }
    }
    if (repositoryRollback) {
      try {
        repository.createAnnotationRecord(currentAnnotation, false);
      } catch (DataAccessException e) {
        log.error("Fatal database exception. Unable to rollback annotation to original state");
      }
    }
  }

  private void filterUpdatesAndRollbackHandleUpdateRecord(
      Set<UpdatedAnnotation> updatedAnnotations) {
    var handleNeedsUpdate = updatedAnnotations.stream()
        .filter(p -> fdoRecordService.handleNeedsUpdate(p.hashedCurrentAnnotation().annotation(),
            p.hashedAnnotation().annotation()))
        .map(UpdatedAnnotation::hashedAnnotation)
        .toList();
    if (handleNeedsUpdate.isEmpty()) {
      return;
    }
    var requestBody = fdoRecordService.buildPatchHandleRequest(handleNeedsUpdate);
    try {
      handleComponent.rollbackHandleUpdate(requestBody);
    } catch (PidCreationException e) {
      log.error("Unable to rollback handle update for annotations {}",
          updatedAnnotations.stream().map(p -> p.hashedCurrentAnnotation().annotation().getId())
              .toList());
    }
  }

  private void filterUpdatesAndRollbackHandleUpdateRecord(Annotation currentAnnotation,
      Annotation annotation) {
    if (!fdoRecordService.handleNeedsUpdate(currentAnnotation, annotation)) {
      return;
    }
    var requestBody = fdoRecordService.buildPatchHandleRequest(annotation
    );
    try {
      handleComponent.rollbackHandleUpdate(requestBody);
    } catch (PidCreationException e) {
      log.error("Unable to rollback handle update for annotations {}", currentAnnotation.getId(),
          e);
    }
  }

  public void rollbackBatchIds(Optional<Map<String, UUID>> batchIds){
    batchIds.ifPresent(stringUUIDMap -> batchRecordRepository.rollbackAnnotationBatchRecord(
        new HashSet<>(stringUUIDMap.values())));
  }
}
