package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.configuration.ApplicationConfiguration.HANDLE_PROXY;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.component.AnnotationHasher;
import eu.dissco.annotationprocessingservice.component.AnnotationValidatorComponent;
import eu.dissco.annotationprocessingservice.database.jooq.enums.ErrorCode;
import eu.dissco.annotationprocessingservice.domain.HashedAnnotation;
import eu.dissco.annotationprocessingservice.domain.HashedAnnotationRequest;
import eu.dissco.annotationprocessingservice.domain.ProcessResult;
import eu.dissco.annotationprocessingservice.domain.UpdatedAnnotation;
import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import eu.dissco.annotationprocessingservice.exception.BatchingException;
import eu.dissco.annotationprocessingservice.exception.ConflictException;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.schema.Annotation;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingEvent;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;
import eu.dissco.annotationprocessingservice.web.HandleComponent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.jooq.exception.DataAccessException;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@Profile(Profiles.KAFKA)
public class ProcessingKafkaService extends AbstractProcessingService {

  private final AnnotationHasher annotationHasher;

  public ProcessingKafkaService(AnnotationRepository repository,
      ElasticSearchRepository elasticRepository,
      KafkaPublisherService kafkaService, FdoRecordService fdoRecordService,
      HandleComponent handleComponent, ApplicationProperties applicationProperties,
      MasJobRecordService masJobRecordService, AnnotationHasher annotationHasher,
      AnnotationValidatorComponent schemaValidator, BatchAnnotationService batchAnnotationService,
      AnnotationBatchRecordService annotationBatchRecordService) {
    super(repository, elasticRepository, kafkaService, fdoRecordService, handleComponent,
        applicationProperties, schemaValidator, masJobRecordService, batchAnnotationService,
        annotationBatchRecordService);
    this.annotationHasher = annotationHasher;
  }

  private static List<String> getIdListFromUpdates(Set<UpdatedAnnotation> updatedAnnotations) {
    return updatedAnnotations.stream()
        .map(UpdatedAnnotation::hashedCurrentAnnotation)
        .map(HashedAnnotation::annotation)
        .map(Annotation::getId).toList();

  }

  public void handleMessage(AnnotationProcessingEvent event)
      throws DataBaseException, FailedProcessingException, AnnotationValidationException, BatchingException, ConflictException {
    log.info("Received annotations event of: {}", event);
    masJobRecordService.verifyMasJobId(event);
    var isBatchResult = event.getBatchId() != null;
    if (event.getAnnotations().isEmpty()) {
      log.info("MAS job completed without any annotations");
      masJobRecordService.markEmptyMasJobRecordAsComplete(event.getJobId(), isBatchResult);
    } else {
      schemaValidator.validateEvent(event);
      var masJobRecord = masJobRecordService.getMasJobRecord(event.getJobId());
      var processResult = processAnnotations(event);
      var equalIds = processEqualAnnotations(processResult.equalAnnotations());
      var updatedIds = updateExistingAnnotations(processResult.changedAnnotations(),
          event.getJobId(),
          isBatchResult);
      var newAnnotations = persistNewAnnotation(processResult.newAnnotations(), event.getJobId(),
          isBatchResult, masJobRecord.batchingRequested(), event);
      var idList = Stream.of(equalIds, updatedIds,
              newAnnotations.stream().map(Annotation::getId).toList()).flatMap(Collection::stream)
          .toList();
      checkForTimeoutErrors(masJobRecord.error(), event.getJobId());
      masJobRecordService.markMasJobRecordAsComplete(event.getJobId(), idList, isBatchResult);
      applyBatchAnnotations(event, newAnnotations);
    }
  }

  private void checkForTimeoutErrors(ErrorCode errorCode, String jobId) {
    if (ErrorCode.TIMEOUT.equals(errorCode)) {
      log.warn("MJR {} previously marked as timed out. Removing error.", jobId);
    }
  }

  private ProcessResult processAnnotations(AnnotationProcessingEvent event) {
    var equalAnnotations = new HashSet<Annotation>();
    var changedAnnotations = new HashSet<UpdatedAnnotation>();
    var hashedAnnotations =
        event.getAnnotations().stream()
            .map(annotation -> new HashedAnnotationRequest(annotation, hashAnnotation(annotation)))
            .collect(Collectors.toSet());
    var existingAnnotations = repository.getAnnotationFromHash(
        hashedAnnotations.stream().map(HashedAnnotationRequest::hash).collect(Collectors.toSet()));
    var newAnnotations = filterNewAnnotations(hashedAnnotations, existingAnnotations);
    var equalOrUpdatedAnnotationsMap = hashedAnnotations.stream()
        .filter(ha -> !newAnnotations.contains(ha))
        .collect(Collectors.toMap(HashedAnnotationRequest::hash, ha -> ha));

    for (var currentAnnotation : existingAnnotations) {
      var eventAnnotation = equalOrUpdatedAnnotationsMap.get(currentAnnotation.hash());
      var updatedAnnotation = buildAnnotation(eventAnnotation.annotation(),
          currentAnnotation.annotation().getId(),
          event, currentAnnotation.annotation().getOdsVersion() + 1);
      if (annotationsAreEqual(currentAnnotation.annotation(), updatedAnnotation)) {
        equalAnnotations.add(currentAnnotation.annotation());
      } else {
        changedAnnotations.add(new UpdatedAnnotation(currentAnnotation,
            new HashedAnnotation(updatedAnnotation, eventAnnotation.hash())));
      }
    }
    return new ProcessResult(equalAnnotations, changedAnnotations, newAnnotations);
  }

  private List<HashedAnnotationRequest> filterNewAnnotations(
      Set<HashedAnnotationRequest> allAnnotations,
      List<HashedAnnotation> existingAnnotations) {
    var existingHashes = existingAnnotations.stream().map(HashedAnnotation::hash).toList();
    return allAnnotations.stream().filter(ha -> !existingHashes.contains(ha.hash())).toList();
  }

  private List<Annotation> persistNewAnnotation(
      List<HashedAnnotationRequest> hashedAnnotationsRequest, String jobId,
      boolean isBatchResult, boolean batchingRequested, AnnotationProcessingEvent event)
      throws FailedProcessingException {
    if (hashedAnnotationsRequest.isEmpty()) {
      return Collections.emptyList();
    }
    var idMap = postHandles(hashedAnnotationsRequest, jobId, isBatchResult);
    var idList = idMap.values().stream().toList();
    var hashedAnnotations = hashedAnnotationsRequest.stream().map(
        p -> new HashedAnnotation(buildAnnotation(p.annotation(), HANDLE_PROXY + idMap.get(p.hash()), event, 1),
            p.hash())).toList();
    var batchIds = annotationBatchRecordService.mintBatchIds(
        hashedAnnotations.stream().map(HashedAnnotation::annotation).toList(),
        batchingRequested, event);
    hashedAnnotations.forEach(annotation -> addBatchIds(annotation.annotation(), batchIds, event));
    log.info("New ids have been generated for Annotations: {}", idList);
    try {
      repository.createAnnotationRecord(hashedAnnotations);
    } catch (DataAccessException e) {
      log.error("Unable to post new Annotation to DB", e);
      annotationBatchRecordService.rollbackAnnotationBatchRecord(batchIds);
      rollbackHandleCreation(idList);
      throw new FailedProcessingException();
    }
    log.info("Annotations {} has been successfully committed to database", idList);

    try {
      indexElasticNewAnnotations(
          hashedAnnotations.stream().map(HashedAnnotation::annotation).toList(),
          idList);
    } catch (FailedProcessingException e) {
      rollbackHandleCreation(idList);
      masJobRecordService.markMasJobRecordAsFailed(jobId, isBatchResult);
      annotationBatchRecordService.rollbackAnnotationBatchRecord(batchIds);
      throw new FailedProcessingException();
    }
    return hashedAnnotations.stream().map(HashedAnnotation::annotation).toList();
  }

  private Map<UUID, String> postHandles(List<HashedAnnotationRequest> hashedAnnotations,
      String jobId, boolean isBatchResult) throws FailedProcessingException {
    var requestBody = fdoRecordService.buildPostHandleRequest(hashedAnnotations);
    try {
      return handleComponent.postHandles(requestBody);
    } catch (PidCreationException e) {
      log.error("Unable to create handle for given annotations. ", e);
      masJobRecordService.markMasJobRecordAsFailed(jobId, isBatchResult);
      throw new FailedProcessingException();
    }
  }

  private List<String> updateExistingAnnotations(Set<UpdatedAnnotation> updatedAnnotations,
      String jobId, boolean isBatchResult) throws FailedProcessingException {
    if (updatedAnnotations.isEmpty()) {
      return Collections.emptyList();
    }
    var idList = getIdListFromUpdates(updatedAnnotations);
    try {
      filterUpdatesAndUpdateHandleRecord(updatedAnnotations);
    } catch (PidCreationException e) {
      log.error("Unable to post update for annotations {}", idList, e);
      masJobRecordService.markMasJobRecordAsFailed(jobId, isBatchResult);
      throw new FailedProcessingException();
    }

    try {
      repository.createAnnotationRecord(
          updatedAnnotations.stream().map(UpdatedAnnotation::hashedAnnotation).toList());
    } catch (DataAccessException e) {
      log.error("Unable to update annotations in database. Rolling back handle updates", e);
      filterUpdatesAndRollbackHandleUpdateRecord(updatedAnnotations);
      throw new FailedProcessingException();
    }
    log.info("Annotation: {} has been successfully committed to database", idList);
    try {
      indexElasticUpdatedAnnotation(updatedAnnotations);
    } catch (FailedProcessingException e) {
      filterUpdatesAndRollbackHandleUpdateRecord(updatedAnnotations);
      masJobRecordService.markMasJobRecordAsFailed(jobId, isBatchResult);
      throw new FailedProcessingException();
    }
    return idList;
  }

  private UUID hashAnnotation(AnnotationProcessingRequest annotation) {
    return annotationHasher.getAnnotationHash(annotation);
  }

  private void indexElasticNewAnnotations(List<Annotation> annotations, List<String> idList)
      throws FailedProcessingException {
    BulkResponse bulkResponse = null;
    try {
      bulkResponse = elasticRepository.indexAnnotations(annotations);
    } catch (IOException | ElasticsearchException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      rollbackNewAnnotations(annotations, false);
      throw new FailedProcessingException();
    }
    if (!bulkResponse.errors()) {
      log.info("Annotations: {} have been successfully indexed", idList);
      try {
        for (var annotation : annotations) {
          kafkaService.publishCreateEvent(annotation);
        }
      } catch (JsonProcessingException e) {
        log.error("Unable to publish annotations to kafka, rolling back");
        rollbackNewAnnotations(annotations, true);
        throw new FailedProcessingException();
      }
    } else {
      partiallyFailedElasticInsert(annotations, bulkResponse);
      throw new FailedProcessingException();
    }
  }

  private void indexElasticUpdatedAnnotation(Set<UpdatedAnnotation> updatedAnnotations)
      throws FailedProcessingException {
    var idList = getIdListFromUpdates(updatedAnnotations);
    BulkResponse bulkResponse = null;
    try {
      bulkResponse = elasticRepository.indexAnnotations(updatedAnnotations
          .stream()
          .map(UpdatedAnnotation::hashedAnnotation)
          .map(HashedAnnotation::annotation)
          .toList());
    } catch (IOException | ElasticsearchException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      rollbackUpdatedAnnotations(updatedAnnotations, false);
      throw new FailedProcessingException();
    }
    if (!bulkResponse.errors()) {
      log.info("Annotations: {} have been successfully indexed", idList);
      try {
        for (var updatedAnnotation : updatedAnnotations) {
          kafkaService.publishUpdateEvent(updatedAnnotation.hashedCurrentAnnotation().annotation(),
              updatedAnnotation.hashedAnnotation()
                  .annotation());
        }
      } catch (JsonProcessingException e) {
        rollbackUpdatedAnnotations(updatedAnnotations, true);
        throw new FailedProcessingException();
      }
    } else {
      partiallyFailedElasticUpdate(updatedAnnotations, bulkResponse);
      throw new FailedProcessingException();
    }
  }

  private void rollbackNewAnnotations(List<Annotation> annotations, boolean elasticRollback) {
    var idList = annotations.stream().map(Annotation::getId).toList();
    log.warn("Rolling back for annotations: {}", idList);
    if (elasticRollback) {
      try {
        elasticRepository.archiveAnnotations(idList);
      } catch (IOException | ElasticsearchException e) {
        log.info("Fatal exception, unable to rollback: {}", idList);
      }
    }
    repository.rollbackAnnotations(idList);

  }

  private void rollbackUpdatedAnnotations(Set<UpdatedAnnotation> updatedAnnotations,
      boolean elasticRollback) {
    var currentAnnotationsHashed = updatedAnnotations.stream()
        .map(UpdatedAnnotation::hashedCurrentAnnotation).toList();
    var idList = getIdListFromUpdates(updatedAnnotations);
    if (elasticRollback) {
      try {
        elasticRepository.indexAnnotations(
            currentAnnotationsHashed.stream().map(HashedAnnotation::annotation).toList());
      } catch (IOException | ElasticsearchException e) {
        log.error("Fatal exception, unable to rollback update for: {}", idList, e);
      }
    }
    try {
      repository.createAnnotationRecord(currentAnnotationsHashed);
    } catch (DataAccessException e) {
      log.error("Fatal database exception. Unable to rollback annotations to original state");
    }

  }

  private void rollbackHandleCreation(List<String> idList) {
    var requestBody = fdoRecordService.buildRollbackCreationRequest(idList);
    try {
      handleComponent.rollbackHandleCreation(requestBody);
    } catch (PidCreationException e) {
      log.error("Unable to rollback creation for annotations {}", idList, e);
    }
  }

  private void filterUpdatesAndRollbackHandleUpdateRecord(
      Set<UpdatedAnnotation> updatedAnnotations) {
    var requestBody = filterHandleUpdates(updatedAnnotations);
    try {
      handleComponent.rollbackHandleUpdate(requestBody);
    } catch (PidCreationException e) {
      log.error("Unable to rollback handle update for annotations {}",
          updatedAnnotations.stream().map(p -> p.hashedCurrentAnnotation().annotation().getId())
              .toList());
    }
  }

  private void filterUpdatesAndUpdateHandleRecord(Set<UpdatedAnnotation> updatedAnnotations)
      throws PidCreationException {
    var requestBody = filterHandleUpdates(updatedAnnotations);
    if (!requestBody.isEmpty()) {
      handleComponent.updateHandle(requestBody);
    }
  }

  private List<JsonNode> filterHandleUpdates(Set<UpdatedAnnotation> updatedAnnotations) {
    var handleNeedsUpdate = updatedAnnotations.stream()
        .filter(p -> fdoRecordService.handleNeedsUpdate(p.hashedCurrentAnnotation().annotation(),
            p.hashedAnnotation().annotation()))
        .map(UpdatedAnnotation::hashedAnnotation)
        .toList();
    if (handleNeedsUpdate.isEmpty()) {
      return Collections.emptyList();
    }
    return fdoRecordService.buildPatchRollbackHandleRequest(handleNeedsUpdate);
  }

  private void partiallyFailedElasticInsert(List<Annotation> annotations,
      BulkResponse bulkResponse) {
    var annotationMap = annotations.stream()
        .collect(Collectors.toMap(Annotation::getId, a -> a));
    var annotationRollbacksElasticFail = new ArrayList<Annotation>();
    var annotationRollbacksElasticSuccess = new ArrayList<Annotation>();

    bulkResponse.items().forEach(
        item -> {
          var annotation = annotationMap.get(item.id());
          if (item.error() != null) {
            annotationRollbacksElasticFail.add(annotation);
            log.error("Failed item to insert into elastic search: {} with errors {}",
                annotation.getId(), item.error().reason());
          } else {
            annotationRollbacksElasticSuccess.add(annotation);
          }
        }
    );
    rollbackNewAnnotations(annotationRollbacksElasticFail, false);
    rollbackNewAnnotations(annotationRollbacksElasticSuccess, true);
  }

  private void partiallyFailedElasticUpdate(Set<UpdatedAnnotation> updatedAnnotations,
      BulkResponse bulkResponse) {

    var annotationMap = updatedAnnotations.stream()
        .collect(Collectors.toMap(p -> p.hashedAnnotation().annotation().getId(), p -> p));
    var annotationRollbacksElasticFail = new HashSet<UpdatedAnnotation>();
    var annotationRollbacksElasticSuccess = new HashSet<UpdatedAnnotation>();

    bulkResponse.items().forEach(
        item -> {
          var annotationPair = annotationMap.get(item.id());
          if (item.error() != null) {
            annotationRollbacksElasticFail.add(annotationPair);
            log.error("Failed item to insert into elastic search: {} with errors {}",
                annotationPair.hashedAnnotation().annotation().getId(), item.error().reason());
          } else {
            annotationRollbacksElasticSuccess.add(annotationPair);
          }
        }
    );
    rollbackUpdatedAnnotations(annotationRollbacksElasticFail, false);
    rollbackUpdatedAnnotations(annotationRollbacksElasticSuccess, true);
  }

}
