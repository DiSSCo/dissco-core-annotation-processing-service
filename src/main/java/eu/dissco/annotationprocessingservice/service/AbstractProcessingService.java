package eu.dissco.annotationprocessingservice.service;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.domain.annotation.Generator;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.web.HandleComponent;
import java.io.IOException;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public abstract class AbstractProcessingService {

  protected final AnnotationRepository repository;
  protected final ElasticSearchRepository elasticRepository;
  protected final KafkaPublisherService kafkaService;
  protected final FdoRecordService fdoRecordService;
  protected final HandleComponent handleComponent;
  protected final ApplicationProperties applicationProperties;

  protected void enrichNewAnnotation(Annotation annotation, String id) {
    annotation
        .withOdsId(id)
        .withOdsVersion(1)
        .withAsGenerator(createGenerator())
        .withOaGenerated(Instant.now());
  }

  private Generator createGenerator() {
    return new Generator()
        .withOdsId(applicationProperties.getProcessorHandle())
        .withFoafName("Annotation Processing Service")
        .withOdsType("tool/Software");
  }

  protected void enrichUpdateAnnotation(Annotation annotation, Annotation currentAnnotation) {
    annotation
        .withOdsId(currentAnnotation.getOdsId())
        .withOdsVersion(currentAnnotation.getOdsVersion() + 1)
        .withOaGenerated(currentAnnotation.getOaGenerated())
        .withAsGenerator(currentAnnotation.getAsGenerator())
        .withOaCreator(currentAnnotation.getOaCreator())
        .withDcTermsCreated(currentAnnotation.getDcTermsCreated());
  }

  protected void rollbackNewAnnotation(Annotation annotation, boolean elasticRollback)
      throws FailedProcessingException {
    log.warn("Rolling back for annotation: {}", annotation);
    if (elasticRollback) {
      try {
        elasticRepository.archiveAnnotation(annotation.getOdsId());
      } catch (IOException | ElasticsearchException e) {
        log.info("Fatal exception, unable to rollback: {}", annotation.getOdsId(), e);
      }
    }
    repository.rollbackAnnotation(annotation.getOdsId());
    rollbackHandleCreation(annotation);
    throw new FailedProcessingException();
  }

  private void rollbackHandleCreation(Annotation annotation) {
    var requestBody = fdoRecordService.buildRollbackCreationRequest(annotation);
    try {
      handleComponent.rollbackHandleCreation(requestBody);
    } catch (PidCreationException e) {
      log.error("Unable to rollback creation for annotation {}", annotation.getOdsId(), e);
    }
  }

  public void archiveAnnotation(String id) throws IOException, FailedProcessingException {
    if (repository.getAnnotationById(id).isPresent()) {
      log.info("Archive annotation: {} in handle service", id);
      var requestBody = fdoRecordService.buildArchiveHandleRequest(id);
      try {
        handleComponent.archiveHandle(requestBody, id);
      } catch (PidCreationException e) {
        log.error("Unable to archive annotation in handle system for annotation {}", id, e);
        throw new FailedProcessingException();
      }
      log.info("Removing annotation: {} from indexing service", id);
      var document = elasticRepository.archiveAnnotation(id);
      if (document.result().equals(Result.Deleted) || document.result().equals(Result.NotFound)) {
        log.info("Archive annotation: {} in database", id);
        repository.archiveAnnotation(id);
        log.info("Archived annotation: {}", id);
        log.info("Tombstoning PID record of annotation: {}", id);
      }
    } else {
      log.info("Annotation with id: {} is already archived", id);
    }
  }

  protected void filterUpdatesAndUpdateHandleRecord(Annotation currentAnnotation,
      Annotation annotation) throws PidCreationException {
    if (!fdoRecordService.handleNeedsUpdate(currentAnnotation, annotation)) {
      return;
    }
    var requestBody = fdoRecordService.buildPatchRollbackHandleRequest(annotation,
        currentAnnotation.getOdsId());
    handleComponent.updateHandle(requestBody);
  }

  protected void filterUpdatesAndRollbackHandleUpdateRecord(Annotation currentAnnotation,
      Annotation annotation) {
    if (!fdoRecordService.handleNeedsUpdate(currentAnnotation, annotation)) {
      return;
    }
    var requestBody = fdoRecordService.buildPatchRollbackHandleRequest(annotation,
        currentAnnotation.getOdsId());
    try {
      handleComponent.rollbackHandleUpdate(requestBody);
    } catch (PidCreationException e) {
      log.error("Unable to rollback handle update for annotation {}", currentAnnotation.getOdsId(),
          e);
    }
  }

  protected void rollbackUpdatedAnnotation(Annotation currentAnnotation, Annotation annotation,
      boolean elasticRollback) throws FailedProcessingException {
    if (elasticRollback) {
      try {
        elasticRepository.indexAnnotation(currentAnnotation);
      } catch (IOException | ElasticsearchException e) {
        log.error("Fatal exception, unable to rollback update for: {}", annotation, e);
      }
    }
    repository.createAnnotationRecord(currentAnnotation);
    filterUpdatesAndRollbackHandleUpdateRecord(currentAnnotation, annotation);
    throw new FailedProcessingException();
  }

  protected void indexElasticNewAnnotation(Annotation annotation, String id)
      throws FailedProcessingException {
    IndexResponse indexDocument = null;
    try {
      indexDocument = elasticRepository.indexAnnotation(annotation);
    } catch (IOException | ElasticsearchException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      rollbackNewAnnotation(annotation, false);
      throw new FailedProcessingException();
    }
    if (indexDocument.result().equals(Result.Created)) {
      log.info("Annotation: {} has been successfully indexed", id);
      try {
        kafkaService.publishCreateEvent(annotation);
      } catch (JsonProcessingException e) {
        rollbackNewAnnotation(annotation, true);
      }
    } else {
      log.error("Elasticsearch did not create annotation: {}", id);
      throw new FailedProcessingException();
    }
  }

  protected void indexElasticUpdatedAnnotation(Annotation annotation, Annotation currentAnnotation)
      throws FailedProcessingException {
    IndexResponse indexDocument = null;
    try {
      indexDocument = elasticRepository.indexAnnotation(annotation);
    } catch (IOException | ElasticsearchException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      rollbackUpdatedAnnotation(currentAnnotation, annotation, false);
      throw new FailedProcessingException();
    }
    if (indexDocument.result().equals(Result.Updated)) {
      log.info("Annotation: {} has been successfully indexed", currentAnnotation.getOdsId());
      try {
        kafkaService.publishUpdateEvent(currentAnnotation, annotation);
      } catch (JsonProcessingException e) {
        rollbackUpdatedAnnotation(currentAnnotation, annotation, true);
        throw new FailedProcessingException();
      }
    }
  }


}
