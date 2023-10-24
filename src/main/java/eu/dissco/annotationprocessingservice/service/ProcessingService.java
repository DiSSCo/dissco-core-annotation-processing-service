package eu.dissco.annotationprocessingservice.service;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.domain.annotation.Generator;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.NotFoundException;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.web.HandleComponent;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProcessingService {

  private final ObjectMapper mapper;
  private final AnnotationRepository repository;
  private final ElasticSearchRepository elasticRepository;
  private final KafkaPublisherService kafkaService;
  private final FdoRecordService fdoRecordService;
  private final HandleComponent handleComponent;
  private final Environment environment;
  private final ApplicationProperties applicationProperties;

  private static boolean annotationAreEqual(Annotation currentAnnotation, Annotation annotation) {
    return currentAnnotation.getOaBody().equals(annotation.getOaBody())
        && currentAnnotation.getOaCreator().equals(annotation.getOaCreator())
        && currentAnnotation.getOaTarget().equals(annotation.getOaTarget()) &&
        (currentAnnotation.getOaMotivatedBy() != null
            && currentAnnotation.getOaMotivatedBy().equals(annotation.getOaMotivatedBy())
            || currentAnnotation.getOaMotivatedBy() == null && annotation.getOaMotivatedBy() == null)
        && currentAnnotation.getOdsAggregateRating().equals(annotation.getOdsAggregateRating())
        && currentAnnotation.getOaMotivation().equals(annotation.getOaMotivation());
  }

  public Annotation updateAnnotation(Annotation annotation)
      throws FailedProcessingException, NotFoundException {
    var currentAnnotation = repository.getAnnotation(annotation.getOdsId());
    if (currentAnnotation == null) {
      throw new NotFoundException(annotation.getOdsId());
    }
    return updateExistingAnnotation(currentAnnotation, annotation);
  }

  public Annotation createNewAnnotation(Annotation annotation) throws FailedProcessingException {
    return persistNewAnnotation(annotation);
  }

  public void handleMessage(AnnotationEvent event)
      throws DataBaseException, FailedProcessingException {
    log.info("Received annotation event of: {}", event);
    var annotation = event.annotation();
    var currentAnnotationOptional = getExistingAnnotation(annotation);
    if (currentAnnotationOptional.isPresent()) {
      var currentAnnotation = currentAnnotationOptional.get();
      if (annotationAreEqual(currentAnnotation, annotation)) {
        log.info("Received annotation is equal to annotation: {}", currentAnnotation.getOdsId());
        processEqualAnnotation(currentAnnotation);
      } else {
        log.info("Annotation with id: {} has received an update", currentAnnotation.getOdsId());
        updateExistingAnnotation(currentAnnotation, annotation);
      }
    } else {
      persistNewAnnotation(annotation);
    }
  }


  private Optional<Annotation> getExistingAnnotation(Annotation annotation)
      throws FailedProcessingException {
    var existingAnnotations = repository.getAnnotation(annotation);
    if (existingAnnotations.size() > 1) {
      log.error(
          "Multiple annotations exist with same motivation, target, creator as {}, and this is not a web request",
          annotation);
      throw new FailedProcessingException();
    }
    if (existingAnnotations.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(existingAnnotations.get(0));
  }

  private Annotation updateExistingAnnotation(Annotation currentAnnotation,
      Annotation annotation) throws FailedProcessingException {
    try {
      filterUpdatesAndUpdateHandleRecord(currentAnnotation, annotation);
    } catch (PidCreationException e) {
      log.error("Unable to post update for annotation {}", currentAnnotation.getOdsId(), e);
      throw new FailedProcessingException();
    }
    enrichUpdateAnnotation(annotation, currentAnnotation, currentAnnotation.getOdsId(),
        currentAnnotation.getOdsVersion() + 1);

    repository.createAnnotationRecord(annotation);

    log.info("Annotation: {} has been successfully committed to database",
        currentAnnotation.getOdsId());
    IndexResponse indexDocument = null;
    try {
      indexDocument = elasticRepository.indexAnnotation(annotation);
    } catch (IOException | ElasticsearchException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      rollbackUpdatedAnnotation(currentAnnotation, annotation, false);
    }
    if (indexDocument.result().equals(Result.Updated)) {
      log.info("Annotation: {} has been successfully indexed", currentAnnotation.getOdsId());
      try {
        kafkaService.publishUpdateEvent(currentAnnotation, annotation);
      } catch (JsonProcessingException e) {
        rollbackUpdatedAnnotation(currentAnnotation, annotation, true);
      }
    }
    return annotation;
  }

  private void rollbackUpdatedAnnotation(Annotation currentAnnotation, Annotation annotation,
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


  private void processEqualAnnotation(Annotation currentAnnotation) {
    repository.updateLastChecked(currentAnnotation);
    log.info("Successfully updated lastChecked for existing annotation: {}",
        currentAnnotation.getOdsId());
  }

  private Annotation persistNewAnnotation(Annotation annotation) throws FailedProcessingException {
    var id = postHandle(annotation);
    enrichAnnotation(annotation, id, 1, true);
    log.info("New id has been generated for Annotation: {}", annotation.getOdsId());
    repository.createAnnotationRecord(annotation);
    log.info("Annotation: {} has been successfully committed to database", id);
    IndexResponse indexDocument = null;
    try {
      indexDocument = elasticRepository.indexAnnotation(annotation);
    } catch (IOException | ElasticsearchException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      rollbackNewAnnotation(annotation, false);
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
    return annotation;
  }

  private void enrichAnnotation(Annotation annotation, String id, int version, boolean isNew) {
    annotation.withOdsId(id);
    annotation.withOdsVersion(version);
    annotation.withAsGenerator(createGenerator());
    if (isNew){
      annotation.withOaGenerated(Instant.now());
    }
  }

  private void enrichUpdateAnnotation(Annotation annotation, Annotation currentAnnotation, String id, int version){
    enrichAnnotation(annotation, id, version, false);
    annotation.withDcTermsCreated(currentAnnotation.getDcTermsCreated());
    annotation.withOaGenerated(currentAnnotation.getOaGenerated());
  }

  private Generator createGenerator() {
    return new Generator()
        .withOdsId(applicationProperties.getProcessorHandle())
        .withFoafName("Annotation Processing Service")
        .withOdsType("tool/Software");
  }

  private String postHandle(Annotation annotation) throws FailedProcessingException {
    var requestBody = fdoRecordService.buildPostHandleRequest(annotation);
    try {
      return handleComponent.postHandle(requestBody);
    } catch (PidCreationException e) {
      log.error("Unable to create handle for given annotation. ", e);
      throw new FailedProcessingException();
    }
  }

  private void rollbackNewAnnotation(Annotation annotation, boolean elasticRollback)
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

  private void filterUpdatesAndUpdateHandleRecord(Annotation currentAnnotation,
      Annotation annotation) throws PidCreationException {
    if (!fdoRecordService.handleNeedsUpdate(currentAnnotation, annotation)) {
      return;
    }
    var requestBody = fdoRecordService.buildPatchRollbackHandleRequest(annotation,
        currentAnnotation.getOdsId());
    handleComponent.updateHandle(requestBody);
  }

  private void filterUpdatesAndRollbackHandleUpdateRecord(Annotation currentAnnotation,
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
}
