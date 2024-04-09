package eu.dissco.annotationprocessingservice.service;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.component.SchemaValidatorComponent;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.NotFoundException;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.web.HandleComponent;
import java.io.IOException;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.jooq.exception.DataAccessException;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@Profile(Profiles.WEB)
public class ProcessingWebService extends AbstractProcessingService {

  public ProcessingWebService(AnnotationRepository repository,
      ElasticSearchRepository elasticRepository, KafkaPublisherService kafkaService,
      FdoRecordService fdoRecordService, HandleComponent handleComponent,
      ApplicationProperties applicationProperties, SchemaValidatorComponent schemaValidator,
      MasJobRecordService masJobRecordService, BatchAnnotationService batchAnnotationService) {
    super(repository, elasticRepository, kafkaService, fdoRecordService, handleComponent,
        applicationProperties, schemaValidator, masJobRecordService, batchAnnotationService);
  }

  public Annotation persistNewAnnotation(Annotation annotation)
      throws FailedProcessingException, AnnotationValidationException {
    schemaValidator.validateAnnotationRequest(annotation, true);
    var id = postHandle(annotation);
    enrichNewAnnotation(annotation, id);
    log.info("New id has been generated for Annotation: {}", annotation.getOdsId());
    try {
      repository.createAnnotationRecord(annotation);
    } catch (DataAccessException e) {
      log.error("Unable to post new Annotation to DB", e);
      rollbackHandleCreation(annotation);
      throw new FailedProcessingException();
    }
    log.info("Annotation: {} has been successfully committed to database", id);
    indexElasticNewAnnotation(annotation, id);
    return annotation;
  }

  public Annotation updateAnnotation(Annotation annotation)
      throws FailedProcessingException, NotFoundException, AnnotationValidationException {
    schemaValidator.validateAnnotationRequest(annotation, false);
    var currentAnnotationOptional = repository.getAnnotationForUser(annotation.getOdsId(),
        annotation.getOaCreator().getOdsId());
    if (currentAnnotationOptional.isEmpty()) {
      log.error("No annotations with id {} found for creator {}", annotation.getOdsId(),
          annotation.getOaCreator().getOdsId());
      throw new NotFoundException(annotation.getOdsId(), annotation.getOaCreator().getOdsId());
    }
    var currentAnnotation = currentAnnotationOptional.get();
    if (annotationsAreEqual(currentAnnotation, annotation)) {
      processEqualAnnotations(Set.of(currentAnnotation));
      return currentAnnotation;
    }
    enrichUpdateAnnotation(annotation, currentAnnotation);
    try {
      filterUpdatesAndUpdateHandleRecord(currentAnnotation, annotation);
    } catch (PidCreationException e) {
      log.error("Unable to post update for annotations {}", currentAnnotation.getOdsId(), e);
      throw new FailedProcessingException();
    }
    try {
      repository.createAnnotationRecord(annotation);
    } catch (DataAccessException e) {
      log.error("Unable to post new Annotation to DB", e);
      filterUpdatesAndRollbackHandleUpdateRecord(currentAnnotation, annotation);
      throw new FailedProcessingException();
    }
    log.info("Annotation: {} has been successfully committed to database",
        currentAnnotation.getOdsId());
    indexElasticUpdatedAnnotation(annotation, currentAnnotation);
    return annotation;
  }

  private String postHandle(Annotation annotation) throws FailedProcessingException {
    var requestBody = fdoRecordService.buildPostHandleRequest(annotation);
    try {
      return handleComponent.postHandle(requestBody).get(0);
    } catch (PidCreationException e) {
      log.error("Unable to create handle for given annotations. ", e);
      throw new FailedProcessingException();
    }
  }

  private void indexElasticNewAnnotation(Annotation annotation, String id)
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
        log.error("Unable to publish create event to kafka.");
        rollbackNewAnnotation(annotation, true);
        throw new FailedProcessingException();
      }
    } else {
      log.error("Elasticsearch did not create annotations: {}", id);
      rollbackNewAnnotation(annotation, false);
      throw new FailedProcessingException();
    }
  }

  private void rollbackNewAnnotation(Annotation annotation, boolean elasticRollback)
      throws FailedProcessingException {
    log.warn("Rolling back for annotations: {}", annotation);
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
      log.error("Unable to rollback creation for annotations {}", annotation.getOdsId(), e);
    }
  }

  private void filterUpdatesAndUpdateHandleRecord(Annotation currentAnnotation,
      Annotation annotation) throws PidCreationException {
    if (!fdoRecordService.handleNeedsUpdate(currentAnnotation, annotation)) {
      return;
    }
    var requestBody = fdoRecordService.buildPatchRollbackHandleRequest(annotation
    );
    handleComponent.updateHandle(requestBody);
  }

  private void filterUpdatesAndRollbackHandleUpdateRecord(Annotation currentAnnotation,
      Annotation annotation) {
    if (!fdoRecordService.handleNeedsUpdate(currentAnnotation, annotation)) {
      return;
    }
    var requestBody = fdoRecordService.buildPatchRollbackHandleRequest(annotation
    );
    try {
      handleComponent.rollbackHandleUpdate(requestBody);
    } catch (PidCreationException e) {
      log.error("Unable to rollback handle update for annotations {}", currentAnnotation.getOdsId(),
          e);
    }
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
    try {
      repository.createAnnotationRecord(currentAnnotation);
    } catch (DataAccessException e){
      log.error("Fatal exception: unable to revert annotation {} to its original state", currentAnnotation.getOdsId(), e);
      throw new FailedProcessingException();
    }

    filterUpdatesAndRollbackHandleUpdateRecord(currentAnnotation, annotation);
    throw new FailedProcessingException();
  }

  private void indexElasticUpdatedAnnotation(Annotation annotation, Annotation currentAnnotation)
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
    } else {
      rollbackUpdatedAnnotation(currentAnnotation, annotation, false);
      throw new FailedProcessingException();
    }
  }

}
