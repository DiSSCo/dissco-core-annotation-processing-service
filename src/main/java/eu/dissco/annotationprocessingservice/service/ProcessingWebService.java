package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.configuration.ApplicationConfiguration.HANDLE_PROXY;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.component.SchemaValidatorComponent;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingEvent;
import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import eu.dissco.annotationprocessingservice.exception.BatchingException;
import eu.dissco.annotationprocessingservice.exception.ConflictException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.NotFoundException;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.schema.Annotation;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;
import eu.dissco.annotationprocessingservice.web.HandleComponent;
import java.io.IOException;
import java.util.List;
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
      MasJobRecordService masJobRecordService, BatchAnnotationService batchAnnotationService,
      AnnotationBatchRecordService annotationBatchRecordService) {
    super(repository, elasticRepository, kafkaService, fdoRecordService, handleComponent,
        applicationProperties, schemaValidator, masJobRecordService, batchAnnotationService,
        annotationBatchRecordService);
  }

  public Annotation persistNewAnnotation(AnnotationProcessingRequest annotationRequest,
      boolean batchingRequested) throws FailedProcessingException, AnnotationValidationException {
    schemaValidator.validateAnnotationRequest(annotationRequest, true);
    var id = postHandle(annotationRequest);
    var annotation = buildAnnotation(annotationRequest, HANDLE_PROXY + id, batchingRequested, 1);
    if (batchingRequested) {
      annotationBatchRecordService.mintBatchId(annotation);
    }
    log.info("New id has been generated for Annotation: {}", annotation.getId());
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

  public void batchWebAnnotations(AnnotationProcessingEvent event, Annotation result) {
    log.info("Batching annotations for web hashedAnnotation {}", result);
    try {
      applyBatchAnnotations(event, List.of(result));
    } catch (ConflictException | BatchingException e) {
      log.error(
          "An exception has occurred while creating batch annotations for parent hashedAnnotation {}",
          result.getId(), e);
    }
  }

  public Annotation updateAnnotation(AnnotationProcessingRequest annotationRequest)
      throws FailedProcessingException, NotFoundException, AnnotationValidationException {
    schemaValidator.validateAnnotationRequest(annotationRequest, false);
    var currentAnnotationOptional = repository.getAnnotationForUser(annotationRequest.getId(),
        annotationRequest.getDctermsCreator().getId());
    if (currentAnnotationOptional.isEmpty()) {
      log.error("No annotations with id {} found for creator {}", annotationRequest.getId(),
          annotationRequest.getDctermsCreator().getId());
      throw new NotFoundException(annotationRequest.getId(),
          annotationRequest.getDctermsCreator().getId());
    }
    var currentAnnotation = currentAnnotationOptional.get();
    var annotation = buildAnnotation(annotationRequest, annotationRequest.getId(),
        currentAnnotation.getOdsVersion() + 1, null);
    if (annotationsAreEqual(currentAnnotation, annotation)) {
      processEqualAnnotations(Set.of(currentAnnotation));
      return currentAnnotation;
    }
    try {
      filterUpdatesAndUpdateHandleRecord(currentAnnotation, annotation);
    } catch (PidCreationException e) {
      log.error("Unable to post update for annotations {}", currentAnnotation.getId(), e);
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
        currentAnnotation.getId());
    indexElasticUpdatedAnnotation(annotation, currentAnnotation);
    return annotation;
  }



  private void filterUpdatesAndUpdateHandleRecord(Annotation currentAnnotation,
      Annotation annotation) throws PidCreationException {
    if (!fdoRecordService.handleNeedsUpdate(currentAnnotation, annotation)) {
      return;
    }
    var requestBody = fdoRecordService.buildPatchRollbackHandleRequest(annotation);
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
      log.error("Unable to rollback handle update for annotations {}", currentAnnotation.getId(),
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
    } catch (DataAccessException e) {
      log.error("Fatal exception: unable to revert hashedAnnotation {} to its original state",
          currentAnnotation.getId(), e);
      throw new FailedProcessingException();
    }

    filterUpdatesAndRollbackHandleUpdateRecord(currentAnnotation, annotation);
    throw new FailedProcessingException();
  }

  private void indexElasticUpdatedAnnotation(Annotation annotation, Annotation currentAnnotation)
      throws FailedProcessingException {
    IndexResponse indexDocument;
    try {
      indexDocument = elasticRepository.indexAnnotation(annotation);
    } catch (IOException | ElasticsearchException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      rollbackUpdatedAnnotation(currentAnnotation, annotation, false);
      throw new FailedProcessingException();
    }
    if (indexDocument.result().equals(Result.Updated)) {
      log.info("Annotation: {} has been successfully indexed", currentAnnotation.getId());
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
