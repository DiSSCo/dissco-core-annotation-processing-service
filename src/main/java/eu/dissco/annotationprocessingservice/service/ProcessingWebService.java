package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.component.AnnotationValidatorComponent.validateAnnotationRequest;
import static eu.dissco.annotationprocessingservice.configuration.ApplicationConfiguration.HANDLE_PROXY;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.component.AnnotationValidatorComponent;
import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import eu.dissco.annotationprocessingservice.exception.BatchingException;
import eu.dissco.annotationprocessingservice.exception.ConflictException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.NotFoundException;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.properties.FdoProperties;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.schema.Annotation;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingEvent;
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
      ApplicationProperties applicationProperties, AnnotationValidatorComponent schemaValidator,
      MasJobRecordService masJobRecordService, BatchAnnotationService batchAnnotationService,
      AnnotationBatchRecordService annotationBatchRecordService, FdoProperties fdoProperties,
      RollbackService rollbackService) {
    super(repository, elasticRepository, kafkaService, fdoRecordService, handleComponent,
        applicationProperties, schemaValidator, masJobRecordService, batchAnnotationService,
        annotationBatchRecordService, fdoProperties, rollbackService);
  }

  public Annotation persistNewAnnotation(AnnotationProcessingRequest annotationRequest,
      boolean batchingRequested) throws FailedProcessingException, AnnotationValidationException {
    validateAnnotationRequest(annotationRequest, true);
    var id = postHandle(annotationRequest);
    var annotation = buildNewAnnotation(annotationRequest, HANDLE_PROXY + id, batchingRequested);
    if (batchingRequested) {
      annotationBatchRecordService.mintBatchId(annotation);
    }
    log.info("New id has been generated for Annotation: {}", annotation.getId());
    try {
      repository.createAnnotationRecord(annotation);
    } catch (DataAccessException e) {
      log.error("Unable to post new Annotation to DB", e);
      rollbackService.rollbackNewAnnotations(List.of(annotation), false, false);
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

  protected Annotation buildNewAnnotation(AnnotationProcessingRequest annotationRequest, String id,
      boolean batchingRequested) {
    var annotation = buildAnnotation(annotationRequest, id, 1, null);
    if (batchingRequested) {
      annotationBatchRecordService.mintBatchId(annotation);
      annotation.setOdsPlaceInBatch(1);
    }
    return annotation;
  }

  public Annotation updateAnnotation(AnnotationProcessingRequest annotationRequest)
      throws FailedProcessingException, NotFoundException, AnnotationValidationException {
    validateAnnotationRequest(annotationRequest, false);
    var currentAnnotationOptional = repository.getAnnotationForUser(annotationRequest.getId(),
        annotationRequest.getDctermsCreator().getId());
    if (currentAnnotationOptional.isEmpty()) {
      log.error("No annotations with id {} found for creator {}", annotationRequest.getId(),
          annotationRequest.getDctermsCreator().getId());
      throw new NotFoundException(annotationRequest.getId(),
          annotationRequest.getDctermsCreator().getId());
    }
    var currentAnnotation = currentAnnotationOptional.get();
    var annotation = buildAnnotation(annotationRequest, HANDLE_PROXY + annotationRequest.getId(),
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
      rollbackService.rollbackUpdatedAnnotation(currentAnnotation, annotation, false, false);
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
    var requestBody = fdoRecordService.buildPatchHandleRequest(annotation);
    handleComponent.updateHandle(requestBody);
  }

  private void indexElasticUpdatedAnnotation(Annotation annotation, Annotation currentAnnotation)
      throws FailedProcessingException {
    IndexResponse indexDocument;
    try {
      indexDocument = elasticRepository.indexAnnotation(annotation);
    } catch (IOException | ElasticsearchException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      rollbackService.rollbackUpdatedAnnotation(currentAnnotation, annotation, false, true);
      throw new FailedProcessingException();
    }
    if (indexDocument.result().equals(Result.Updated)) {
      log.info("Annotation: {} has been successfully indexed", currentAnnotation.getId());
      try {
        kafkaService.publishUpdateEvent(currentAnnotation, annotation);
      } catch (JsonProcessingException e) {
        rollbackService.rollbackUpdatedAnnotation(currentAnnotation, annotation, true, true);
        throw new FailedProcessingException();
      }
    } else {
      log.info("Elastic update failed. Rolling back annotation {}", currentAnnotation.getId());
      rollbackService.rollbackUpdatedAnnotation(currentAnnotation, annotation, false, true);
      throw new FailedProcessingException();
    }
  }

}
