package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.configuration.ApplicationConfiguration.HANDLE_PROXY;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.annotationprocessingservice.component.SchemaValidatorComponent;
import eu.dissco.annotationprocessingservice.domain.ProcessedAnnotationBatch;
import eu.dissco.annotationprocessingservice.exception.BatchingException;
import eu.dissco.annotationprocessingservice.exception.ConflictException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.schema.Agent;
import eu.dissco.annotationprocessingservice.schema.Agent.Type;
import eu.dissco.annotationprocessingservice.schema.Annotation;
import eu.dissco.annotationprocessingservice.schema.Annotation.OaMotivation;
import eu.dissco.annotationprocessingservice.schema.Annotation.OdsStatus;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingEvent;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;
import eu.dissco.annotationprocessingservice.web.HandleComponent;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
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
  protected final SchemaValidatorComponent schemaValidator;
  protected final MasJobRecordService masJobRecordService;
  protected final BatchAnnotationService batchAnnotationService;
  protected final AnnotationBatchRecordService annotationBatchRecordService;

  protected static boolean annotationsAreEqual(Annotation currentAnnotation,
      Annotation annotation) {
    return currentAnnotation.getOaHasBody().equals(annotation.getOaHasBody())
        && currentAnnotation.getDctermsCreator().equals(annotation.getDctermsCreator())
        && currentAnnotation.getOaHasTarget().equals(annotation.getOaHasTarget())
        && (currentAnnotation.getOaMotivatedBy() != null && (currentAnnotation.getOaMotivatedBy()
        .equals(annotation.getOaMotivatedBy()))
        || (currentAnnotation.getOaMotivatedBy() == null && annotation.getOaMotivatedBy() == null))
        && (currentAnnotation.getSchemaAggregateRating() != null
        && currentAnnotation.getSchemaAggregateRating()
        .equals(annotation.getSchemaAggregateRating())
        || (currentAnnotation.getSchemaAggregateRating() == null
        && annotation.getSchemaAggregateRating() == null))
        && currentAnnotation.getOaMotivation().equals(annotation.getOaMotivation());
  }

  protected Annotation buildAnnotation(AnnotationProcessingRequest annotationRequest, String id,
      int version, String jobId) {
    return new Annotation()
        .withId(id)
        .withOdsID(id)
        .withType("ods:Annotation")
        .withRdfType("ods:Annotation")
        .withOdsVersion(version)
        .withOdsStatus(OdsStatus.ODS_ACTIVE)
        .withOaMotivation(OaMotivation.fromValue(annotationRequest.getOaMotivation().value()))
        .withOaMotivatedBy(annotationRequest.getOaMotivatedBy())
        .withOaHasTarget(annotationRequest.getOaHasTarget())
        .withOaHasBody(annotationRequest.getOaHasBody())
        .withDctermsCreator(annotationRequest.getDctermsCreator())
        .withDctermsCreated(annotationRequest.getDctermsCreated())
        .withAsGenerator(createGenerator())
        .withDctermsIssued(Date.from(Instant.now()))
        .withDctermsModified(Date.from(Instant.now()))
        .withOdsJobID(jobId)
        .withOdsPlaceInBatch(annotationRequest.getOdsPlaceInBatch())
        .withOdsBatchID(annotationRequest.getOdsBatchID());
  }

  protected Annotation buildAnnotation(AnnotationProcessingRequest annotationRequest, String id,
      boolean batchingRequested, int version) {
    var annotation = buildAnnotation(annotationRequest, id, version, null);
    if (batchingRequested) {
      annotationBatchRecordService.mintBatchId(annotation);
      annotation.setOdsPlaceInBatch(1);
    }
    return annotation;
  }

  protected Annotation buildAnnotation(AnnotationProcessingRequest annotationRequest, String id,
      AnnotationProcessingEvent event, int version) {
    var annotation = buildAnnotation(annotationRequest, id, version, event.getJobId());
    annotation.setOdsJobID(HANDLE_PROXY + event.getJobId());
    return annotation;
  }

  protected void addBatchIds(Annotation annotation, Optional<Map<String, UUID>> batchIds,
      AnnotationProcessingEvent event) {
    batchIds.ifPresentOrElse(idMap -> annotation.setOdsBatchID(idMap.get(annotation.getId())),
        () -> {
          if (event.getBatchId() != null) {
            annotation.setOdsBatchID(event.getBatchId());
          }
        });
  }

  private Agent createGenerator() {
    return new Agent()
        .withId(applicationProperties.getProcessorHandle())
        .withType(Type.AS_APPLICATION)
        .withSchemaName("Annotation Processing Service");
  }

  protected List<String> processEqualAnnotations(Set<Annotation> currentAnnotations) {
    if (currentAnnotations.isEmpty()) {
      return Collections.emptyList();
    }
    var idList = currentAnnotations.stream().map(Annotation::getId).toList();
    repository.updateLastChecked(idList);
    log.info("Successfully updated lastChecked for existing annotations: {}", idList);
    return idList;
  }

  public void archiveAnnotation(String id) throws IOException, FailedProcessingException {
    if (repository.getAnnotationById(id).isPresent()) {
      log.info("Archive annotations: {} in handle service", id);
      var requestBody = fdoRecordService.buildArchiveHandleRequest(id);
      try {
        handleComponent.archiveHandle(requestBody, id);
      } catch (PidCreationException e) {
        log.error("Unable to archive annotations in handle system for annotations {}", id, e);
        throw new FailedProcessingException();
      }
      log.info("Removing annotations: {} from indexing service", id);
      var document = elasticRepository.archiveAnnotation(id);
      if (document.result().equals(Result.Deleted) || document.result().equals(Result.NotFound)) {
        log.info("Archive annotations: {} in database", id);
        repository.archiveAnnotation(id);
        log.info("Archived annotations: {}", id);
        log.info("Tombstoning PID record of annotations: {}", id);
      }
    } else {
      log.info("Annotation with id: {} is already archived", id);
    }
  }

  protected void applyBatchAnnotations(AnnotationProcessingEvent event,
      List<Annotation> newAnnotations) throws BatchingException, ConflictException {
    if (newAnnotations.isEmpty()) {
      return;
    }

    if (event.getBatchId() != null) { // This is a batchResult
      annotationBatchRecordService.updateAnnotationBatchRecord(event.getBatchId(),
          newAnnotations.size());
      return;
    }
    if (event.getBatchMetadata() != null) {
      // New hashedAnnotation event with processed annotations because we need the ids of the parent annotations for batching
      var processedEvent = new ProcessedAnnotationBatch(newAnnotations, event.getJobId(),
          event.getBatchMetadata(), null);
      try {
        batchAnnotationService.applyBatchAnnotations(processedEvent);
      } catch (IOException e) {
        log.error("An error with elastic has occurred", e);
        throw new BatchingException();
      }
    } else {
      log.warn(
          "User requested batchingRequested, but MAS did not provide batch metadata. JobId: {}",
          event.getJobId());
    }
  }

  protected String postHandle(AnnotationProcessingRequest annotationRequest)
      throws FailedProcessingException {
    var requestBody = fdoRecordService.buildPostHandleRequest(annotationRequest);
    try {
      return handleComponent.postHandle(requestBody).get(0);
    } catch (PidCreationException e) {
      log.error("Unable to create handle for given annotations. ", e);
      throw new FailedProcessingException();
    }
  }

  protected void rollbackHandleCreation(Annotation annotation) {
    var requestBody = fdoRecordService.buildRollbackCreationRequest(annotation);
    try {
      handleComponent.rollbackHandleCreation(requestBody);
    } catch (PidCreationException e) {
      log.error("Unable to rollback creation for annotations {}", annotation.getId(), e);
    }
  }

  protected void indexElasticNewAnnotation(Annotation annotation, String id)
      throws FailedProcessingException {
    IndexResponse indexDocument;
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
        elasticRepository.archiveAnnotation(annotation.getId());
      } catch (IOException | ElasticsearchException e) {
        log.info("Fatal exception, unable to rollback: {}", annotation.getId(), e);
      }
    }
    repository.rollbackAnnotation(annotation.getId());
    rollbackHandleCreation(annotation);
    throw new FailedProcessingException();
  }

}
