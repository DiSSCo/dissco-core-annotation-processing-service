package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.configuration.ApplicationConfiguration.HANDLE_PROXY;
import static eu.dissco.annotationprocessingservice.domain.AgentRoleType.PROCESSING_SERVICE;
import static eu.dissco.annotationprocessingservice.utils.HandleUtils.removeProxy;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.annotationprocessingservice.component.AnnotationValidatorComponent;
import eu.dissco.annotationprocessingservice.domain.ProcessedAnnotationBatch;
import eu.dissco.annotationprocessingservice.exception.BatchingException;
import eu.dissco.annotationprocessingservice.exception.ConflictException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.properties.FdoProperties;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.schema.Agent;
import eu.dissco.annotationprocessingservice.schema.Agent.Type;
import eu.dissco.annotationprocessingservice.schema.Annotation;
import eu.dissco.annotationprocessingservice.schema.Annotation.OaMotivation;
import eu.dissco.annotationprocessingservice.schema.Annotation.OdsStatus;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingEvent;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;
import eu.dissco.annotationprocessingservice.schema.Identifier.DctermsType;
import eu.dissco.annotationprocessingservice.schema.TombstoneMetadata;
import eu.dissco.annotationprocessingservice.utils.AgentUtils;
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
  protected final AnnotationValidatorComponent schemaValidator;
  protected final MasJobRecordService masJobRecordService;
  protected final BatchAnnotationService batchAnnotationService;
  protected final AnnotationBatchRecordService annotationBatchRecordService;
  protected final FdoProperties fdoProperties;

  protected static boolean annotationsAreEqual(Annotation currentAnnotation,
      Annotation annotation) {
    return currentAnnotation.getOaHasBody().equals(annotation.getOaHasBody())
        && currentAnnotation.getDctermsCreator().equals(annotation.getDctermsCreator())
        && currentAnnotation.getOaHasTarget().equals(annotation.getOaHasTarget())
        && (currentAnnotation.getOaMotivatedBy() != null && (currentAnnotation.getOaMotivatedBy()
        .equals(annotation.getOaMotivatedBy()))
        || (currentAnnotation.getOaMotivatedBy() == null && annotation.getOaMotivatedBy() == null))
        && (currentAnnotation.getOdsHasAggregateRating() != null
        && currentAnnotation.getOdsHasAggregateRating()
        .equals(annotation.getOdsHasAggregateRating())
        || (currentAnnotation.getOdsHasAggregateRating() == null
        && annotation.getOdsHasAggregateRating() == null))
        && currentAnnotation.getOaMotivation().equals(annotation.getOaMotivation());
  }

  protected Annotation buildAnnotation(AnnotationProcessingRequest annotationRequest, String id,
      int version, String jobId) {
    var timestamp = Instant.now();
    return new Annotation()
        .withId(id)
        .withDctermsIdentifier(id)
        .withType("ods:Annotation")
        .withOdsFdoType(fdoProperties.getType())
        .withOdsVersion(version)
        .withOdsStatus(OdsStatus.ACTIVE)
        .withOaMotivation(OaMotivation.fromValue(annotationRequest.getOaMotivation().value()))
        .withOaMotivatedBy(annotationRequest.getOaMotivatedBy())
        .withOaHasTarget(annotationRequest.getOaHasTarget())
        .withOaHasBody(annotationRequest.getOaHasBody())
        .withDctermsCreator(annotationRequest.getDctermsCreator())
        .withDctermsCreated(annotationRequest.getDctermsCreated())
        .withAsGenerator(createGenerator())
        .withDctermsIssued(Date.from(timestamp))
        .withDctermsModified(Date.from(timestamp))
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
    return AgentUtils.createMachineAgent(applicationProperties.getProcessorName(),
        applicationProperties.getProcessorHandle(), PROCESSING_SERVICE,
        DctermsType.DOI.value(), Type.SCHEMA_SOFTWARE_APPLICATION);
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

  public void archiveAnnotation(Annotation currentAnnotation, Agent tombstoningAgent)
      throws IOException, FailedProcessingException {
    log.info("Archive annotations: {} in handle service", currentAnnotation.getId());
    var handle = removeProxy(currentAnnotation);
    var requestBody = fdoRecordService.buildTombstoneHandleRequest(handle);
    try {
      handleComponent.archiveHandle(requestBody, handle);
    } catch (PidCreationException e) {
      log.error("Unable to archive annotations in handle system for annotations {}",
          currentAnnotation.getId(), e);
      throw new FailedProcessingException();
    }
    log.info("Removing annotations: {} from indexing service", currentAnnotation.getId());
    var document = elasticRepository.archiveAnnotation(currentAnnotation.getId());
    if (document.result().equals(Result.Deleted) || document.result().equals(Result.NotFound)) {
      log.info("Archive annotations: {} in database", currentAnnotation.getId());
      var timestamp = Instant.now();
      var tombstoneAnnotation = buildTombstoneAnnotation(currentAnnotation, tombstoningAgent,
          timestamp);
      repository.archiveAnnotation(tombstoneAnnotation);
      log.info("Sending Tombstone event to provenance servie");
      kafkaService.publishTombstoneEvent(tombstoneAnnotation, currentAnnotation);
      log.info("Archived annotations: {}", currentAnnotation.getId());
    } else {
      log.error("Unable to archive annotation in Elastic. Result is {}", document.result());
    }
  }

  private Annotation buildTombstoneAnnotation(Annotation annotation,
      Agent tombstoningAgent, Instant timestamp) {
    return new Annotation()
        .withId(annotation.getId())
        .withType(annotation.getType())
        .withDctermsIdentifier(annotation.getDctermsIdentifier())
        .withOdsStatus(OdsStatus.TOMBSTONE)
        .withOdsJobID(annotation.getOdsJobID())
        .withOdsFdoType(annotation.getOdsFdoType())
        .withOdsVersion(annotation.getOdsVersion() + 1)
        .withOaMotivation(annotation.getOaMotivation())
        .withOaMotivatedBy(annotation.getOaMotivatedBy())
        .withOaHasTarget(annotation.getOaHasTarget())
        .withOaHasBody(annotation.getOaHasBody())
        .withDctermsCreator(annotation.getDctermsCreator())
        .withDctermsCreated(annotation.getDctermsCreated())
        .withDctermsModified(Date.from(timestamp))
        .withDctermsIssued(annotation.getDctermsIssued())
        .withAsGenerator(annotation.getAsGenerator())
        .withOdsHasAggregateRating(annotation.getOdsHasAggregateRating())
        .withOdsBatchID(annotation.getOdsBatchID())
        .withOdsPlaceInBatch(annotation.getOdsPlaceInBatch())
        .withOdsHasTombstoneMetadata(new TombstoneMetadata()
            .withType("ods:Tombstone")
            .withOdsTombstoneDate(Date.from(timestamp))
            .withOdsTombstoneText("This annotation was archived")
            .withOdsHasAgents(List.of(tombstoningAgent)));
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
