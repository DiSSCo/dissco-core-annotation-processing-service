package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.configuration.ApplicationConfiguration.HANDLE_PROXY;

import co.elastic.clients.elasticsearch._types.Result;
import eu.dissco.annotationprocessingservice.component.SchemaValidatorComponent;
import eu.dissco.annotationprocessingservice.domain.AnnotationProcessingEvent;
import eu.dissco.annotationprocessingservice.domain.BatchMetadata;
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
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;
import eu.dissco.annotationprocessingservice.schema.OaHasBody;
import eu.dissco.annotationprocessingservice.schema.OaHasBody__1;
import eu.dissco.annotationprocessingservice.schema.OaHasSelector__1;
import eu.dissco.annotationprocessingservice.schema.OaHasTarget;
import eu.dissco.annotationprocessingservice.schema.OaHasTarget__1;
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
        && currentAnnotation.getSchemaAggregateRating().equals(annotation.getSchemaAggregateRating())
        || (currentAnnotation.getSchemaAggregateRating() == null
        && annotation.getSchemaAggregateRating() == null))
        && currentAnnotation.getOaMotivation().equals(annotation.getOaMotivation());
  }

  private static OaHasSelector__1 buildSelector(OaHasTarget oaHasTarget) {
    var selector = new OaHasSelector__1();
    for (var entry : oaHasTarget.getOaHasSelector()
        .getAdditionalProperties().entrySet()) {
      selector.setAdditionalProperty(entry.getKey(), entry.getValue());
    }
    return selector;
  }

  protected Annotation buildAnnotation(AnnotationProcessingRequest annotationRequest, String id, int version) {
    return new Annotation()
        .withId(id)
        .withOdsID(id)
        .withType("ods:Annotation")
        .withRdfType("ods:Annotation")
        .withOdsVersion(version)
        .withOdsStatus(OdsStatus.ODS_ACTIVE)
        .withOaMotivation(OaMotivation.fromValue(annotationRequest.getOaMotivation().value()))
        .withOaMotivatedBy(annotationRequest.getOaMotivatedBy())
        .withOaHasTarget(buildTarget(annotationRequest.getOaHasTarget()))
        .withOaHasBody(buildBody(annotationRequest.getOaHasBody()))
        .withDctermsCreator(annotationRequest.getDctermsCreator())
        .withDctermsCreated(annotationRequest.getDctermsCreated())
        .withAsGenerator(createGenerator())
        .withDctermsIssued(Date.from(Instant.now()))
        .withDctermsModified(Date.from(Instant.now()))
        .withOdsPlaceInBatch(annotationRequest.getOdsPlaceInBatch())
        .withOdsBatchID(annotationRequest.getOdsBatchID());
  }

  private OaHasTarget__1 buildTarget(OaHasTarget oaHasTarget) {
    return new OaHasTarget__1()
        .withId(oaHasTarget.getId())
        .withOdsID(oaHasTarget.getOdsID())
        .withType(oaHasTarget.getType())
        .withOdsType(oaHasTarget.getOdsType())
        .withOaHasSelector(buildSelector(oaHasTarget));
  }

  private OaHasBody__1 buildBody(OaHasBody annotationRequestBody) {
    return new OaHasBody__1()
        .withType(annotationRequestBody.getType())
        .withOaValue(annotationRequestBody.getOaValue())
        .withDctermsReferences(annotationRequestBody.getDctermsReferences())
        .withOdsScore(annotationRequestBody.getOdsScore());
  }

  protected Annotation buildAnnotation(AnnotationProcessingRequest annotationRequest, String id,
      boolean batchingRequested, int version) {
    var annotation = buildAnnotation(annotationRequest, id, version);
    if (batchingRequested) {
      annotationBatchRecordService.mintBatchId(annotation);
      annotation.setOdsPlaceInBatch(1);
    }
    return annotation;
  }

  protected Annotation buildAnnotation(AnnotationProcessingRequest annotationRequest, String id,
      String batchId, int version) {
    var annotation = buildAnnotation(annotationRequest, id, version);
    if (batchId != null) {
      annotation.setOdsBatchID(UUID.fromString(batchId));
    }
    return annotation;
  }

  protected Annotation buildAnnotation(AnnotationProcessingRequest annotationRequest, String id,
      AnnotationProcessingEvent event, int version) {
    var annotation = buildAnnotation(annotationRequest, id, version);
    annotation.setOdsJobID(HANDLE_PROXY + event.jobId());
    return annotation;
  }

  protected void addBatchIds(Annotation annotation, Optional<Map<String, UUID>> batchIds,
      AnnotationProcessingEvent event) {
    batchIds.ifPresentOrElse(idMap -> annotation.setOdsBatchID(idMap.get(annotation.getId())),
        () -> {
          if (event.batchId() != null) {
            annotation.setOdsBatchID(event.batchId());
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
      List<Annotation> newAnnotations)
      throws BatchingException, ConflictException {
    if (newAnnotations.isEmpty()) {
      return;
    }

    if (event.batchId() != null) { // This is a batchResult
      annotationBatchRecordService.updateAnnotationBatchRecord(event.batchId(),
          newAnnotations.size());
      return;
    }
    if (event.batchMetadata() != null) {
      // New hashedAnnotation event with processed annotations because we need the ids of the parent annotations for batching
      var processedEvent = new BatchMetadata(newAnnotations, event.jobId(),
          event.batchMetadata(), null);
      try {
        batchAnnotationService.applyBatchAnnotations(processedEvent);
      } catch (IOException e) {
        log.error("An error with elastic has occurred", e);
        throw new BatchingException();
      }
    } else {
      log.warn(
          "User requested batchingRequested, but MAS did not provide batch metadata. JobId: {}",
          event.jobId());
    }
  }

}
