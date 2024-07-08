package eu.dissco.annotationprocessingservice.service;

import co.elastic.clients.elasticsearch._types.Result;
import eu.dissco.annotationprocessingservice.component.SchemaValidatorComponent;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
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

  private void enrichNewAnnotation(Annotation annotation, String id) {
    var now = Date.from(Instant.now());
    annotation.withId(id)
        .withOdsVersion(1)
        .withAsGenerator(createGenerator())
        .withDctermsIssued(now)
        .withDctermsModified(now);
  }

  protected void enrichNewAnnotation(Annotation annotation, String id, boolean batchingRequested) {
    enrichNewAnnotation(annotation, id);
    if (batchingRequested) {
      annotationBatchRecordService.mintBatchId(annotation);
      annotation.setOdsPlaceInBatch(1);
    }
  }

  protected void enrichNewAnnotation(Annotation annotation, String id, AnnotationEvent event) {
    enrichNewAnnotation(annotation, id);
    annotation.setOdsJobID(applicationProperties.getHandleProxy() + event.jobId());
  }

  protected void addBatchIds(Annotation annotation, Optional<Map<String, UUID>> batchIds,
      AnnotationEvent event) {
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

  protected void enrichUpdateAnnotation(Annotation annotation, Annotation currentAnnotation) {
    annotation.withId(currentAnnotation.getId())
        .withOdsVersion(currentAnnotation.getOdsVersion() + 1)
        .withDctermsIssued(currentAnnotation.getDctermsIssued())
        .withAsGenerator(currentAnnotation.getAsGenerator())
        .withDctermsCreator(currentAnnotation.getDctermsCreator())
        .withDctermsCreated(currentAnnotation.getDctermsCreated())
        .withDctermsModified(Date.from(Instant.now()));
  }

  protected void enrichUpdateAnnotation(Annotation annotation, Annotation currentAnnotation,
      String jobId) {
    enrichUpdateAnnotation(annotation, currentAnnotation);
    annotation.setOdsJobID(jobId);
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

  protected void applyBatchAnnotations(AnnotationEvent event, List<Annotation> newAnnotations)
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
      var processedEvent = new AnnotationEvent(newAnnotations, event.jobId(),
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
