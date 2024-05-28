package eu.dissco.annotationprocessingservice.service;

import co.elastic.clients.elasticsearch._types.Result;
import eu.dissco.annotationprocessingservice.component.SchemaValidatorComponent;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.domain.annotation.Generator;
import eu.dissco.annotationprocessingservice.exception.BatchingException;
import eu.dissco.annotationprocessingservice.exception.ConflictException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.web.HandleComponent;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import java.util.UUID;


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

  private void enrichNewAnnotation(Annotation annotation, String id) {
    annotation.setOdsId(id)
        .setOdsVersion(1)
        .setAsGenerator(createGenerator())
        .setOaGenerated(Instant.now());
  }

  protected void enrichNewAnnotation(Annotation annotation, String id, AnnotationEvent event,
      Optional<Map<String, UUID>> batchIds) {
    enrichNewAnnotation(annotation, id);
    annotation.setOdsJobId(applicationProperties.getHandleProxy() + event.jobId());
    batchIds.ifPresentOrElse(idMap -> annotation.setOdsBatchId(idMap.get(id)),
        () -> {
          if (event.batchId() != null) {
            annotation.setOdsBatchId(event.batchId());
          }
        });
  }

  private Generator createGenerator() {
    return Generator.builder()
        .odsId(applicationProperties.getProcessorHandle())
        .foafName("Annotation Processing Service")
        .odsType("oa:SoftwareAgent")
        .build();
  }

  protected void enrichUpdateAnnotation(Annotation annotation, Annotation currentAnnotation) {
    annotation.setOdsId(currentAnnotation.getOdsId())
        .setOdsVersion(currentAnnotation.getOdsVersion() + 1)
        .setOaGenerated(currentAnnotation.getOaGenerated())
        .setAsGenerator(currentAnnotation.getAsGenerator())
        .setOaCreator(currentAnnotation.getOaCreator())
        .setDcTermsCreated(currentAnnotation.getDcTermsCreated());
  }

  protected void enrichUpdateAnnotation(Annotation annotation, Annotation currentAnnotation,
      String jobId) {
    enrichUpdateAnnotation(annotation, currentAnnotation);
    annotation.setOdsJobId(jobId);
  }

  protected static boolean annotationsAreEqual(Annotation currentAnnotation,
      Annotation annotation) {
    return currentAnnotation.getOaBody().equals(annotation.getOaBody())
        && currentAnnotation.getOaCreator().equals(annotation.getOaCreator())
        && currentAnnotation.getOaTarget().equals(annotation.getOaTarget())
        && (currentAnnotation.getOaMotivatedBy() != null && (currentAnnotation.getOaMotivatedBy()
        .equals(annotation.getOaMotivatedBy()))
        || (currentAnnotation.getOaMotivatedBy() == null && annotation.getOaMotivatedBy() == null))
        && (currentAnnotation.getOdsAggregateRating() != null
        && currentAnnotation.getOdsAggregateRating().equals(annotation.getOdsAggregateRating())
        || (currentAnnotation.getOdsAggregateRating() == null
        && annotation.getOdsAggregateRating() == null))
        && currentAnnotation.getOaMotivation().equals(annotation.getOaMotivation());
  }

  protected List<String> processEqualAnnotations(Set<Annotation> currentAnnotations) {
    if (currentAnnotations.isEmpty()) {
      return Collections.emptyList();
    }
    var idList = currentAnnotations.stream().map(Annotation::getOdsId).toList();
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
    if (event.batchId() != null) { // This is a batchResult
      annotationBatchRecordService.updateAnnotationBatchRecord(event.batchId(),
          newAnnotations.size());
      return;
    }
    if (event.batchMetadata() != null) {
      // New annotation event with processed annotations because we need the ids of the parent annotations for batching
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
