package eu.dissco.annotationprocessingservice.service;

import co.elastic.clients.elasticsearch._types.Result;
import eu.dissco.annotationprocessingservice.component.SchemaValidatorComponent;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.domain.annotation.Generator;
import eu.dissco.annotationprocessingservice.domain.annotation.Target;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.web.HandleComponent;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
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
        .withOdsType("oa:SoftwareAgent");
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

  protected void applyBatchAnnotations(AnnotationEvent annotationEvent)
      throws IOException {
    boolean moreBatching = true;
    int pageNumber = 1;
    var pageSizePlusOne = applicationProperties.getBatchPageSize() + 1;
    var targetType = annotationEvent.annotations().get(0).getOaTarget().getOdsType();
    while (moreBatching) {
      var targetIds = elasticRepository.searchByBatchMetadata(targetType,
          annotationEvent.batchMetadata(), pageNumber, pageSizePlusOne);
      if (targetIds.size() <= applicationProperties.getBatchPageSize()) {
        moreBatching = false;
      }
      log.info("Successfully identified {} {}s to apply batch annotations to", targetIds.size(),
          targetType);
      var annotations = generateBatchAnnotations(annotationEvent.annotations(), targetIds);
      annotations = moreBatching ? annotations.subList(pageNumber,
          applicationProperties.getBatchPageSize()) : annotations;
      var batchEvent = new AnnotationEvent(annotations, annotationEvent.jobId(), null);
      kafkaService.publishBatchAnnotation(batchEvent);
      log.info("Successfully published {} batch annotations to queue", targetIds.size());
      pageNumber = pageNumber + 1;

    }
  }

  private List<Annotation> generateBatchAnnotations(List<Annotation> baseAnnotations,
      List<String> targetIds) {
    var batchAnnotations = new ArrayList<Annotation>();
    for (var baseAnnotation : baseAnnotations) {
      batchAnnotations.addAll(
          targetIds.stream().map(targetId -> copyAnnotation(baseAnnotation, targetId)).toList());
    }
    return batchAnnotations;
  }

  private Annotation copyAnnotation(Annotation annotation, String targetId) {
    return new Annotation()
        .withOdsId(null)
        .withOdsJobId(annotation.getOdsJobId())
        .withOaMotivation(annotation.getOaMotivation())
        .withDcTermsCreated(annotation.getDcTermsCreated())
        .withOaCreator(annotation.getOaCreator())
        .withOaBody(annotation.getOaBody())
        .withOdsAggregateRating(annotation.getOdsAggregateRating())
        .withOaTarget(
            new Target()
                .withOdsId(targetId)
                .withSelector(annotation.getOaTarget().getOaSelector())
                .withOdsType(annotation.getOaTarget().getOdsType())
        );
  }

}
