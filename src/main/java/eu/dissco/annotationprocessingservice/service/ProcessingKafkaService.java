package eu.dissco.annotationprocessingservice.service;

import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.NotFoundException;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.web.HandleComponent;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@Profile(Profiles.KAFKA)
public class ProcessingKafkaService extends AbstractProcessingService {

  private final MasJobRecordService masJobRecordService;

  public ProcessingKafkaService(AnnotationRepository repository,
      ElasticSearchRepository elasticRepository,
      KafkaPublisherService kafkaService, FdoRecordService fdoRecordService,
      HandleComponent handleComponent, ApplicationProperties applicationProperties,
      MasJobRecordService masJobRecordService) {
    super(repository, elasticRepository, kafkaService, fdoRecordService, handleComponent,
        applicationProperties);
    this.masJobRecordService = masJobRecordService;
  }

  private static boolean annotationAreEqual(Annotation currentAnnotation, Annotation annotation) {
    return currentAnnotation.getOaBody().equals(annotation.getOaBody())
        && currentAnnotation.getOaCreator().equals(annotation.getOaCreator())
        && currentAnnotation.getOaTarget().equals(annotation.getOaTarget()) &&
        (currentAnnotation.getOaMotivatedBy() != null
            && currentAnnotation.getOaMotivatedBy().equals(annotation.getOaMotivatedBy())
            || currentAnnotation.getOaMotivatedBy() == null
            && annotation.getOaMotivatedBy() == null)
        && currentAnnotation.getOdsAggregateRating().equals(annotation.getOdsAggregateRating())
        && currentAnnotation.getOaMotivation().equals(annotation.getOaMotivation());
  }

  public Annotation updateAnnotation(Annotation annotation)
      throws FailedProcessingException, NotFoundException {
    var currentAnnotation = repository.getAnnotation(annotation.getOdsId());
    if (currentAnnotation == null) {
      throw new NotFoundException(annotation.getOdsId());
    }
    return updateExistingAnnotation(currentAnnotation, new AnnotationEvent(annotation, null));
  }

  public Annotation createNewAnnotation(Annotation annotation) throws FailedProcessingException {
    log.info("Received create annotation request of: {}", annotation);
    return persistNewAnnotation(new AnnotationEvent(annotation, null));
  }

  public void handleMessage(AnnotationEvent event)
      throws DataBaseException, FailedProcessingException {
    log.info("Received annotation event of: {}", event);
    masJobRecordService.verifyMasJobId(event);
    var annotation = event.annotation();
    var currentAnnotationOptional = getExistingAnnotation(event);
    if (currentAnnotationOptional.isPresent()) {
      var currentAnnotation = currentAnnotationOptional.get();
      if (annotationAreEqual(currentAnnotation, annotation)) {
        log.info("Received annotation is equal to annotation: {}", currentAnnotation.getOdsId());
        processEqualAnnotation(currentAnnotation);
      } else {
        log.info("Annotation with id: {} has received an update", currentAnnotation.getOdsId());
        updateExistingAnnotation(currentAnnotation, event);
      }
    } else {
      persistNewAnnotation(event);
    }
  }

  private Optional<Annotation> getExistingAnnotation(AnnotationEvent event)
      throws FailedProcessingException {
    var annotation = event.annotation();
    var existingAnnotations = repository.getAnnotation(annotation);
    if (existingAnnotations.size() > 1) {
      log.error(
          "Multiple annotations exist with same motivation, target, creator as {}, and this is not a web request",
          annotation);
      masJobRecordService.markMasJobRecordAsFailed(event);
      throw new FailedProcessingException();
    }
    if (existingAnnotations.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(existingAnnotations.get(0));
  }

  private Annotation updateExistingAnnotation(Annotation currentAnnotation,
      AnnotationEvent event) throws FailedProcessingException {
    var annotation = event.annotation();
    try {
      filterUpdatesAndUpdateHandleRecord(currentAnnotation, annotation);
    } catch (PidCreationException e) {
      log.error("Unable to post update for annotation {}", currentAnnotation.getOdsId(), e);
      masJobRecordService.markMasJobRecordAsFailed(event);
      throw new FailedProcessingException();
    }
    enrichUpdateAnnotation(annotation, currentAnnotation);
    repository.createAnnotationRecord(annotation);
    log.info("Annotation: {} has been successfully committed to database",
        currentAnnotation.getOdsId());
    try {
      indexElasticUpdatedAnnotation(annotation, currentAnnotation);
    } catch (FailedProcessingException e) {
      masJobRecordService.markMasJobRecordAsFailed(event);
      throw new FailedProcessingException();
    }
    masJobRecordService.markMasJobRecordAsComplete(event.jobId(), annotation.getOdsId());
    return annotation;
  }

  private void processEqualAnnotation(Annotation currentAnnotation) {
    repository.updateLastChecked(currentAnnotation);
    log.info("Successfully updated lastChecked for existing annotation: {}",
        currentAnnotation.getOdsId());
  }

  private Annotation persistNewAnnotation(AnnotationEvent event) throws FailedProcessingException {
    var annotation = event.annotation();
    var id = postHandle(event);
    enrichNewAnnotation(annotation, id);
    log.info("New id has been generated for Annotation: {}", annotation.getOdsId());
    repository.createAnnotationRecord(annotation);
    log.info("Annotation: {} has been successfully committed to database", id);
    try {
      indexElasticNewAnnotation(annotation, id);
    } catch (FailedProcessingException e) {
      masJobRecordService.markMasJobRecordAsFailed(event);
      throw new FailedProcessingException();
    }
    masJobRecordService.markMasJobRecordAsComplete(event.jobId(), annotation.getOdsId());
    return annotation;
  }


  private String postHandle(AnnotationEvent event) throws FailedProcessingException {
    var annotation = event.annotation();
    var requestBody = fdoRecordService.buildPostHandleRequest(annotation);
    try {
      return handleComponent.postHandle(requestBody);
    } catch (PidCreationException e) {
      log.error("Unable to create handle for given annotation. ", e);
      masJobRecordService.markMasJobRecordAsFailed(event);
      throw new FailedProcessingException();
    }
  }

}
