package eu.dissco.annotationprocessingservice.service;

import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.ForbiddenException;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.web.HandleComponent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@Profile(Profiles.WEB)
public class ProcessingWebService extends AbstractProcessingService {

  public ProcessingWebService(AnnotationRepository repository,
      ElasticSearchRepository elasticRepository, KafkaPublisherService kafkaService,
      FdoRecordService fdoRecordService, HandleComponent handleComponent,
      ApplicationProperties applicationProperties) {
    super(repository, elasticRepository, kafkaService, fdoRecordService, handleComponent,
        applicationProperties);
  }

  public Annotation persistNewAnnotation(Annotation annotation) throws FailedProcessingException {
    var id = postHandle(annotation);
    enrichNewAnnotation(annotation, id);
    log.info("New id has been generated for Annotation: {}", annotation.getOdsId());
    repository.createAnnotationRecord(annotation);
    log.info("Annotation: {} has been successfully committed to database", id);
    indexElasticNewAnnotation(annotation, id);
    return annotation;
  }

  public Annotation updateAnnotation(Annotation annotation)
      throws FailedProcessingException, ForbiddenException {

    var currentAnnotationOptional = repository.getAnnotationForUser(annotation.getOdsId(),
        annotation.getOaCreator().getOdsId());
    if (currentAnnotationOptional.isEmpty()) {
      log.error("No annotations with id {} found for creator {}", annotation.getOdsId(),
          annotation.getOaCreator().getOdsId());
      throw new ForbiddenException(annotation.getOdsId(), annotation.getOaCreator().getOdsId());
    }
    var currentAnnotation = currentAnnotationOptional.get();
    enrichUpdateAnnotation(annotation, currentAnnotation);
    try {
      filterUpdatesAndUpdateHandleRecord(currentAnnotation, annotation);
    } catch (PidCreationException e) {
      log.error("Unable to post update for annotations {}", currentAnnotation.getOdsId(), e);
      throw new FailedProcessingException();
    }
    repository.createAnnotationRecord(annotation);
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

}
