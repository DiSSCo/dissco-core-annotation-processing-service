package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.configuration.ApplicationConfiguration.HANDLE_PROXY;

import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.component.AnnotationHasher;
import eu.dissco.annotationprocessingservice.domain.AutoAcceptedAnnotation;
import eu.dissco.annotationprocessingservice.domain.HashedAnnotationRequest;
import eu.dissco.annotationprocessingservice.domain.HashedAutoAcceptedAnnotationRequest;
import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.properties.FdoProperties;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.schema.Annotation;
import eu.dissco.annotationprocessingservice.schema.Annotation.OdsMergingDecisionStatus;
import eu.dissco.annotationprocessingservice.web.HandleComponent;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.jooq.exception.DataAccessException;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@Profile(Profiles.RABBIT_MQ_AUTO)
public class ProcessingAutoAcceptedService extends AbstractProcessingService {

  public ProcessingAutoAcceptedService(
      AnnotationRepository repository,
      ElasticSearchRepository elasticRepository,
      RabbitMqPublisherService rabbitMqPublisherService, FdoRecordService fdoRecordService,
      HandleComponent handleComponent,
      ApplicationProperties applicationProperties,
      AnnotationValidatorService annotationValidator,
      MasJobRecordService masJobRecordService, BatchAnnotationService batchAnnotationService,
      AnnotationBatchRecordService annotationBatchRecordService, FdoProperties fdoProperties,
      RollbackService rollbackService,
      AnnotationHasher annotationHasher) {
    super(repository, elasticRepository, rabbitMqPublisherService, fdoRecordService,
        handleComponent,
        applicationProperties, annotationValidator, masJobRecordService, batchAnnotationService,
        annotationBatchRecordService, fdoProperties, rollbackService,
        annotationHasher);
  }

  private static void addMergingInformation(AutoAcceptedAnnotation autoAcceptedAnnotation,
      Annotation annotation) {
    annotation.setOdsMergingStateChangeDate(Date.from(Instant.now()));
    annotation.setOdsHasMergingStateChangedBy(autoAcceptedAnnotation.acceptingAgent());
  }

  public void handleMessage(List<AutoAcceptedAnnotation> autoAcceptedAnnotations)
      throws FailedProcessingException, AnnotationValidationException {
    log.debug("Processing auto-accepted annotation: {}", autoAcceptedAnnotations);
    annotationValidator.validateAnnotationRequest(autoAcceptedAnnotations.stream()
        .map(AutoAcceptedAnnotation::annotation).toList(), true);
    var hashedAnnotations = autoAcceptedAnnotations.stream()
        .map(annotation -> new HashedAutoAcceptedAnnotationRequest(
            annotation.acceptingAgent(),
            new HashedAnnotationRequest(annotation.annotation(),
                hashAnnotation(annotation.annotation()))))
        .collect(Collectors.toSet());
    var ids = postHandles(
        hashedAnnotations.stream().map(HashedAutoAcceptedAnnotationRequest::hashedRequest).toList(),
        null, false);
    var annotations = autoAcceptedAnnotations.stream().map(autoAcceptedAnnotation ->
            buildAutoAcceptedAnnotation(autoAcceptedAnnotation, ids))
        .toList();
    log.info("New ids have been generated for {} Annotations", ids.size());
    try {
      repository.createAnnotationRecords(annotations);
    } catch (DataAccessException e) {
      log.error("Unable to post new Annotation to DB", e);
      rollbackService.rollbackNewAnnotations(annotations, false, false);
      throw new FailedProcessingException();
    }
    log.info("Annotations have been successfully committed to database");
    indexElasticNewAnnotations(annotations);
    log.info("Annotations have been successfully indexed in elastic");
  }

  private Annotation buildAutoAcceptedAnnotation(AutoAcceptedAnnotation autoAcceptedAnnotation,
      Map<UUID, String> ids) {
    var id = HANDLE_PROXY + ids.get(hashAnnotation(autoAcceptedAnnotation.annotation()));
    var annotation = buildAnnotation(autoAcceptedAnnotation.annotation(), id, 1, null, true);
    addMergingInformation(autoAcceptedAnnotation, annotation);
    return annotation;
  }

}
