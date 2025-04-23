package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.configuration.ApplicationConfiguration.HANDLE_PROXY;

import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.component.AnnotationValidatorComponent;
import eu.dissco.annotationprocessingservice.domain.AutoAcceptedAnnotation;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.properties.FdoProperties;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.schema.Annotation;
import eu.dissco.annotationprocessingservice.schema.Annotation.OdsMergingDecisionStatus;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;
import eu.dissco.annotationprocessingservice.web.HandleComponent;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
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
      AnnotationValidatorComponent schemaValidator,
      MasJobRecordService masJobRecordService, BatchAnnotationService batchAnnotationService,
      AnnotationBatchRecordService annotationBatchRecordService, FdoProperties fdoProperties,
      RollbackService rollbackService) {
    super(repository, elasticRepository, rabbitMqPublisherService, fdoRecordService, handleComponent,
        applicationProperties, schemaValidator, masJobRecordService, batchAnnotationService,
        annotationBatchRecordService, fdoProperties, rollbackService);
  }

  private static void addMergingInformation(AutoAcceptedAnnotation autoAcceptedAnnotation,
      Annotation annotation) {
    annotation.setOdsMergingDecisionStatus(OdsMergingDecisionStatus.APPROVED);
    annotation.setOdsMergingStateChangeDate(Date.from(Instant.now()));
    annotation.setOdsHasMergingStateChangedBy(autoAcceptedAnnotation.acceptingAgent());
  }

  public void handleMessage(List<AutoAcceptedAnnotation> autoAcceptedAnnotations)
      throws FailedProcessingException {
    log.debug("Processing auto-accepted annotation: {}", autoAcceptedAnnotations);
    var ids = postHandles(
        autoAcceptedAnnotations.stream().map(AutoAcceptedAnnotation::annotation).toList());
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

  private Annotation buildAutoAcceptedAnnotation(AutoAcceptedAnnotation autoAcceptedAnnotation, Map<String, String> ids){
    var annotation =  buildAnnotation(autoAcceptedAnnotation.annotation(),
        HANDLE_PROXY + ids.get(autoAcceptedAnnotation.annotation().getOaHasTarget().getId()), 1,
        null);
    addMergingInformation(autoAcceptedAnnotation, annotation);
    return annotation;
  }

  protected Map<String, String> postHandles(List<AnnotationProcessingRequest> annotationRequest)
      throws FailedProcessingException {
    var requestBody = fdoRecordService.buildPostHandleRequest(annotationRequest);
    try {
      return handleComponent.postHandlesTargetPid(requestBody);
    } catch (PidCreationException e) {
      log.error("Unable to create handle for given annotations. ", e);
      throw new FailedProcessingException();
    }
  }

}
