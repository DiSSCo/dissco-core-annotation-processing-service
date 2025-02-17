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
@Profile(Profiles.KAFKA_AUTO)
public class ProcessingAutoAcceptedService extends AbstractProcessingService {

  public ProcessingAutoAcceptedService(
      AnnotationRepository repository,
      ElasticSearchRepository elasticRepository,
      KafkaPublisherService kafkaService, FdoRecordService fdoRecordService,
      HandleComponent handleComponent,
      ApplicationProperties applicationProperties,
      AnnotationValidatorComponent schemaValidator,
      MasJobRecordService masJobRecordService, BatchAnnotationService batchAnnotationService,
      AnnotationBatchRecordService annotationBatchRecordService, FdoProperties fdoProperties, RollbackService rollbackService) {
    super(repository, elasticRepository, kafkaService, fdoRecordService, handleComponent,
        applicationProperties, schemaValidator, masJobRecordService, batchAnnotationService,
        annotationBatchRecordService, fdoProperties, rollbackService);
  }

  private static void addMergingInformation(AutoAcceptedAnnotation autoAcceptedAnnotation,
      Annotation annotation) {
    annotation.setOdsMergingDecisionStatus(OdsMergingDecisionStatus.APPROVED);
    annotation.setOdsMergingStateChangeDate(Date.from(Instant.now()));
    annotation.setOdsHasMergingStateChangedBy(autoAcceptedAnnotation.acceptingAgent());
  }

  public void handleMessage(AutoAcceptedAnnotation autoAcceptedAnnotation)
      throws FailedProcessingException {
    log.debug("Processing auto-accepted annotations: {}", autoAcceptedAnnotation);
    var ids = postHandles(autoAcceptedAnnotation.annotations());
    var annotations = autoAcceptedAnnotation.annotations().stream().map(annotation ->
        buildAnnotation(annotation, HANDLE_PROXY + ids.get(annotation.getOaHasTarget().getId()), 1,
        null)).toList();
    var idList = ids.values().stream().toList();
    annotations.forEach(annotation -> addMergingInformation(autoAcceptedAnnotation, annotation));
    log.info("New id has been generated for {} Annotations", ids.size());
    try {
      repository.createAnnotationRecords(annotations);
    } catch (DataAccessException e) {
      log.error("Unable to post new Annotation to DB", e);
      rollbackService.rollbackNewAnnotations(annotations, false, false);
      throw new FailedProcessingException();
    }
    log.info("Annotations have been successfully committed to database");
    indexElasticNewAnnotations(annotations, idList);
    log.info("Annotation annotations have been successfully indexed in elastic");
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
