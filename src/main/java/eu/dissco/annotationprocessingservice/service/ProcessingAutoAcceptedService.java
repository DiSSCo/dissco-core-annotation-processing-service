package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.configuration.ApplicationConfiguration.HANDLE_PROXY;

import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.component.AnnotationValidatorComponent;
import eu.dissco.annotationprocessingservice.domain.AutoAcceptedAnnotation;
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
import lombok.extern.slf4j.Slf4j;
import org.jooq.exception.DataAccessException;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@Profile(Profiles.KAFKA)
public class ProcessingAutoAcceptedService extends AbstractProcessingService {

  public ProcessingAutoAcceptedService(
      AnnotationRepository repository,
      ElasticSearchRepository elasticRepository,
      KafkaPublisherService kafkaService, FdoRecordService fdoRecordService,
      HandleComponent handleComponent,
      ApplicationProperties applicationProperties,
      AnnotationValidatorComponent schemaValidator,
      MasJobRecordService masJobRecordService, BatchAnnotationService batchAnnotationService,
      AnnotationBatchRecordService annotationBatchRecordService, FdoProperties fdoProperties) {
    super(repository, elasticRepository, kafkaService, fdoRecordService, handleComponent,
        applicationProperties, schemaValidator, masJobRecordService, batchAnnotationService,
        annotationBatchRecordService, fdoProperties);
  }

  private static void addMergingInformation(AutoAcceptedAnnotation autoAcceptedAnnotation,
      Annotation annotation) {
    annotation.setOdsMergingDecisionStatus(OdsMergingDecisionStatus.APPROVED);
    annotation.setOdsMergingStateChangeDate(Date.from(Instant.now()));
    annotation.setOdsHasMergingStateChangedBy(autoAcceptedAnnotation.acceptingAgent());
  }

  public void handleMessage(AutoAcceptedAnnotation autoAcceptedAnnotation)
      throws FailedProcessingException {
    log.info("Processing auto-accepted annotation: {}", autoAcceptedAnnotation);
    var id = postHandle(autoAcceptedAnnotation.annotation());
    var annotation = buildAnnotation(autoAcceptedAnnotation.annotation(), HANDLE_PROXY + id, 1, null);
    addMergingInformation(autoAcceptedAnnotation, annotation);
    log.info("New id has been generated for Annotation: {}", annotation.getId());
    try {
      repository.createAnnotationRecord(annotation);
    } catch (DataAccessException e) {
      log.error("Unable to post new Annotation to DB", e);
      rollbackHandleCreation(annotation);
      throw new FailedProcessingException();
    }
    log.info("Annotation: {} has been successfully committed to database", id);
    indexElasticNewAnnotation(annotation, id);
  }

}
