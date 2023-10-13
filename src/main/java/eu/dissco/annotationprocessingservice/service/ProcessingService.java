package eu.dissco.annotationprocessingservice.service;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.domain.Annotation;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.domain.AnnotationRecord;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.web.HandleComponent;
import java.io.IOException;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProcessingService {

  private static final int SUCCESS = 1;

  private final ObjectMapper mapper;
  private final AnnotationRepository repository;
  private final ElasticSearchRepository elasticRepository;
  private final KafkaPublisherService kafkaService;
  private final FdoRecordService fdoRecordService;
  private final HandleComponent handleComponent;
  private final Environment environment;

  private static boolean annotationAreEqual(Annotation currentAnnotation, Annotation annotation) {
    return currentAnnotation.body().equals(annotation.body()) &&
        currentAnnotation.creator().equals(annotation.creator()) &&
        currentAnnotation.preferenceScore() == annotation.preferenceScore();
  }

  public AnnotationRecord handleMessage(AnnotationEvent event)
      throws DataBaseException, FailedProcessingException {
    log.info("Received annotation event of: {}", event);
    var annotation = convertToAnnotation(event);
    if (environment.matchesProfiles(Profiles.WEB)){
      return persistNewAnnotation(annotation);
    }
    else {
      var currentAnnotationOptional = repository.getAnnotation(annotation.target(),
          annotation.creator(), annotation.motivation());
      if (currentAnnotationOptional.isEmpty()) {
        return persistNewAnnotation(annotation);
      } else {
        var currentAnnotationRecord = currentAnnotationOptional.get();
        if (annotationAreEqual(currentAnnotationRecord.annotation(), annotation)) {
          log.info("Received annotation is equal to annotation: {}", currentAnnotationRecord.id());
          processEqualAnnotation(currentAnnotationRecord);
          return null;
        } else {
          log.info("Annotation with id: {} has received an update",
              currentAnnotationRecord.id());
          return updateExistingAnnotation(currentAnnotationRecord, annotation);
        }
      }
    }
  }

  private AnnotationRecord updateExistingAnnotation(AnnotationRecord currentAnnotationRecord,
      Annotation annotation) throws FailedProcessingException {
    try {
      filterUpdatesAndUpdateHandleRecord(currentAnnotationRecord, annotation);
    } catch (PidCreationException e) {
      log.error("Unable to post update for annotation {}", currentAnnotationRecord.id(), e);
      throw new FailedProcessingException();
    }
    var id = currentAnnotationRecord.id();
    var version = currentAnnotationRecord.version() + 1;
    var annotationRecord = new AnnotationRecord(id, version, annotation.created(), annotation);
    var result = repository.createAnnotationRecord(annotationRecord);
    if (result == SUCCESS) {
      log.info("Annotation: {} has been successfully committed to database", id);
      IndexResponse indexDocument = null;
      try {
        indexDocument = elasticRepository.indexAnnotation(annotationRecord);
      } catch (IOException | ElasticsearchException e) {
        log.error("Rolling back, failed to insert records in elastic", e);
        rollbackUpdatedAnnotation(currentAnnotationRecord, annotationRecord, false);
      }
      if (indexDocument.result().equals(Result.Updated)) {
        log.info("Annotation: {} has been successfully indexed", id);
        try {
          kafkaService.publishUpdateEvent(currentAnnotationRecord, annotationRecord);
        } catch (JsonProcessingException e) {
          rollbackUpdatedAnnotation(currentAnnotationRecord, annotationRecord, true);
        }
      } else {
        log.error("Elasticsearch did not update annotation: {}", id);
        throw new FailedProcessingException();
      }
    }
    return annotationRecord;
  }

  private void rollbackUpdatedAnnotation(AnnotationRecord currentAnnotationRecord,
      AnnotationRecord annotationRecord, boolean elasticRollback) throws FailedProcessingException {
    if (elasticRollback) {
      try {
        elasticRepository.indexAnnotation(currentAnnotationRecord);
      } catch (IOException | ElasticsearchException e) {
        log.error("Fatal exception, unable to rollback update for: {}", annotationRecord, e);
      }
    }
    repository.createAnnotationRecord(currentAnnotationRecord);
    filterUpdatesAndRollbackHandleUpdateRecord(currentAnnotationRecord,
        annotationRecord.annotation());
    throw new FailedProcessingException();
  }


  private void processEqualAnnotation(AnnotationRecord currentAnnotationRecord)
      throws FailedProcessingException {
    var result = repository.updateLastChecked(currentAnnotationRecord);
    if (result == SUCCESS) {
      log.info("Successfully updated lastChecked for existing annotation: {}",
          currentAnnotationRecord.id());
    } else {
      throw new FailedProcessingException();
    }
  }

  private AnnotationRecord persistNewAnnotation(Annotation annotation)
      throws FailedProcessingException {
    var id = postHandle(annotation);
    log.info("New id has been generated for Annotation: {}", id);
    var annotationRecord = new AnnotationRecord(id, 1, annotation.created(), annotation);
    var result = repository.createAnnotationRecord(annotationRecord);
    if (result == SUCCESS) {
      log.info("Annotation: {} has been successfully committed to database", id);
      IndexResponse indexDocument = null;
      try {
        indexDocument = elasticRepository.indexAnnotation(annotationRecord);
      } catch (IOException | ElasticsearchException e) {
        log.error("Rolling back, failed to insert records in elastic", e);
        rollbackNewAnnotation(annotationRecord, false);
      }
      if (indexDocument.result().equals(Result.Created)) {
        log.info("Annotation: {} has been successfully indexed", id);
        try {
          kafkaService.publishCreateEvent(annotationRecord);
        } catch (JsonProcessingException e) {
          rollbackNewAnnotation(annotationRecord, true);
        }
      } else {
        log.error("Elasticsearch did not create annotation: {}", id);
        throw new FailedProcessingException();
      }
    }
    return annotationRecord;
  }

  private String postHandle(Annotation annotation) throws FailedProcessingException {
    var requestBody = fdoRecordService.buildPostHandleRequest(annotation);
    try {
      return handleComponent.postHandle(requestBody);
    } catch (PidCreationException e) {
      log.error("Unable to create handle for given annotation. ", e);
      throw new FailedProcessingException();
    }
  }

  private void rollbackNewAnnotation(AnnotationRecord annotationRecord, boolean elasticRollback)
      throws FailedProcessingException {
    log.warn("Rolling back for annotation: {}", annotationRecord);
    if (elasticRollback) {
      try {
        elasticRepository.archiveAnnotation(annotationRecord.id());
      } catch (IOException | ElasticsearchException e) {
        log.info("Fatal exception, unable to rollback: {}", annotationRecord.id(), e);
      }
    }
    repository.rollbackAnnotation(annotationRecord.id());
    rollbackHandleCreation(annotationRecord);
    throw new FailedProcessingException();
  }

  private void filterUpdatesAndUpdateHandleRecord(AnnotationRecord currentAnnotationRecord,
      Annotation annotation)
      throws PidCreationException {
    if (!fdoRecordService.handleNeedsUpdate(currentAnnotationRecord.annotation(), annotation)) {
      return;
    }
    var requestBody = fdoRecordService.buildPatchRollbackHandleRequest(annotation,
        currentAnnotationRecord.id());
    handleComponent.updateHandle(requestBody);
  }

  private void filterUpdatesAndRollbackHandleUpdateRecord(AnnotationRecord currentAnnotationRecord,
      Annotation annotation) {
    if (!fdoRecordService.handleNeedsUpdate(currentAnnotationRecord.annotation(), annotation)) {
      return;
    }
    var requestBody = fdoRecordService.buildPatchRollbackHandleRequest(annotation,
        currentAnnotationRecord.id());
    try {
      handleComponent.rollbackHandleUpdate(requestBody);
    } catch (PidCreationException e) {
      log.error("Unable to rollback handle update for annotation {}", currentAnnotationRecord.id(),
          e);
    }
  }

  private void rollbackHandleCreation(AnnotationRecord annotationRecord) {
    var requestBody = fdoRecordService.buildRollbackCreationRequest(annotationRecord);
    try {
      handleComponent.rollbackHandleCreation(requestBody);
    } catch (PidCreationException e) {
      log.error("Unable to rollback creation for annotation {}", annotationRecord.id(), e);
    }
  }

  private Annotation convertToAnnotation(AnnotationEvent event) {
    var generator = createGenerator();
    return new Annotation(
        event.type(),
        event.motivation(),
        event.target(),
        event.body(),
        100,
        event.creator(),
        event.created(),
        generator,
        Instant.now()
    );
  }

  private JsonNode createGenerator() {
    var objectNode = mapper.createObjectNode();
    objectNode.put("id", "https://hdl.handle.net/anno-process-service-pid");
    objectNode.put("type", "tool/Software");
    objectNode.put("name", "Annotation Procession Service");
    return objectNode;
  }

  public void archiveAnnotation(String id) throws IOException, FailedProcessingException {
    if (repository.getAnnotationById(id).isPresent()) {
      log.info("Archive annotation: {} in handle service", id);
      var requestBody = fdoRecordService.buildArchiveHandleRequest(id);
      try {
        handleComponent.archiveHandle(requestBody, id);
      } catch (PidCreationException e) {
        log.error("Unable to archive annotation in handle system for annotation {}", id, e);
        throw new FailedProcessingException();
      }
      log.info("Removing annotation: {} from indexing service", id);
      var document = elasticRepository.archiveAnnotation(id);
      if (document.result().equals(Result.Deleted) || document.result().equals(Result.NotFound)) {
        log.info("Archive annotation: {} in database", id);
        repository.archiveAnnotation(id);
        log.info("Archived annotation: {}", id);
        log.info("Tombstoning PID record of annotation: {}", id);
      }
    } else {
      log.info("Annotation with id: {} is already archived", id);
    }
  }
}
