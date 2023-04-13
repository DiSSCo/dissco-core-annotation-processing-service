package eu.dissco.annotationprocessingservice.service;

import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.domain.Annotation;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.domain.AnnotationRecord;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import java.io.IOException;
import java.time.Instant;
import javax.xml.transform.TransformerException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProcessingService {

  private static final int SUCCESS = 1;

  private final ObjectMapper mapper;
  private final AnnotationRepository repository;
  private final HandleService handleService;
  private final ElasticSearchRepository elasticRepository;
  private final KafkaPublisherService kafkaService;

  private static boolean annotationAreEqual(Annotation currentAnnotation, Annotation annotation) {
    return currentAnnotation.body().equals(annotation.body()) &&
        currentAnnotation.creator().equals(annotation.creator()) &&
        currentAnnotation.preferenceScore() == annotation.preferenceScore();
  }

  public AnnotationRecord handleMessage(AnnotationEvent event)
      throws TransformerException, DataBaseException, FailedProcessingException {
    log.info("Received annotation event of: {}", event);
    var annotation = convertToAnnotation(event);
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

  private AnnotationRecord updateExistingAnnotation(AnnotationRecord currentAnnotationRecord,
      Annotation annotation) throws FailedProcessingException {
    if (handleNeedsUpdate(currentAnnotationRecord.annotation(), annotation)) {
      handleService.updateHandle(currentAnnotationRecord.id(), annotation);
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
      } catch (IOException e) {
        rollbackUpdatedAnnotation(currentAnnotationRecord, annotationRecord, false);
      }
      if (indexDocument.result().equals(Result.Updated)) {
        log.info("Annotation: {} has been successfully indexed", id);
        try {
          kafkaService.publishUpdateEvent(currentAnnotationRecord, annotationRecord);
        } catch (JsonProcessingException e) {
          rollbackUpdatedAnnotation(currentAnnotationRecord, annotationRecord, true);
        }
      }
    }
    return annotationRecord;
  }

  private void rollbackUpdatedAnnotation(AnnotationRecord currentAnnotationRecord,
      AnnotationRecord annotationRecord, boolean elasticRollback) throws FailedProcessingException {
    if (elasticRollback) {
      try {
        elasticRepository.indexAnnotation(currentAnnotationRecord);
      } catch (IOException e) {
        log.error("Fatal exception, unable to rollback update for: {}", annotationRecord, e);
      }
    }
    repository.createAnnotationRecord(currentAnnotationRecord);
    if (handleNeedsUpdate(currentAnnotationRecord.annotation(), annotationRecord.annotation())) {
      handleService.deleteVersion(currentAnnotationRecord);
    }
    throw new FailedProcessingException();
  }

  private boolean handleNeedsUpdate(Annotation currentAnnotation, Annotation annotation) {
    return !currentAnnotation.motivation().equals(annotation.motivation());
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
      throws TransformerException, FailedProcessingException {
    var id = handleService.createNewHandle(annotation);
    log.info("New id has been generated for Annotation: {}", id);
    var annotationRecord = new AnnotationRecord(id, 1, annotation.created(), annotation);
    var result = repository.createAnnotationRecord(annotationRecord);
    if (result == SUCCESS) {
      log.info("Annotation: {} has been successfully committed to database", id);
      IndexResponse indexDocument = null;
      try {
        indexDocument = elasticRepository.indexAnnotation(annotationRecord);
      } catch (IOException e) {
        rollbackNewAnnotation(annotationRecord, false);
      }
      if (indexDocument.result().equals(Result.Created)) {
        log.info("Annotation: {} has been successfully indexed", id);
        try {
          kafkaService.publishCreateEvent(annotationRecord);
        } catch (JsonProcessingException e) {
          rollbackNewAnnotation(annotationRecord, true);
        }
      }
    }
    return annotationRecord;
  }

  private void rollbackNewAnnotation(AnnotationRecord annotationRecord, boolean elasticRollback)
      throws FailedProcessingException {
    log.warn("Rolling back for annotation: {}", annotationRecord);
    if (elasticRollback) {
      try {
        elasticRepository.archiveAnnotation(annotationRecord.id());
      } catch (IOException e) {
        log.info("Fatal exception, unable to rollback: {}", annotationRecord.id(), e);
      }
    }
    repository.rollbackAnnotation(annotationRecord.id());
    handleService.rollbackHandleCreation(annotationRecord);
    throw new FailedProcessingException();
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

  public void archiveAnnotation(String id) throws IOException {
    if (repository.getAnnotationById(id).isPresent()) {
      log.info("Removing annotation: {} from indexing service", id);
      var document = elasticRepository.archiveAnnotation(id);
      if (document.result().equals(Result.Deleted) || document.result().equals(Result.NotFound)) {
        log.info("Archive annotation: {} in database", id);
        repository.archiveAnnotation(id);
        log.info("Archived annotation: {}", id);
        log.info("Tombstoning PID record of annotation: {}", id);
        handleService.archiveRecord(id, "e2befba6-9324-4bb4-9f41-d7dfae4a44b0");
      }
    } else {
      log.info("Annotation with id: {} is already archived", id);
    }
  }
}
