package eu.dissco.annotationprocessingservice.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.domain.Annotation;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.domain.AnnotationRecord;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
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

  public AnnotationRecord handleMessages(AnnotationEvent event) throws TransformerException {
    log.info("Received annotation event of: {}", event);
    var annotation = convertToAnnotation(event);
    var currentAnnotationOptional = repository.getAnnotation(annotation.target(),
        annotation.generator());
    if (currentAnnotationOptional.isEmpty()) {
      return persistNewAnnotation(annotation);
    } else {
      var currentAnnotationRecord = currentAnnotationOptional.get();
      if (currentAnnotationRecord.annotation().equals(annotation)) {
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
      Annotation annotation) {
    if (handleNeedsUpdate(currentAnnotationRecord.annotation(), annotation)) {
      handleService.updateHandle(currentAnnotationRecord.id(), annotation);
    }
    var id = currentAnnotationRecord.id();
    var version = currentAnnotationRecord.version() + 1;
    var annotationRecord = new AnnotationRecord(id, version, annotation);
    var result = repository.createAnnotationRecord(annotationRecord);
    if (result == SUCCESS) {
      log.info("Annotation: {} has been successfully committed to database", id);
      var indexDocument = elasticRepository.indexAnnotation(annotationRecord);
      if (indexDocument.result().jsonValue().equals("updated")) {
        log.info("Annotation: {} has been successfully indexed", id);
        kafkaService.publishUpdateEvent(currentAnnotationRecord, annotationRecord);
      }
    }
    return annotationRecord;
  }

  private boolean handleNeedsUpdate(Annotation currentAnnotation, Annotation annotation) {
    return !currentAnnotation.motivation().equals(annotation.motivation());
  }

  private void processEqualAnnotation(AnnotationRecord currentAnnotationRecord) {
    var result = repository.updateLastChecked(currentAnnotationRecord);
    if (result == SUCCESS) {
      log.info("Successfully updated lastChecked for existing annotation: {}",
          currentAnnotationRecord.id());
    }
  }

  private AnnotationRecord persistNewAnnotation(Annotation annotation) throws TransformerException {
    var id = handleService.createNewHandle(annotation);
    log.info("New id has been generated for Annotation: {}", id);
    var annotationRecord = new AnnotationRecord(id, 1, annotation);
    var result = repository.createAnnotationRecord(annotationRecord);
    if (result == SUCCESS) {
      log.info("Annotation: {} has been successfully committed to database", id);
      var indexDocument = elasticRepository.indexAnnotation(annotationRecord);
      if (indexDocument.result().jsonValue().equals("created")) {
        log.info("Annotation: {} has been successfully indexed", id);
        kafkaService.publishCreateEvent(annotationRecord);
      }
    }
    return annotationRecord;
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

  public void archiveAnnotation(String id) {
    if(repository.getAnnotationById(id).isPresent()){
      log.info("Removing annotation: {} from indexing service", id);
      var document = elasticRepository.archiveAnnotation(id);
      if (document.result().jsonValue().equals("deleted") || document.result().jsonValue().equals("not_found")) {
        log.info("Archive annotation: {} in database", id);
        var result = repository.archiveAnnotation(id);
        if (result > 0){
          log.info("Archived {} versions of annotation: {}", result, id);
          log.info("Tombstoning PID record of annotation: {}", id);
          handleService.archiveRecord(id, "e2befba6-9324-4bb4-9f41-d7dfae4a44b0");
        }
      }
    } else {
      log.info("Annotation with id: {} is already archived", id);
    }
  }
}
