package eu.dissco.annotationprocessingservice.service;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.domain.HashedAnnotation;
import eu.dissco.annotationprocessingservice.domain.ProcessResult;
import eu.dissco.annotationprocessingservice.domain.UpdatedAnnotation;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.service.serviceuitls.AnnotationHasher;
import eu.dissco.annotationprocessingservice.web.HandleComponent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
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

  public void handleMessage(AnnotationEvent event)
      throws DataBaseException, FailedProcessingException {
    log.info("Received annotations event of: {}", event);
    masJobRecordService.verifyMasJobId(event);
    var processResult = processAnnotations(event);
    processEqualAnnotations(processResult.equalAnnotations());
    updateExistingAnnotations(processResult.changedAnnotations(), event.jobId());
    persistNewAnnotation(processResult.newAnnotations(), event.jobId());
  }

  private ProcessResult processAnnotations(AnnotationEvent event) {
    var equalAnnotations = new HashSet<Annotation>();
    var changedAnnotations = new HashSet<UpdatedAnnotation>();
    var hashedAnnotations =
        event.annotations().stream()
            .map(annotation -> new HashedAnnotation(annotation, hashAnnotation(annotation)))
            .collect(Collectors.toSet());
    var existingAnnotations = repository.getAnnotationFromHash(
        hashedAnnotations.stream().map(HashedAnnotation::hash).collect(Collectors.toSet()));
    var newAnnotations = filterNewAnnotations(existingAnnotations, hashedAnnotations);
    var equalOrUpdatedAnnotationsMap = hashedAnnotations.stream()
        .filter(newAnnotations::contains)
        .collect(Collectors.toMap(HashedAnnotation::hash, ha -> ha));

    for (var currentAnnotation : existingAnnotations) {
      var eventAnnotation = equalOrUpdatedAnnotationsMap.get(currentAnnotation.hash());
      if (annotationAreEqual(eventAnnotation.annotation(), currentAnnotation.annotation())) {
        equalAnnotations.add(currentAnnotation.annotation());
      } else {
        changedAnnotations.add(new UpdatedAnnotation(currentAnnotation, eventAnnotation));
      }
    }
    return new ProcessResult(equalAnnotations, changedAnnotations, newAnnotations);
  }

  private void processEqualAnnotations(Set<Annotation> currentAnnotations) {
    var idList = currentAnnotations.stream().map(Annotation::getOdsId).toList();
    repository.updateLastChecked(idList);
    log.info("Successfully updated lastChecked for existing annotations: {}", idList);
  }


  private List<HashedAnnotation> filterNewAnnotations(List<HashedAnnotation> existingAnnotations,
      Set<HashedAnnotation> allAnnotations) {
    var currentAnnotationHashes = existingAnnotations.stream().map(HashedAnnotation::hash).collect(
        Collectors.toSet());
    return allAnnotations.stream()
        .filter(ha -> !currentAnnotationHashes.contains(ha.hash())).toList();
  }

  private void persistNewAnnotation(List<HashedAnnotation> annotations, UUID jobId)
      throws FailedProcessingException {
    var idList = postHandles(annotations, jobId);
    for (int i = 0; i < annotations.size(); i++) {
      enrichNewAnnotation(annotations.get(i).annotation(), idList.get(i));
    }
    log.info("New ids have been generated for Annotations: {}", idList);
    repository.createAnnotationRecord(annotations);
    log.info("Annotations {} has been successfully committed to database", idList);
    try {
      indexElasticNewAnnotations(annotations.stream().map(HashedAnnotation::annotation).toList(),
          idList);
    } catch (FailedProcessingException e) {
      masJobRecordService.markMasJobRecordAsFailed(jobId);
      throw new FailedProcessingException();
    }
    masJobRecordService.markMasJobRecordAsComplete(jobId, idList);
  }

  private List<String> postHandles(List<HashedAnnotation> hashedAnnotations, UUID jobId)
      throws FailedProcessingException {
    var requestBody = fdoRecordService.buildPostHandleRequest(hashedAnnotations);
    try {
      return handleComponent.postHandle(requestBody);
    } catch (PidCreationException e) {
      log.error("Unable to create handle for given annotations. ", e);
      masJobRecordService.markMasJobRecordAsFailed(jobId);
      throw new FailedProcessingException();
    }
  }

  private void updateExistingAnnotations(Set<UpdatedAnnotation> updatedAnnotations, UUID jobId)
      throws FailedProcessingException {
    var idList = getIdListFromUpdates(updatedAnnotations);
    try {
      filterUpdatesAndUpdateHandleRecord(updatedAnnotations);
    } catch (PidCreationException e) {
      log.error("Unable to post update for annotations {}", idList, e);
      masJobRecordService.markMasJobRecordAsFailed(jobId);
      throw new FailedProcessingException();
    }
    updatedAnnotations.forEach(
        p -> enrichUpdateAnnotation(p.annotation().annotation(),
            p.currentAnnotation().annotation()));
    repository.createAnnotationRecord(
        updatedAnnotations.stream().map(UpdatedAnnotation::annotation).toList());
    log.info("Annotation: {} has been successfully committed to database", idList);
    try {
      indexElasticUpdatedAnnotation(updatedAnnotations);
    } catch (FailedProcessingException e) {
      masJobRecordService.markMasJobRecordAsFailed(jobId);
      throw new FailedProcessingException();
    }
    masJobRecordService.markMasJobRecordAsComplete(jobId, idList);
  }

  private UUID hashAnnotation(Annotation annotation) {
    return AnnotationHasher.getAnnotationHash(annotation);
  }

  protected void indexElasticNewAnnotations(List<Annotation> annotations, List<String> idList)
      throws FailedProcessingException {
    BulkResponse bulkResponse = null;
    try {
      bulkResponse = elasticRepository.indexAnnotations(annotations);
    } catch (IOException | ElasticsearchException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      rollbackNewAnnotations(annotations, false);
      throw new FailedProcessingException();
    }
    if (!bulkResponse.errors()) {
      log.info("Annotations: {} have been successfully indexed", idList);
      try {
        for (var annotation : annotations) {
          kafkaService.publishCreateEvent(annotation);
        }
      } catch (JsonProcessingException e) {
        log.error("Unable to publish annotations to kafka, rolling back");
        rollbackNewAnnotations(annotations, true);
        throw new FailedProcessingException();
      }
    } else {
      partiallyFailedElasticInsert(annotations, bulkResponse);
      throw new FailedProcessingException();
    }
  }

  private void indexElasticUpdatedAnnotation(Set<UpdatedAnnotation> updatedAnnotations)
      throws FailedProcessingException {
    var idList = getIdListFromUpdates(updatedAnnotations);
    BulkResponse bulkResponse = null;
    try {
      bulkResponse = elasticRepository.indexAnnotations(updatedAnnotations
          .stream()
          .map(UpdatedAnnotation::annotation)
          .map(HashedAnnotation::annotation)
          .toList());
    } catch (IOException | ElasticsearchException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      rollbackUpdatedAnnotations(updatedAnnotations, false);
      throw new FailedProcessingException();
    }
    if (!bulkResponse.errors()) {
      log.info("Annotations: {} have been successfully indexed", idList);
      try {
        for (var updatedAnnotation : updatedAnnotations) {
          kafkaService.publishUpdateEvent(updatedAnnotation.currentAnnotation().annotation(),
              updatedAnnotation.annotation()
                  .annotation());
        }
      } catch (JsonProcessingException e) {
        rollbackUpdatedAnnotations(updatedAnnotations, true);
        throw new FailedProcessingException();
      }
    } else {
      partiallyFailedElasticUpdate(updatedAnnotations, bulkResponse);
      throw new FailedProcessingException();
    }
  }

  private void rollbackNewAnnotations(List<Annotation> annotations, boolean elasticRollback) {
    var idList = annotations.stream().map(Annotation::getOdsId).toList();
    log.warn("Rolling back for annotations: {}", idList);
    if (elasticRollback) {
      try {
        elasticRepository.archiveAnnotations(idList);
      } catch (IOException | ElasticsearchException e) {
        log.info("Fatal exception, unable to rollback: {}", idList);
      }
    }
    repository.rollbackAnnotations(idList);
    rollbackHandleCreation(idList);
  }

  private void rollbackUpdatedAnnotations(Set<UpdatedAnnotation> updatedAnnotations,
      boolean elasticRollback) {
    var currentAnnotationsHashed = updatedAnnotations.stream()
        .map(UpdatedAnnotation::currentAnnotation).toList();
    var idList = getIdListFromUpdates(updatedAnnotations);
    if (elasticRollback) {
      try {
        elasticRepository.indexAnnotations(
            currentAnnotationsHashed.stream().map(HashedAnnotation::annotation).toList());
      } catch (IOException | ElasticsearchException e) {
        log.error("Fatal exception, unable to rollback update for: {}", idList, e);
      }
    }
    repository.createAnnotationRecord(currentAnnotationsHashed);
    filterUpdatesAndRollbackHandleUpdateRecord(updatedAnnotations);
  }

  private void rollbackHandleCreation(List<String> idList) {
    var requestBody = fdoRecordService.buildRollbackCreationRequest(idList);
    try {
      handleComponent.rollbackHandleCreation(requestBody);
    } catch (PidCreationException e) {
      log.error("Unable to rollback creation for annotations {}", idList, e);
    }
  }

  private void filterUpdatesAndRollbackHandleUpdateRecord(
      Set<UpdatedAnnotation> updatedAnnotations) {
    var requestBody = filterHandleUpdates(updatedAnnotations);
    try {
      handleComponent.rollbackHandleUpdate(requestBody);
    } catch (PidCreationException e) {
      log.error("Unable to rollback handle update for annotations {}",
          updatedAnnotations.stream().map(p -> p.currentAnnotation().annotation().getOdsId())
              .toList());
    }
  }

  private void filterUpdatesAndUpdateHandleRecord(Set<UpdatedAnnotation> updatedAnnotations)
      throws PidCreationException {
    var requestBody = filterHandleUpdates(updatedAnnotations);
    handleComponent.updateHandle(requestBody);
  }

  private List<JsonNode> filterHandleUpdates(Set<UpdatedAnnotation> updatedAnnotations) {
    var handleNeedsUpdate = updatedAnnotations.stream()
        .filter(p -> fdoRecordService.handleNeedsUpdate(p.currentAnnotation().annotation(),
            p.annotation().annotation()))
        .map(UpdatedAnnotation::annotation)
        .toList();
    return fdoRecordService.buildPatchRollbackHandleRequest(
        handleNeedsUpdate.stream().map(HashedAnnotation::annotation).toList());
  }

  private static List<String> getIdListFromUpdates(Set<UpdatedAnnotation> updatedAnnotations) {
    return updatedAnnotations.stream()
        .map(UpdatedAnnotation::currentAnnotation)
        .map(HashedAnnotation::annotation)
        .map(Annotation::getOdsId).toList();

  }

  private void partiallyFailedElasticInsert(List<Annotation> annotations,
      BulkResponse bulkResponse) {
    var annotationMap = annotations.stream()
        .collect(Collectors.toMap(Annotation::getOdsId, a -> a));
    var handleRollbacks = new ArrayList<String>();
    var annotationRollbacksElasticFail = new ArrayList<Annotation>();
    var annotationRollbacksElasticSuccess = new ArrayList<Annotation>();

    bulkResponse.items().forEach(
        item -> {
          var annotation = annotationMap.get(item.id());
          if (item.error() != null) {
            handleRollbacks.add(annotation.getOdsId());
            annotationRollbacksElasticFail.add(annotation);
            log.error("Failed item to insert into elastic search: {} with errors {}",
                annotation.getOdsId(), item.error().reason());
          } else {
            try {
              kafkaService.publishCreateEvent(annotation);
            } catch (JsonProcessingException e) {
              log.error("Unable to publish annotation to kafka");
              handleRollbacks.add(annotation.getOdsId());
              annotationRollbacksElasticSuccess.add(annotation);
            }
          }
        }
    );
    rollbackHandleCreation(handleRollbacks);
    rollbackNewAnnotations(annotationRollbacksElasticFail, false);
    rollbackNewAnnotations(annotationRollbacksElasticSuccess, true);
  }

  private void partiallyFailedElasticUpdate(Set<UpdatedAnnotation> updatedAnnotations,
      BulkResponse bulkResponse) {
    var annotationMap = updatedAnnotations.stream()
        .collect(Collectors.toMap(updatePair -> updatePair.annotation().annotation().getOdsId(),
            p -> p));
    var annotationRollbacksElasticFail = new HashSet<UpdatedAnnotation>();
    var annotationRollbacksElasticSuccess = new HashSet<UpdatedAnnotation>();

    bulkResponse.items().forEach(
        item -> {
          var annotationPair = annotationMap.get(item.id());
          if (item.error() != null) {
            annotationRollbacksElasticFail.add(annotationPair);
            log.error("Failed item to insert into elastic search: {} with errors {}",
                annotationPair.annotation().annotation().getOdsId(), item.error().reason());
          } else {
            try {
              kafkaService.publishUpdateEvent(annotationPair.currentAnnotation().annotation(),
                  annotationPair.annotation()
                      .annotation());
            } catch (JsonProcessingException e) {
              log.error("Unable to publish annotation to kafka");
              annotationRollbacksElasticSuccess.add(annotationPair);
            }
          }
        }
    );
    rollbackUpdatedAnnotations(annotationRollbacksElasticFail, false);
    rollbackUpdatedAnnotations(annotationRollbacksElasticSuccess, true);
  }

}
