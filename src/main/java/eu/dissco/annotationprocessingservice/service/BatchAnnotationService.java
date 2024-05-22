package eu.dissco.annotationprocessingservice.service;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.annotationprocessingservice.component.JsonPathComponent;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.domain.BatchMetadataExtended;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.domain.annotation.AnnotationTargetType;
import eu.dissco.annotationprocessingservice.domain.annotation.Target;
import eu.dissco.annotationprocessingservice.exception.BatchingException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.repository.AnnotationBatchRecordRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@RequiredArgsConstructor
@Slf4j
@Service
public class BatchAnnotationService {

  private final ApplicationProperties applicationProperties;
  private final ElasticSearchRepository elasticRepository;
  private final KafkaPublisherService kafkaService;
  private final JsonPathComponent jsonPathComponent;
  private final AnnotationBatchRecordService annotationBatchRecordService;

  public void applyBatchAnnotations(AnnotationEvent annotationEvent, Map<String, UUID> batchIds)
      throws IOException, BatchingException {
    int pageSizePlusOne = applicationProperties.getBatchPageSize() + 1;
    for (var batchMetadata : annotationEvent.batchMetadata()) {
      var baseAnnotations = getBaseAnnotation(batchMetadata.placeInBatch(),
          annotationEvent.annotations());
      runBatchForMetadata(baseAnnotations, batchMetadata, annotationEvent.jobId(), pageSizePlusOne, batchIds);
    }
  }

  private void runBatchForMetadata(List<Annotation> baseAnnotations,
      BatchMetadataExtended batchMetadata, String jobId, int pageSizePlusOne,
      Map<String, UUID> batchIds) throws IOException, BatchingException {
    int pageNumber = 1;
    boolean moreBatching = true;
    HashMap<UUID, Long> batchCount = batchIds.entrySet().stream().collect(Collectors.toMap(
        Entry::getValue,
        value -> 0L,
        (left, right) -> left, HashMap::new
    ));
    while (moreBatching) {
      var targetType = getTargetTypeFromList(baseAnnotations);
      var annotatedObjects = elasticRepository.searchByBatchMetadataExtended(
          batchMetadata, targetType, pageNumber, pageSizePlusOne);
      if (annotatedObjects.isEmpty()) {
        log.info("No annotated objects found. Page number: {}", pageNumber);
        return;
      }
      if (annotatedObjects.size() <= applicationProperties.getBatchPageSize()) {
        moreBatching = false;
      }
      log.info("Successfully identified {} {}s to apply batch annotations to",
          annotatedObjects.size(), targetType);
      for (var baseAnnotation : baseAnnotations) {
        var batchId = batchIds.get(baseAnnotation.getOdsId());
        var annotations = generateBatchAnnotations(baseAnnotation, batchMetadata,
            annotatedObjects);
        if (!annotations.isEmpty()) {
          var batchEvent = new AnnotationEvent(annotations, jobId, null, true);
          kafkaService.publishBatchAnnotation(batchEvent);
          log.info("Successfully published {} batch annotations to queue", annotatedObjects.size());
          batchCount.put(batchId, batchCount.get(batchId) + annotations.size());
        }
      }
      pageNumber = pageNumber + 1;
    }
    annotationBatchRecordService.updateAnnotationBatchRecord(batchCount);
  }

  private AnnotationTargetType getTargetTypeFromList(List<Annotation> baseAnnotations)
      throws BatchingException {
    var types = baseAnnotations.stream().map(p -> p.getOaTarget().getOdsType()).distinct().toList();
    if (types.size() != 1) {
      log.error("Annotations corresponding to the same batch metadata have different types: {}",
          types);
      throw new BatchingException();
    }
    return types.get(0);
  }

  private List<Annotation> getBaseAnnotation(Integer placeInBatch, List<Annotation> annotations)
      throws BatchingException {
    var subAnnotations = annotations.stream().filter(p -> placeInBatch.equals(p.getPlaceInBatch()))
        .toList();
    if (subAnnotations.isEmpty()) {
      log.error("Unable to find batch metadata for annotation with placeInBatch {}",
          placeInBatch);
      throw new BatchingException();
    }
    return subAnnotations;
  }

  private List<Annotation> generateBatchAnnotations(Annotation baseAnnotation,
      BatchMetadataExtended batchMetadata, List<JsonNode> annotatedObjects) {
    var batchAnnotations = new ArrayList<Annotation>();
    for (var annotatedObject : annotatedObjects) {
      var targets = jsonPathComponent.getAnnotationTargetsExtended(batchMetadata, annotatedObject,
          baseAnnotation.getOaTarget());
      batchAnnotations.addAll(copyAnnotation(baseAnnotation, targets));
    }
    return batchAnnotations;
  }

  private List<Annotation> copyAnnotation(Annotation baseAnnotation, List<Target> targets) {
    ArrayList<Annotation> newAnnotations = new ArrayList<>();
    for (var target : targets) {
      newAnnotations.add(Annotation.builder()
          .odsId(null)
          .oaMotivation(baseAnnotation.getOaMotivation())
          .dcTermsCreated(baseAnnotation.getDcTermsCreated())
          .oaCreator(baseAnnotation.getOaCreator())
          .oaBody(baseAnnotation.getOaBody())
          .odsAggregateRating(baseAnnotation.getOdsAggregateRating())
          .oaTarget(target).build());
    }
    return newAnnotations;
  }

}
