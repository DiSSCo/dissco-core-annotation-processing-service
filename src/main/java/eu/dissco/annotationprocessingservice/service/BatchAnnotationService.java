package eu.dissco.annotationprocessingservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.annotationprocessingservice.component.JsonPathComponent;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.domain.BatchMetadata;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.domain.annotation.Target;
import eu.dissco.annotationprocessingservice.exception.BatchingException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

  public void applyBatchAnnotations(AnnotationEvent annotationEvent)
      throws IOException, BatchingException {
    var pageSizePlusOne = applicationProperties.getBatchPageSize() + 1;
    for (var batchMetadata : annotationEvent.batchMetadata()) {
      boolean moreBatching = true;
      int pageNumber = 1;
      while (moreBatching) {
        var baseAnnotation = getBaseAnnotation(batchMetadata.placeInBatch(),
            annotationEvent.annotations());
        var targetType = baseAnnotation.getOaTarget().getOdsType();
        var annotatedObjects = elasticRepository.searchByBatchMetadata(targetType,
            batchMetadata, pageNumber, pageSizePlusOne);
        if (annotatedObjects.isEmpty()) {
          log.info("No annotated objects found. Page number: {}", pageNumber);
          return;
        }
        if (annotatedObjects.size() <= applicationProperties.getBatchPageSize()) {
          moreBatching = false;
        }
        log.info("Successfully identified {} {}s to apply batch annotations to",
            annotatedObjects.size(),
            targetType);
        var annotations = generateBatchAnnotations(baseAnnotation, batchMetadata, annotatedObjects);
        annotations = moreBatching ? annotations.subList(0,
            applicationProperties.getBatchPageSize()) : annotations;
        var batchEvent = new AnnotationEvent(annotations, annotationEvent.jobId(), null, true);
        kafkaService.publishBatchAnnotation(batchEvent);
        log.info("Successfully published {} batch annotations to queue", annotatedObjects.size());
        pageNumber = pageNumber + 1;
      }
    }
  }

  private Annotation getBaseAnnotation(int placeInBatch, List<Annotation> annotations)
      throws BatchingException {
    for (var annotation : annotations) {
      if (placeInBatch == annotation.getPlaceInBatch()) {
        return annotation;
      }
    }
    log.error("Unable to find batch metadata for annotation with placeInBatch {}",
        placeInBatch);
    throw new BatchingException();
  }

  private List<Annotation> generateBatchAnnotations(Annotation baseAnnotation,
      BatchMetadata batchMetadata, List<JsonNode> annotatedObjects)
      throws BatchingException, JsonProcessingException {
    var batchAnnotations = new ArrayList<Annotation>();
    for (var annotatedObject : annotatedObjects) {
      var targets = jsonPathComponent.getAnnotationTargets(batchMetadata, annotatedObject,
          baseAnnotation.getOaTarget());
      batchAnnotations.addAll(copyAnnotation(baseAnnotation, targets));
    }
    return batchAnnotations;
  }

  private List<Annotation> copyAnnotation(Annotation baseAnnotation, List<Target> targets) {
    ArrayList<Annotation> newAnnotations = new ArrayList<>();
    for (var target : targets) {
      newAnnotations.add(new Annotation()
          .withOdsId(null)
          .withOdsJobId(baseAnnotation.getOdsJobId())
          .withOaMotivation(baseAnnotation.getOaMotivation())
          .withDcTermsCreated(baseAnnotation.getDcTermsCreated())
          .withOaCreator(baseAnnotation.getOaCreator())
          .withOaBody(baseAnnotation.getOaBody())
          .withOdsAggregateRating(baseAnnotation.getOdsAggregateRating())
          .withOaTarget(target
          ));
    }
    return newAnnotations;
  }

}
