package eu.dissco.annotationprocessingservice.service;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.annotationprocessingservice.component.JsonPathComponent;
import eu.dissco.annotationprocessingservice.domain.AnnotationTargetType;
import eu.dissco.annotationprocessingservice.domain.ProcessedAnnotationBatch;
import eu.dissco.annotationprocessingservice.exception.BatchingException;
import eu.dissco.annotationprocessingservice.exception.ConflictException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.schema.Annotation;
import eu.dissco.annotationprocessingservice.schema.AnnotationBatchMetadata;
import eu.dissco.annotationprocessingservice.schema.AnnotationTarget;
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
  private final RabbitMqPublisherService rabbitMqPublisherService;
  private final JsonPathComponent jsonPathComponent;

  public void applyBatchAnnotations(ProcessedAnnotationBatch batchmetadata)
      throws IOException, ConflictException, BatchingException {
    int pageSizePlusOne = applicationProperties.getBatchPageSize() + 1;
    for (var batchMetadata : batchmetadata.annotationBatchMetadata()) {
      var baseAnnotations = getBaseAnnotation(batchMetadata.getOdsPlaceInBatch(),
          batchmetadata.annotations());
      runBatchForMetadata(baseAnnotations, batchMetadata, batchmetadata.jobId(), pageSizePlusOne);
    }
  }

  private void runBatchForMetadata(List<Annotation> baseAnnotations,
      AnnotationBatchMetadata batchMetadata, String jobId, int pageSizePlusOne)
      throws IOException, ConflictException, BatchingException {
    int pageNumber = 1;
    boolean moreBatching = true;
    int errorCount = 0;
    while (moreBatching) {
      try {
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
          // Find our batch id based on the parent hashedAnnotation
          var annotations = generateBatchAnnotations(baseAnnotation, batchMetadata,
              annotatedObjects);
          if (!annotations.isEmpty()) {
            rabbitMqPublisherService.publishBatchAnnotation(new ProcessedAnnotationBatch(
                annotations,
                jobId,
                null,
                baseAnnotation.getOdsBatchID()
            ));
            log.info("Successfully published {} batch annotations to queue",
                annotatedObjects.size());
          }
        }
        pageNumber = pageNumber + 1;
      } catch (BatchingException e) {
        errorCount = errorCount + 1;
        if (errorCount >= applicationProperties.getMaxBatchRetries()) {
          throw e;
        }
      }
    }
  }

  private AnnotationTargetType getTargetTypeFromList(List<Annotation> baseAnnotations)
      throws ConflictException {
    var types = baseAnnotations.stream().map(p -> p.getOaHasTarget().getOdsFdoType()).distinct()
        .toList();
    if (types.size() != 1) {
      log.error("Annotations corresponding to the same batch metadata have different types: {}",
          types);
      throw new ConflictException();
    }
    return AnnotationTargetType.fromString(types.getFirst());
  }

  private List<Annotation> getBaseAnnotation(Integer placeInBatch, List<Annotation> annotations)
      throws ConflictException {
    var subAnnotations = annotations.stream()
        .filter(p -> placeInBatch.equals(p.getOdsPlaceInBatch()))
        .toList();
    if (subAnnotations.isEmpty()) {
      log.error("Unable to find batch metadata for hashedAnnotation with placeInBatch {}",
          placeInBatch);
      throw new ConflictException();
    }
    return subAnnotations;
  }

  private List<Annotation> generateBatchAnnotations(Annotation baseAnnotation,
      AnnotationBatchMetadata batchMetadata, List<JsonNode> annotatedObjects)
      throws BatchingException {
    var batchAnnotations = new ArrayList<Annotation>();
    for (var annotatedObject : annotatedObjects) {
      var targets = jsonPathComponent.getAnnotationTargets(batchMetadata, annotatedObject,
          baseAnnotation.getOaHasTarget());
      batchAnnotations.addAll(copyAnnotation(baseAnnotation, targets));
    }
    return batchAnnotations;
  }

  private List<Annotation> copyAnnotation(Annotation baseAnnotation,
      List<AnnotationTarget> targets) {
    ArrayList<Annotation> newAnnotations = new ArrayList<>();
    for (var target : targets) {
      newAnnotations.add(new Annotation()
          .withOaMotivation(baseAnnotation.getOaMotivation())
          .withOaMotivatedBy(baseAnnotation.getOaMotivatedBy())
          .withDctermsCreated(baseAnnotation.getDctermsCreated())
          .withDctermsCreator(baseAnnotation.getDctermsCreator())
          .withOaHasBody(baseAnnotation.getOaHasBody())
          .withOdsHasAggregateRating(baseAnnotation.getOdsHasAggregateRating())
          .withOaHasTarget(target)
          .withOdsBatchID(baseAnnotation.getOdsBatchID()));
    }
    return newAnnotations;
  }

}
