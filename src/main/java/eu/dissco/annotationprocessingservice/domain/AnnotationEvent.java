package eu.dissco.annotationprocessingservice.domain;

import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import java.util.List;

public record AnnotationEvent(
    List<Annotation> annotations,
    String jobId,
    List<BatchMetadataExtended> batchMetadata,
    Boolean isBatchResult) {

}
