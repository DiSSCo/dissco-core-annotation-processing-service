package eu.dissco.annotationprocessingservice.domain;

import eu.dissco.annotationprocessingservice.schema.Annotation;
import eu.dissco.annotationprocessingservice.schema.AnnotationBatchMetadata;
import java.util.List;
import java.util.UUID;

public record ProcessedAnnotationBatch(
    List<Annotation> annotations,
    String jobId,
    List<AnnotationBatchMetadata> annotationBatchMetadata,
    UUID batchId
) {

}
