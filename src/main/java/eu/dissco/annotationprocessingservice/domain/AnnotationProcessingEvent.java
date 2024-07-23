package eu.dissco.annotationprocessingservice.domain;

import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;
import java.util.List;
import java.util.UUID;

public record AnnotationProcessingEvent(
    List<AnnotationProcessingRequest> annotations,
    String jobId,
    List<BatchMetadataExtended> batchMetadata,
    UUID batchId) {

}
