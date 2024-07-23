package eu.dissco.annotationprocessingservice.domain;

import eu.dissco.annotationprocessingservice.schema.Annotation;
import java.util.List;
import java.util.UUID;

public record BatchMetadata(
    List<Annotation> annotations,
    String jobId,
    List<BatchMetadataExtended> batchMetadataExtended,
    UUID batchId
) {

}
