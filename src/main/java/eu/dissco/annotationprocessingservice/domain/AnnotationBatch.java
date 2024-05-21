package eu.dissco.annotationprocessingservice.domain;

import java.time.Instant;
import java.util.UUID;

public record AnnotationBatch(
    UUID batchId,
    String userId,
    String masId,
    String parentAnnotationId,
    Instant createdOn,
    String jobId
) {

}
