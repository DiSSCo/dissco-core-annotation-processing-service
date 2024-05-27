package eu.dissco.annotationprocessingservice.domain;

import java.time.Instant;
import java.util.UUID;

public record AnnotationBatchRecord(
    UUID batchId,
    String creatorId,
    String generatorId,
    String parentAnnotationId,
    Instant createdOn,
    String jobId
) {

}
