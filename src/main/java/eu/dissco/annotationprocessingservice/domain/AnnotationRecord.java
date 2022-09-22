package eu.dissco.annotationprocessingservice.domain;

import java.time.Instant;

public record AnnotationRecord(
    String id,
    int version,
    Instant created,
    Annotation annotation
) {

}
