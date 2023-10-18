package eu.dissco.annotationprocessingservice.domain;

import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import java.time.Instant;

public record AnnotationRecord(
    String id,
    int version,
    Instant created,
    Annotation annotation
) {

}
