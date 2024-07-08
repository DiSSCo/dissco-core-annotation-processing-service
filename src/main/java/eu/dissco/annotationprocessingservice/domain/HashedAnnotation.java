package eu.dissco.annotationprocessingservice.domain;

import eu.dissco.annotationprocessingservice.schema.Annotation;
import java.util.UUID;

public record HashedAnnotation(
    Annotation annotation,
    UUID hash
) {

}
