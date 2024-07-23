package eu.dissco.annotationprocessingservice.domain;

import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;
import java.util.UUID;

public record HashedAnnotationRequest(
    AnnotationProcessingRequest annotation,
    UUID hash
) {

}
