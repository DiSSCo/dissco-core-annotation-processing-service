package eu.dissco.annotationprocessingservice.domain;

import eu.dissco.annotationprocessingservice.domain.annotation.AnnotationTargetType;

public record BatchMetadata(
    int placeInBatch,
    String inputField,
    String inputValue,
    AnnotationTargetType targetType
) {

}
