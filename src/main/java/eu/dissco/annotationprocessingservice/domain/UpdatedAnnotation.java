package eu.dissco.annotationprocessingservice.domain;

public record UpdatedAnnotation(
    HashedAnnotation currentAnnotation,
    HashedAnnotation annotation
) {

}
