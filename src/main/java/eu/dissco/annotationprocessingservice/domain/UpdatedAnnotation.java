package eu.dissco.annotationprocessingservice.domain;

public record UpdatedAnnotation(
    HashedAnnotation hashedCurrentAnnotation,
    HashedAnnotation hashedAnnotation
) {

}
