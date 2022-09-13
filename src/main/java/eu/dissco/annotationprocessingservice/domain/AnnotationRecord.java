package eu.dissco.annotationprocessingservice.domain;

public record AnnotationRecord(
    String id,
    int version,
    Annotation annotation
) {

}
