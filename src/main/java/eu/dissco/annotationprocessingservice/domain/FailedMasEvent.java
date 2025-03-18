package eu.dissco.annotationprocessingservice.domain;

public record FailedMasEvent(
    String jobId,
    String errorMessage
) {

}
