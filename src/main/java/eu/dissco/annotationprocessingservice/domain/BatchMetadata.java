package eu.dissco.annotationprocessingservice.domain;

public record BatchMetadata(
    String placeInBatch,
    String inputField,
    Object inputValue
) {

}
