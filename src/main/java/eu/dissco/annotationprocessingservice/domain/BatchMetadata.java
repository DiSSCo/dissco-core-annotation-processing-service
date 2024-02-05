package eu.dissco.annotationprocessingservice.domain;

public record BatchMetadata(
    int placeInBatch,
    String inputField,
    String inputValue
) {

}
