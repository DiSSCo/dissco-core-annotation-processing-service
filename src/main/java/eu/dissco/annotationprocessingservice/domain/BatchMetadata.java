package eu.dissco.annotationprocessingservice.domain;

import jakarta.validation.constraints.NotNull;

@NotNull
public record BatchMetadata(
    int placeInBatch,
    String inputField,
    String inputValue
) {

}
