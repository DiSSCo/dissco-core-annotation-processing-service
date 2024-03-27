package eu.dissco.annotationprocessingservice.domain;

import jakarta.validation.constraints.NotNull;

@NotNull
public record BatchMetadata(
    Integer placeInBatch,
    String inputField,
    String inputValue
) {

}
