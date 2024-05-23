package eu.dissco.annotationprocessingservice.domain;

import jakarta.validation.constraints.NotNull;
import java.util.List;

@NotNull
public record BatchMetadataExtended(
    Integer placeInBatch,
    List<BatchMetadataSearchParam> searchParams
) {

}
