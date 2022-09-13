package eu.dissco.annotationprocessingservice.domain;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

public record Annotation(
    String type,
    String motivation,
    JsonNode target,
    JsonNode body,
    int preferenceScore,
    String creator,
    Instant created,
    JsonNode generator,
    Instant generated
) {

}
