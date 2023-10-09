package eu.dissco.annotationprocessingservice.domain;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

public record AnnotationEvent(
    String type,
    String motivation,
    String creator,
    Instant created,
    JsonNode target,
    JsonNode body,
    UUID jobId
) {

}
