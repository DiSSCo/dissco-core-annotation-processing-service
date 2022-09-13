package eu.dissco.annotationprocessingservice.domain;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

public record CreateUpdateDeleteEvent(UUID id, String eventType, String agent, String subject,
                                      Instant timestamp, JsonNode data, String comment) {

}
