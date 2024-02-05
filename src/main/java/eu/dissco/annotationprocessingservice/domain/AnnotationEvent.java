package eu.dissco.annotationprocessingservice.domain;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import jakarta.validation.constraints.NotBlank;
import java.util.List;

public record AnnotationEvent(
    @NotBlank List<Annotation> annotations,
    @NotBlank String jobId,
    List<BatchMetadata> batchMetadata,
    Boolean isBatchResult) {

}
