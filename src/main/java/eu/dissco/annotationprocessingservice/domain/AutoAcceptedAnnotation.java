package eu.dissco.annotationprocessingservice.domain;

import eu.dissco.annotationprocessingservice.schema.Agent;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;
import java.util.List;

public record AutoAcceptedAnnotation(
    Agent acceptingAgent,
    List<AnnotationProcessingRequest> annotations
) {

}
