package eu.dissco.annotationprocessingservice.domain;

import eu.dissco.annotationprocessingservice.schema.Agent;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;

public record AutoAcceptedAnnotation(
    Agent acceptingAgent,
    AnnotationProcessingRequest annotation
) {

}
