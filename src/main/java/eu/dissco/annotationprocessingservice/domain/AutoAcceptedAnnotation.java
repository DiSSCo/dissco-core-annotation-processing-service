package eu.dissco.annotationprocessingservice.domain;

import eu.dissco.annotationprocessingservice.schema.Agent;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;
import java.util.Objects;

public record AutoAcceptedAnnotation(
    Agent acceptingAgent,
    AnnotationProcessingRequest annotation,
    Boolean isDataFromSourceSystem
) {

  public AutoAcceptedAnnotation(Agent acceptingAgent, AnnotationProcessingRequest annotation,
      Boolean isDataFromSourceSystem) {
    this.acceptingAgent = acceptingAgent;
    this.annotation = annotation;
    this.isDataFromSourceSystem = Objects.requireNonNullElse(isDataFromSourceSystem, Boolean.TRUE);
  }

}
