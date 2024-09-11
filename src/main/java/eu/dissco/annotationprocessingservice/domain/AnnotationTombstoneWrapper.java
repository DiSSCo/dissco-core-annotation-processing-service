package eu.dissco.annotationprocessingservice.domain;

import eu.dissco.annotationprocessingservice.schema.Agent;
import eu.dissco.annotationprocessingservice.schema.Annotation;

public record AnnotationTombstoneWrapper (
    Annotation annotation,
    Agent tombstoningAgent
){

}
