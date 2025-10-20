package eu.dissco.annotationprocessingservice.domain;

import eu.dissco.annotationprocessingservice.schema.Agent;

public record HashedAutoAcceptedAnnotationRequest(
    Agent acceptingAgent,
    HashedAnnotationRequest hashedRequest
) {

}
