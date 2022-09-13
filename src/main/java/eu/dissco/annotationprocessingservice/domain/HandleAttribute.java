package eu.dissco.annotationprocessingservice.domain;

public record HandleAttribute(
    int index,
    String type,
    byte[] data) {

}
