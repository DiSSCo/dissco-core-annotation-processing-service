package eu.dissco.annotationprocessingservice.domain;

import lombok.Getter;

@Getter
public enum AnnotationState {
  SCHEDULED("scheduled"),
  FAILED("failed"),
  COMPLETED("completed");

  private final String state;

  AnnotationState(String s) {
    this.state = s;
  }

}
