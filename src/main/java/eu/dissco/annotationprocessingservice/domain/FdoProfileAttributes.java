package eu.dissco.annotationprocessingservice.domain;

import lombok.Getter;

@Getter
public enum FdoProfileAttributes {

  // Issued for agent should be DiSSCo PID; currently it's set as Naturalis's ROR
  ISSUED_FOR_AGENT("issuedForAgent"),
  TARGET_PID("targetPid"),
  TARGET_TYPE("targetType"),
  ANNOTATION_HASH("annotationHash");

  private final String attribute;

  FdoProfileAttributes(String attribute) {
    this.attribute = attribute;
  }

}
