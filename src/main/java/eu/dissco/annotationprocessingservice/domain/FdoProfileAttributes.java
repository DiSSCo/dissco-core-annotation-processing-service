package eu.dissco.annotationprocessingservice.domain;

import lombok.Getter;

@Getter
public enum FdoProfileAttributes {

  TYPE("type"),
  // Issued for agent should be DiSSCo PID; currently it's set as Naturalis's ROR
  ISSUED_FOR_AGENT("issuedForAgent"),
  TARGET_PID("targetPid"),
  TARGET_TYPE("targetType"),
  MOTIVATION("motivation"),
  ANNOTATION_HASH("annotationHash");

  private final String attribute;

  private FdoProfileAttributes(String attribute) {
    this.attribute = attribute;
  }

}
