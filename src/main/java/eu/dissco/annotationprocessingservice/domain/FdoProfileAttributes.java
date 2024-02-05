package eu.dissco.annotationprocessingservice.domain;

import lombok.Getter;

@Getter
public enum FdoProfileAttributes {

  TYPE("type", "annotation"),
  FDO_PROFILE("fdoProfile", "https://hdl.handle.net/21.T11148/64396cf36b976ad08267"),
  DIGITAL_OBJECT_TYPE("digitalObjectType", "https://hdl.handle.net/21.T11148/64396cf36b976ad08267"),
  // Issued for agent should be DiSSCo PID; currently it's set as Naturalis's ROR
  ISSUED_FOR_AGENT("issuedForAgent", "https://ror.org/0566bfb96"),

  TARGET_PID("targetPid", null),
  TARGET_TYPE("targetType", null),
  MOTIVATION("motivation", null),
  ANNOTATION_HASH("annotationHash", null);

  private final String attribute;
  private final String defaultValue;

  private FdoProfileAttributes(String attribute, String defaultValue) {
    this.attribute = attribute;
    this.defaultValue = defaultValue;
  }

}
