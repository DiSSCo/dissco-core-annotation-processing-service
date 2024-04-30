package eu.dissco.annotationprocessingservice.domain;

import lombok.Getter;

@Getter
public enum FdoProfileAttributes {

  TYPE("type", "https://hdl.handle.net/21.T11148/cf458ca9ee1d44a5608f"),
  FDO_PROFILE("fdoProfile", "https://hdl.handle.net/21.T11148/cf458ca9ee1d44a5608f"),
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
