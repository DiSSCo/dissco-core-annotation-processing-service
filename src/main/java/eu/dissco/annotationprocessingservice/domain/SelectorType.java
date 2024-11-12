package eu.dissco.annotationprocessingservice.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum SelectorType {
  @JsonProperty("TermSelector") FIELD_SELECTOR("ods:TermSelector"),
  @JsonProperty("ClassSelector") CLASS_SELECTOR("ods:ClassSelector"),
  @JsonProperty("FragmentSelector") FRAGMENT_SELECTOR("oa:FragmentSelector");

  private final String state;

  SelectorType(String s) {
    this.state = s;
  }

  public static SelectorType fromString(String name) {
    for (SelectorType type : SelectorType.values()) {
      if (type.state.equals(name)) {
        return type;
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return state;
  }

}
