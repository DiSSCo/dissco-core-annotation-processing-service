package eu.dissco.annotationprocessingservice.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum SelectorType {
  @JsonProperty("FieldSelector") FIELD_SELECTOR("ods:FieldSelector"),
  @JsonProperty("ClassSelector") CLASS_SELECTOR("ods:ClassSelector"),
  @JsonProperty("FragmentSelector") FRAGMENT_SELECTOR("oa:FragmentSelector");

  private final String state;

  SelectorType(String s){
    this.state = s;
  }

  @Override
  public String toString() {
    return state;
  }

  public static SelectorType fromString(String name) {
    for (SelectorType type : SelectorType.values()) {
      if (type.state.equals(name)) {
        return type;
      }
    }
    return null;
  }

}
