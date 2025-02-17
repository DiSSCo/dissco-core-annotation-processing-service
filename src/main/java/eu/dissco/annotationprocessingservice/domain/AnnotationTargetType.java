package eu.dissco.annotationprocessingservice.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public enum AnnotationTargetType {
  @JsonProperty("https://doi.org/21.T11148/894b1e6cad57e921764e") DIGITAL_SPECIMEN(
      "https://doi.org/21.T11148/894b1e6cad57e921764e"),
  @JsonProperty("https://doi.org/21.T11148/bbad8c4e101e8af01115") MEDIA_OBJECT(
      "https://doi.org/21.T11148/bbad8c4e101e8af01115");

  private final String name;

  AnnotationTargetType(String name) {
    this.name = name;
  }

  public static AnnotationTargetType fromString(String name) {
    for (AnnotationTargetType type : AnnotationTargetType.values()) {
      if (type.name.equals(name)) {
        return type;
      }
    }
    log.error("Invalid annotations target type: {}", name);
    throw new IllegalStateException();
  }

  @Override
  public String toString() {
    return this.name;
  }
}
