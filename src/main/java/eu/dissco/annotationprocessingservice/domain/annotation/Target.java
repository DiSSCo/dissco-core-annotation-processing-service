package eu.dissco.annotationprocessingservice.domain.annotation;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class Target {
  @JsonProperty("ods:id")
  String odsId;
  @JsonProperty("ods:type")
  AnnotationTargetType odsType;
  @JsonProperty("oa:selector")
  Selector oaSelector;
}
