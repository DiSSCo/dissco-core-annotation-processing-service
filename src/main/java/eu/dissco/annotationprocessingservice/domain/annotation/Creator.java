package eu.dissco.annotationprocessingservice.domain.annotation;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class Creator {
  @JsonProperty("ods:type")
  String odsType;
  @JsonProperty("foaf:name")
  String foafName;
  @JsonProperty("ods:id")
  String odsId;

}
