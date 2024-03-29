package eu.dissco.annotationprocessingservice.domain.annotation;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@EqualsAndHashCode
@ToString
public class Target {

  @JsonProperty("ods:id")
  private String odsId;
  @JsonProperty("ods:type")
  private AnnotationTargetType odsType;
  @JsonProperty("oa:selector")
  private Selector oaSelector;

  public Target withOdsId(String odsId) {
    this.odsId = odsId;
    return this;
  }

  public Target withOdsType(AnnotationTargetType odsType) {
    this.odsType = odsType;
    return this;
  }

  public Target withSelector(Selector oaSelector) {
    this.oaSelector = oaSelector;
    return this;
  }


}
