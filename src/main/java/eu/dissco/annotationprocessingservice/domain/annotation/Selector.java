package eu.dissco.annotationprocessingservice.domain.annotation;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
@JsonTypeInfo(use = Id.DEDUCTION)
@JsonSubTypes({
    @Type(FieldSelector.class),
    @Type(ClassSelector.class),
    @Type(FragmentSelector.class)})
public abstract class Selector {

  @JsonProperty("ods:type")
  protected final SelectorType odsType;

  protected Selector(SelectorType odsType) {
    this.odsType = odsType;
  }
}
