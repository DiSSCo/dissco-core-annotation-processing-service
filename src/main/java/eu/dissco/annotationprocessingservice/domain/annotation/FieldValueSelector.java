package eu.dissco.annotationprocessingservice.domain.annotation;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;


@Getter
@EqualsAndHashCode(callSuper = true)
@ToString
public class FieldValueSelector extends Selector {

  @JsonProperty("ods:field")
  private String odsField;

  public FieldValueSelector() {
    super(SelectorType.FIELD_VALUE_SELECTOR);
  }

  public FieldValueSelector(String odsField) {
    super(SelectorType.FIELD_VALUE_SELECTOR);
    this.odsField = odsField;
  }

  public FieldValueSelector withOdsField(String odsField) {
    this.odsField = odsField;
    return this;
  }
}