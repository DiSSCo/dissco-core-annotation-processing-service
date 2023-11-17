package eu.dissco.annotationprocessingservice.domain.annotation;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class HasRoi {

  @JsonProperty("ac:xFrac")
  private double valX;
  @JsonProperty("ac:yFrac")
  private double valY;
  @JsonProperty("ac:widthFrac")
  private double widthFrac;
  @JsonProperty("ac:heightFrac")
  private double heightFrac;

  public HasRoi withAcXFrac(double valX) {
    this.valX = valX;
    return this;
  }

  public HasRoi withAcYFrac(double valY) {
    this.valY = valY;
    return this;
  }

  public HasRoi withAcWidthFrac(double widthFrac) {
    this.widthFrac = widthFrac;
    return this;
  }

  public HasRoi withAcHeightFrac(double heightFrac) {
    this.heightFrac = heightFrac;
    return this;
  }

}
