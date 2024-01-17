package eu.dissco.annotationprocessingservice.domain.annotation;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum AnnotationTargetType {
  @JsonProperty("DigitalSpecimen")DIGITAL_SPECIMEN("DigitalSpecimen"),
  @JsonProperty("DigitalMedia")MEDIA_OBJECT("DigitalMedia");

  private final String state;

  AnnotationTargetType(String state){
    this.state = state;
  }

  @Override
  public String toString(){
    return this.state;
  }
}
