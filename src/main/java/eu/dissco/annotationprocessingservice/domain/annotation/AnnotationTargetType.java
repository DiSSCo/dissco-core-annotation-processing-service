package eu.dissco.annotationprocessingservice.domain.annotation;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum AnnotationTargetType {
  @JsonProperty("DigitalSpecimen")DIGITAL_SPECIMEN("DigitalSpecimen"),
  @JsonProperty("DigitalMedia")MEDIA_OBJECT("DigitalMedia");

  private final String name;

  AnnotationTargetType(String name){
    this.name = name;
  }

  @Override
  public String toString(){
    return this.name;
  }
}
