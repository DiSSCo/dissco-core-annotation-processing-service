package eu.dissco.annotationprocessingservice.utils;


import static eu.dissco.annotationprocessingservice.configuration.ApplicationConfiguration.HANDLE_PROXY;

import eu.dissco.annotationprocessingservice.schema.Annotation;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;

public class HandleUtils {

  private HandleUtils() {
    // Utility class
  }

  public static String removeProxy(Annotation annotation) {
    return annotation.getId().replace(HANDLE_PROXY, "");
  }

  public static String removeProxy(AnnotationProcessingRequest annotation) {
    return annotation.getId().replace(HANDLE_PROXY, "");
  }

  public static String removeProxy(String id) {
    return id.replace(HANDLE_PROXY, "");
  }

}
