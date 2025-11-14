package eu.dissco.annotationprocessingservice.utils;

import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest.OaMotivation;
import java.util.Set;

public class ServiceUtils {

  private ServiceUtils() {
    // Utility class
  }

  private static final Set<OaMotivation> TRANSFORMATIVE_MOTIVATIONS = Set.of(
      AnnotationProcessingRequest.OaMotivation.ODS_ADDING,
      AnnotationProcessingRequest.OaMotivation.OA_EDITING,
      AnnotationProcessingRequest.OaMotivation.ODS_DELETING);

  public static boolean isTransformativeMotivation(
      AnnotationProcessingRequest.OaMotivation oaMotivation) {
    return TRANSFORMATIVE_MOTIVATIONS.contains(oaMotivation);
  }

}
