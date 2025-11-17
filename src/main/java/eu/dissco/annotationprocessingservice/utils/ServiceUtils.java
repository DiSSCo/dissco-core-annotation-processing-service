package eu.dissco.annotationprocessingservice.utils;

import static eu.dissco.annotationprocessingservice.domain.AgentRoleType.PROCESSING_SERVICE;

import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.schema.Agent;
import eu.dissco.annotationprocessingservice.schema.Agent.Type;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest.OaMotivation;
import eu.dissco.annotationprocessingservice.schema.Identifier.DctermsType;
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

  public static Agent createGenerator(ApplicationProperties applicationProperties) {
    return AgentUtils.createAgent(applicationProperties.getProcessorName(),
        applicationProperties.getProcessorHandle(), PROCESSING_SERVICE,
        DctermsType.DOI.value(), Type.SCHEMA_SOFTWARE_APPLICATION);
  }

}
