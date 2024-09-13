package eu.dissco.annotationprocessingservice.utils;

import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingEvent;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;
import eu.dissco.annotationprocessingservice.schema.AnnotationTarget;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class AnnotationValidationUtils {
  private AnnotationValidationUtils(){
    // Utility
  }

  public static void validateEvent(AnnotationProcessingEvent event) throws AnnotationValidationException {
    for (var annotation : event.getAnnotations()) {
      validateAnnotationRequest(annotation, true);
    }
  }

  public static void validateAnnotationRequest(AnnotationProcessingRequest annotation,
      boolean isNewAnnotation)
      throws AnnotationValidationException {
    targetPathIsInBlockNotation(annotation.getOaHasTarget());
    validateId(annotation, isNewAnnotation);
    if (Boolean.TRUE.equals(isNewAnnotation) && annotation.getDctermsCreated() == null
    || Boolean.TRUE.equals(isNewAnnotation && annotation.getDctermsCreator() == null)) {
      log.error("Invalid hashedAnnotation received. dcterms:creator and dcterms:created are required for new annotations");
      throw new AnnotationValidationException();
    }
  }

  private static void validateId(AnnotationProcessingRequest annotation, Boolean isNewAnnotation)
      throws AnnotationValidationException {
    if (Boolean.TRUE.equals(isNewAnnotation) && annotation.getId() != null) {
      log.warn("Attempting overwrite hashedAnnotation with \"@id\" {}", annotation.getId());
      throw new AnnotationValidationException();
    }
    if (Boolean.FALSE.equals(isNewAnnotation) && annotation.getId() == null) {
      log.error("\"@id\" not provided for hashedAnnotation update");
      throw new AnnotationValidationException();
    }
  }

  private static void targetPathIsInBlockNotation(AnnotationTarget target)
      throws AnnotationValidationException {
    var type = target.getOaHasSelector().getAdditionalProperties().get("@type");
    if (type.equals("oa:FragmentSelector")) {
      return;
    }
    String path =
        type.equals("oa:FragmentSelector") ? target.getOaHasSelector().getAdditionalProperties()
            .get("ods:field").toString() :
            target.getOaHasSelector().getAdditionalProperties().get("ods:class").toString();
    if (path.contains(".")) {
      log.error("Selector target Path {} is invalid. Path must be in block notation", path);
      throw new AnnotationValidationException();
    }
  }

}
