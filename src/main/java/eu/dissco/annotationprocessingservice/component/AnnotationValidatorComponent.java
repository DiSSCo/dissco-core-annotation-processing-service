package eu.dissco.annotationprocessingservice.component;

import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingEvent;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;
import eu.dissco.annotationprocessingservice.schema.AnnotationTarget;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class AnnotationValidatorComponent {

  private final Pattern blockNotationPattern = Pattern.compile("^\\$((\\['[a-zA-Z:]++'\\])++|(\\[[\\d]++\\]))++");

  public void validateEvent(AnnotationProcessingEvent event) throws AnnotationValidationException {
    for (var annotation : event.getAnnotations()) {
      validateAnnotationTargetPath(annotation.getOaHasTarget());
    }
  }

  public static void validateAnnotationRequest(AnnotationProcessingRequest annotation,
      boolean isNewAnnotation)
      throws AnnotationValidationException {
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

  private void validateAnnotationTargetPath(AnnotationTarget target)
      throws AnnotationValidationException {
    var type = target.getOaHasSelector().getAdditionalProperties().get("@type");
    if (type.equals("oa:FragmentSelector")) {
      return;
    }
    String path =
        type.equals("ods:FieldSelector") ? target.getOaHasSelector().getAdditionalProperties()
            .get("ods:field").toString() :
            target.getOaHasSelector().getAdditionalProperties().get("ods:class").toString();
    targetPathIsInBlockNotation(path);
  }

  public void targetPathIsInBlockNotation(String path) throws AnnotationValidationException {
    var matcher = blockNotationPattern.matcher(path);
    if (matcher.find()){
      var result = matcher.group();
      if (!result.equals(path)){
        log.error("Path {} is not valid annotation target path", path);
        throw new AnnotationValidationException();
      }
    }


  }

}
