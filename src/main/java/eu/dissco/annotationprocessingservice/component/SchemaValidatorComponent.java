package eu.dissco.annotationprocessingservice.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingEvent;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class SchemaValidatorComponent {

  public void validateEvent(AnnotationProcessingEvent event) throws AnnotationValidationException {
    for (var annotation : event.getAnnotations()) {
      validateAnnotationRequest(annotation, true);
    }
  }

  public void validateAnnotationRequest(AnnotationProcessingRequest annotation,
      boolean isNewAnnotation)
      throws AnnotationValidationException {
    validateId(annotation, isNewAnnotation);
    if (Boolean.TRUE.equals(isNewAnnotation) && annotation.getDctermsCreated() == null
    || Boolean.TRUE.equals(isNewAnnotation && annotation.getDctermsCreator() == null)) {
      log.error("Invalid hashedAnnotation received. dcterms:creator and dcterms:created are required for new annotations");
      throw new AnnotationValidationException();
    }
  }

  private void validateId(AnnotationProcessingRequest annotation, Boolean isNewAnnotation)
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

}
