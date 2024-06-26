package eu.dissco.annotationprocessingservice.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class SchemaValidatorComponent {

  private final JsonSchema annotationSchema;
  private final ObjectMapper mapper;
  private final Environment env;

  public void validateEvent(AnnotationEvent event) throws AnnotationValidationException {
    for (var annotation : event.annotations()) {
      validateAnnotationRequest(annotation, true);
    }
  }

  public void validateAnnotationRequest(Annotation annotation, boolean isNewAnnotation)
      throws AnnotationValidationException {
    validateId(annotation, isNewAnnotation);
    var annotationRequest = mapper.valueToTree(annotation);
    var errors = annotationSchema.validate(annotationRequest);
    if (Boolean.TRUE.equals(isNewAnnotation) && annotation.getDcTermsCreated() == null) {
      log.error("Invalid annotation received. Missing dcterms created");
      throw new AnnotationValidationException();
    }
    if (errors.isEmpty()) {
      return;
    }
    log.error("Invalid annotation received: {}. errors: {}", annotation, errors);
    throw new AnnotationValidationException();
  }

  private void validateId(Annotation annotation, Boolean isNewAnnotation)
      throws AnnotationValidationException {
    if (Boolean.TRUE.equals(isNewAnnotation) && annotation.getOdsId() != null) {
      log.warn("Attempting overwrite annotation with \"ods:id\" " + annotation.getOdsId());
      throw new AnnotationValidationException();
    }
    if (Boolean.FALSE.equals(isNewAnnotation) && annotation.getOdsId() == null) {
      log.error("\"ods:id\" not provided for annotation update");
      throw new AnnotationValidationException();
    }
  }

}
