package eu.dissco.annotationprocessingservice.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import eu.dissco.annotationprocessingservice.domain.ProcessResult;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class SchemaValidatorComponent {

  private final JsonSchema annotationSchema;
  private final ObjectMapper mapper;

  public void validateProcessResult(ProcessResult processResult) throws AnnotationValidationException {
    for (var newAnnotation : processResult.newAnnotations()){
      validateAnnotation(newAnnotation.annotation());
    }
    for (var changedAnnotation : processResult.changedAnnotations()){
      validateAnnotation(changedAnnotation.annotation().annotation());
    }
    for (var equalAnnotation : processResult.equalAnnotations()){
      validateAnnotation(equalAnnotation);
    }
  }

  public void validateAnnotationRequest(Annotation annotation, boolean isNew) throws AnnotationValidationException {
    validateId(annotation, isNew);
    validateAnnotation(annotation);

  }

  private void validateAnnotation(Annotation annotation)
      throws AnnotationValidationException {
    var annotationRequest = mapper.valueToTree(annotation);
    var errors = annotationSchema.validate(annotationRequest);
    if (errors.isEmpty()) {
      return;
    }
    log.error("Invalid annotation received: {}. errors: {}", annotation, errors);
    throw new AnnotationValidationException();
  }

  private void validateId(Annotation annotation, Boolean isNew)
      throws AnnotationValidationException {
    if (Boolean.TRUE.equals(isNew) && annotation.getOdsId() != null) {
      log.error( "Attempting overwrite annotation with \"ods:id\" " + annotation.getOdsId());
      throw new AnnotationValidationException();
    }
    if (Boolean.FALSE.equals(isNew) && annotation.getOdsId() == null) {
      log.error("\"ods:id\" not provided for annotation update");
      throw new AnnotationValidationException();
    }
  }


}
