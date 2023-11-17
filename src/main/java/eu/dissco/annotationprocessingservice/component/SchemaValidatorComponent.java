package eu.dissco.annotationprocessingservice.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import eu.dissco.annotationprocessingservice.domain.ProcessResult;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class SchemaValidatorComponent {

  private final JsonSchema annotationSchema;
  private final ObjectMapper mapper;

  public void validateProcessResult(ProcessResult processResult) throws AnnotationValidationException {
    for (var newAnnotation : processResult.newAnnotations()){
      validateAnnotationRequest(newAnnotation.annotation(), true);
    }
    for (var changedAnnotation : processResult.changedAnnotations()){
      validateAnnotationRequest(changedAnnotation.annotation().annotation(), false);
    }
    for (var equalAnnotation : processResult.equalAnnotations()){
      validateAnnotationRequest(equalAnnotation, false);
    }
  }

  public void validateAnnotationRequest(Annotation annotation, boolean isNew)
      throws AnnotationValidationException {
    validateId(annotation, isNew);
    var annotationRequest = mapper.valueToTree(annotation);
    var errors = annotationSchema.validate(annotationRequest);
    if (errors.isEmpty()) {
      return;
    }
    throw new AnnotationValidationException(errors.toString());
  }

  private void validateId(Annotation annotation, Boolean isNew)
      throws AnnotationValidationException {
    if (Boolean.TRUE.equals(isNew) && annotation.getOdsId() != null) {
      throw new AnnotationValidationException(
          "Attempting overwrite annotation with \"ods:id\" " + annotation.getOdsId());
    }
    if (Boolean.FALSE.equals(isNew) && annotation.getOdsId() == null) {
      throw new AnnotationValidationException("\"ods:id\" not provided for annotation update");
    }
  }


}
