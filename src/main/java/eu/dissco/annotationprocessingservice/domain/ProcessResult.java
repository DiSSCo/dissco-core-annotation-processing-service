package eu.dissco.annotationprocessingservice.domain;

import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import java.util.List;
import java.util.Set;

public record ProcessResult(
    Set<Annotation> equalAnnotations,
    Set<UpdatedAnnotation> changedAnnotations,
    List<HashedAnnotation> newAnnotations
) {

}
