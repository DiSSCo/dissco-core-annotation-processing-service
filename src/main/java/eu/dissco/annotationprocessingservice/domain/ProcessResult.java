package eu.dissco.annotationprocessingservice.domain;

import eu.dissco.annotationprocessingservice.database.jooq.tables.Annotation;
import java.util.List;

public record ProcessResult(
    List<Annotation> equalAnnotations,
    List<Annotation> updatedAnnotations,
    List<Annotation> newAnnotations
) {

}
