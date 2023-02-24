package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.TestUtils.CREATOR;
import static eu.dissco.annotationprocessingservice.TestUtils.MOTIVATION;
import static eu.dissco.annotationprocessingservice.TestUtils.generateTarget;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotatioNRecord;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRecord;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AnnotationRepositoryIT extends BaseRepositoryIT {

  private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
  private AnnotationRepository repository;

  @BeforeEach
  void setup() {
    repository = new AnnotationRepository(mapper, context);
  }


  @Test
  void createAnnotationRecord() throws JsonProcessingException {
    // Given
    var annotation = givenAnnotationRecord();

    // When
    var result = repository.createAnnotationRecord(annotation);

    // Then
    var actual = repository.getAnnotation(generateTarget(), CREATOR, MOTIVATION);
    assertThat(result).isEqualTo(1);
    assertThat(actual).contains(annotation);
  }

  @Test
  void updateAnnotationRecord() throws JsonProcessingException {
    // Given
    var annotation = givenAnnotationRecord();
    repository.createAnnotationRecord(annotation);
    var updatedAnnotation = givenAnnotatioNRecord("new_motivation");

    // When
    var result = repository.createAnnotationRecord(updatedAnnotation);

    // Then
    var actual = repository.getAnnotation(generateTarget(), CREATOR, MOTIVATION);
    assertThat(result).isEqualTo(1);
    assertThat(actual).contains(updatedAnnotation);
  }

}
