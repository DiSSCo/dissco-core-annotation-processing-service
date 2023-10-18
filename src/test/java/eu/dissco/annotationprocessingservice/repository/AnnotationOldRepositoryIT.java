package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.TestUtils.CREATOR;
import static eu.dissco.annotationprocessingservice.TestUtils.MOTIVATION;
import static eu.dissco.annotationprocessingservice.TestUtils.generateTarget;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRecord;
import static eu.dissco.annotationprocessingservice.database.jooq.Tables.NEW_ANNOTATION;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import org.jooq.Record1;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AnnotationOldRepositoryIT extends BaseRepositoryIT {

  private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
  private AnnotationRepository repository;

  @BeforeEach
  void setup() {
    repository = new AnnotationRepository(mapper, context);
  }

  @AfterEach
  void destroy() {
    context.truncate(NEW_ANNOTATION).execute();
  }

  @Test
  void createAnnotationRecord() throws JsonProcessingException, DataBaseException {
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
  void updateAnnotationRecord() throws JsonProcessingException, DataBaseException {
    // Given
    var annotation = givenAnnotationRecord();
    repository.createAnnotationRecord(annotation);
    var updatedAnnotation = givenAnnotationRecord("new_motivation");

    // When
    var result = repository.createAnnotationRecord(updatedAnnotation);

    // Then
    var actual = repository.getAnnotation(generateTarget(), CREATOR, "new_motivation");
    assertThat(result).isEqualTo(1);
    assertThat(actual).contains(updatedAnnotation);
  }

  @Test
  void testUpdateLastChecked() throws JsonProcessingException {
    // Given
    var annotation = givenAnnotationRecord();
    repository.createAnnotationRecord(annotation);
    var initInstant = context.select(NEW_ANNOTATION.LAST_CHECKED).from(NEW_ANNOTATION)
        .where(NEW_ANNOTATION.ID.eq(annotation.id())).fetchOne(Record1::value1);

    // When
    repository.updateLastChecked(annotation);

    // Then
    var updatedTimestamp = context.select(NEW_ANNOTATION.LAST_CHECKED).from(NEW_ANNOTATION)
        .where(NEW_ANNOTATION.ID.eq(annotation.id())).fetchOne(Record1::value1);
    assertThat(updatedTimestamp).isAfter(initInstant);
  }

  @Test
  void testGetAnnotationById() throws JsonProcessingException {
    // Given
    var annotation = givenAnnotationRecord();
    repository.createAnnotationRecord(annotation);

    // When
    var result = repository.getAnnotationById(annotation.id());

    // Then
    assertThat(result).hasValue(annotation.id());
  }

  @Test
  void testArchiveAnnotation() throws JsonProcessingException {
    // Given
    var annotation = givenAnnotationRecord();
    repository.createAnnotationRecord(annotation);

    // When
    repository.archiveAnnotation(annotation.id());

    // Then
    var deletedTimestamp = context.select(NEW_ANNOTATION.DELETED).from(NEW_ANNOTATION)
        .where(NEW_ANNOTATION.ID.eq(annotation.id())).fetchOne(Record1::value1);
    assertThat(deletedTimestamp).isNotNull();
  }

  @Test
  void testRollbackAnnotation() throws JsonProcessingException {
    // Given
    var annotation = givenAnnotationRecord();
    repository.createAnnotationRecord(annotation);

    // When
    repository.archiveAnnotation(annotation.id());

    // Then
    var result = repository.getAnnotationById(annotation.id());
    assertThat(result).isEmpty();
  }

}
