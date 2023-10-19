package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.database.jooq.Tables.ANNOTATION;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.domain.annotation.Motivation;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import org.jooq.Record1;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AnnotationRepositoryIT extends BaseRepositoryIT {

  private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
  private AnnotationRepository repository;

  @BeforeEach
  void setup() {
    repository = new AnnotationRepository(mapper, context);
  }

  @AfterEach
  void destroy() {
    context.truncate(ANNOTATION).execute();
  }

  @Test
  void createAnnotationRecord() throws JsonProcessingException, DataBaseException {
    // Given
    var annotation = givenAnnotationProcessed();

    // When
    repository.createAnnotationRecord(annotation);
    var actual = repository.getAnnotation(annotation.getOdsId());


    // Then
    assertThat(actual).isEqualTo(annotation);
  }

  @Test
  void updateAnnotationRecord() throws DataBaseException {
    // Given
    var annotation = givenAnnotationProcessed();
    repository.createAnnotationRecord(annotation);
    var updatedAnnotation = givenAnnotationProcessed().withOaMotivation(Motivation.EDITING);

    // When
    repository.createAnnotationRecord(updatedAnnotation);
    var actual = repository.getAnnotation(annotation.getOdsId());

    // Then
    assertThat(actual).isEqualTo(updatedAnnotation);
  }

  @Test
  void testUpdateLastChecked() throws JsonProcessingException {
    // Given
    var annotation = givenAnnotationProcessed();
    repository.createAnnotationRecord(annotation);
    var initInstant = context.select(ANNOTATION.LAST_CHECKED).from(ANNOTATION)
        .where(ANNOTATION.ID.eq(annotation.getOdsId())).fetchOne(Record1::value1);

    // When
    repository.updateLastChecked(annotation);

    // Then
    var updatedTimestamp = context.select(ANNOTATION.LAST_CHECKED).from(ANNOTATION)
        .where(ANNOTATION.ID.eq(annotation.getOdsId())).fetchOne(Record1::value1);
    assertThat(updatedTimestamp).isAfter(initInstant);
  }

  @Test
  void testGetAnnotationById() {
    // Given
    var annotation = givenAnnotationProcessed();
    repository.createAnnotationRecord(annotation);

    // When
    var result = repository.getAnnotationById(annotation.getOdsId());

    // Then
    assertThat(result).hasValue(annotation.getOdsId());
  }

  @Test
  void testGetAnnotation(){
    // Given
    var annotation = givenAnnotationProcessed();
    repository.createAnnotationRecord(annotation);

    // When
    var result = repository.getAnnotation(annotation.getOdsId());

    // Then
    assertThat(result).isEqualTo(annotation);
  }

  @Test
  void testGetAnnotationIsNull(){
    // When
    var result = repository.getAnnotation(givenAnnotationProcessed());

    // Then
    assertThat(result).isNull();
  }

  @Test
  void testArchiveAnnotation() throws JsonProcessingException {
    // Given
    var annotation = givenAnnotationProcessed();
    repository.createAnnotationRecord(annotation);

    // When
    repository.archiveAnnotation(annotation.getOdsId());

    // Then
    var deletedTimestamp = context.select(ANNOTATION.DELETED_ON).from(ANNOTATION)
        .where(ANNOTATION.ID.eq(annotation.getOdsId())).fetchOne(Record1::value1);
    assertThat(deletedTimestamp).isNotNull();
  }

  @Test
  void testRollbackAnnotation() {
    // Given
    var annotation = givenAnnotationProcessed();
    repository.createAnnotationRecord(annotation);

    // When
    repository.rollbackAnnotation(annotation.getOdsId());

    // Then
    var result = repository.getAnnotationById(annotation.getOdsId());
    assertThat(result).isEmpty();
  }

}
