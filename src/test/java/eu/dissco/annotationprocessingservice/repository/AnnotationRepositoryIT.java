package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.TestUtils.ANNOTATION_HASH;
import static eu.dissco.annotationprocessingservice.TestUtils.CREATOR;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.JOB_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessedAlt;
import static eu.dissco.annotationprocessingservice.TestUtils.givenCreator;
import static eu.dissco.annotationprocessingservice.database.jooq.Tables.ANNOTATION;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.domain.HashedAnnotation;
import eu.dissco.annotationprocessingservice.domain.annotation.Motivation;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import java.util.List;
import java.util.Set;
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
  void testCreateAnnotationRecord() throws DataBaseException {
    // Given
    var annotation = givenAnnotationProcessed();

    // When
    repository.createAnnotationRecord(annotation);
    var actual = repository.getAnnotation(annotation.getOdsId());

    // Then
    assertThat(actual).isEqualTo(annotation);
  }

  @Test
  void testCreateAnnotationRecordWithHash() throws DataBaseException {
    // Given
    var annotation = givenAnnotationProcessed();

    // When
    repository.createAnnotationRecord(annotation, ANNOTATION_HASH);
    var result = context.select(ANNOTATION.asterisk()).from(ANNOTATION).fetchOne();

    // Then
    assertThat(result.get(ANNOTATION.ANNOTATION_HASH)).isEqualTo(ANNOTATION_HASH);
    assertThat(result.get(ANNOTATION.ID)).isEqualTo(ID);
  }

  @Test
  void testGetAnnotationForUser() {
    // Given
    var expected = givenAnnotationProcessed();
    var altAnnotation = givenAnnotationProcessed("alt id", "alt user", "alt target");
    repository.createAnnotationRecord(expected);
    repository.createAnnotationRecord(altAnnotation);

    // When
    var result = repository.getAnnotationForUser(ID, CREATOR);

    // Then
    assertThat(result).isPresent().contains(expected);
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
  void testUpdateLastChecked() {
    // Given
    var annotation = givenAnnotationProcessed();
    repository.createAnnotationRecord(annotation);
    var initInstant = context.select(ANNOTATION.LAST_CHECKED).from(ANNOTATION)
        .where(ANNOTATION.ID.eq(annotation.getOdsId())).fetchOne(Record1::value1);

    // When
    repository.updateLastChecked(List.of(annotation.getOdsId()));

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
  void testGetAnnotation() {
    // Given
    var annotation = givenAnnotationProcessed();
    repository.createAnnotationRecord(annotation);

    // When
    var result = repository.getAnnotation(annotation.getOdsId());

    // Then
    assertThat(result).isEqualTo(annotation);
  }

  @Test
  void testGetAnnotationFromHash() {
    // Given
    repository.createAnnotationRecord(givenAnnotationProcessed(), ANNOTATION_HASH);
    repository.createAnnotationRecord(givenAnnotationProcessedAlt().withOdsId("alt id"), JOB_ID);

    // When
    var result = repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH));

    // Then
    assertThat(result).isEqualTo(List.of(new HashedAnnotation(givenAnnotationProcessed(), ANNOTATION_HASH)));
  }

  @Test
  void testGetAnnotationFromHashEmpty() {
    // Given
    repository.createAnnotationRecord(givenAnnotationProcessed(), ANNOTATION_HASH);

    // When
    var result = repository.getAnnotationFromHash(Set.of(JOB_ID));

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  void testGetAnnotationIsEmpty() {
    // When
    var result = repository.getAnnotation(givenAnnotationProcessed());

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  void testGetAnnotationIsNull() {
    // When
    var result = repository.getAnnotation(ID);

    // Then
    assertThat(result).isNull();
  }

  @Test
  void testArchiveAnnotation() {
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
