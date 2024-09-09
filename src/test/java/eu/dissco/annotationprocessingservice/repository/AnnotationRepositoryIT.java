package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.TestUtils.ANNOTATION_HASH;
import static eu.dissco.annotationprocessingservice.TestUtils.ANNOTATION_HASH_2;
import static eu.dissco.annotationprocessingservice.TestUtils.BARE_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.CREATOR;
import static eu.dissco.annotationprocessingservice.TestUtils.HANDLE_PROXY;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenHashedAnnotation;
import static eu.dissco.annotationprocessingservice.database.jooq.Tables.ANNOTATION;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.domain.HashedAnnotation;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.schema.Annotation;
import eu.dissco.annotationprocessingservice.schema.Annotation.OaMotivation;
import java.util.List;
import java.util.Set;
import org.jooq.Record;
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
    var expected = givenAnnotationProcessed(ID);

    // When
    repository.createAnnotationRecord(expected);
    var actual = getAnnotation(ID);

    // Then
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  void testCreateAnnotationRecordWithHash() throws DataBaseException {
    // Given
    var expected = givenHashedAnnotation(BARE_ID);

    // When
    repository.createAnnotationRecord(List.of(expected));
    var result = getAnnotation(expected.annotation().getId());

    // Then
    assertThat(result).isEqualTo(expected.annotation());
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
    var updatedAnnotation = givenAnnotationProcessed().withOaMotivation(OaMotivation.OA_EDITING);

    // When
    repository.createAnnotationRecord(updatedAnnotation);
    var actual = getAnnotation(annotation.getId());

    // Then
    assertThat(actual).isEqualTo(updatedAnnotation);
  }

  @Test
  void testUpdateLastChecked() {
    // Given
    var annotation = givenAnnotationProcessed();
    repository.createAnnotationRecord(annotation);
    var initInstant = context.select(ANNOTATION.LAST_CHECKED).from(ANNOTATION)
        .where(ANNOTATION.ID.eq(annotation.getId().replace(HANDLE_PROXY, "")))
        .fetchOne(Record1::value1);

    // When
    repository.updateLastChecked(List.of(annotation.getId()));

    // Then
    var updatedTimestamp = context.select(ANNOTATION.LAST_CHECKED).from(ANNOTATION)
        .where(ANNOTATION.ID.eq(annotation.getId().replace(HANDLE_PROXY, "")))
        .fetchOne(Record1::value1);
    assertThat(updatedTimestamp).isAfter(initInstant);
  }

  @Test
  void testGetAnnotationById() {
    // Given
    var annotation = givenAnnotationProcessed(BARE_ID);
    repository.createAnnotationRecord(annotation);

    // When
    var result = repository.getAnnotationById(ID);

    // Then
    assertThat(result).hasValue(annotation.getId());
  }

  @Test
  void testGetAnnotationFromHash() {
    // Given
    var altHashedAnnotation = new HashedAnnotation(givenAnnotationProcessed().withId("alt id"),
        ANNOTATION_HASH_2);
    repository.createAnnotationRecord(List.of(givenHashedAnnotation(), altHashedAnnotation));

    // When
    var result = repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH));

    // Then
    assertThat(result).isEqualTo(
        List.of(new HashedAnnotation(givenAnnotationProcessed(), ANNOTATION_HASH)));
  }

  @Test
  void testGetAnnotationFromHashEmpty() {
    // Given
    repository.createAnnotationRecord(List.of(givenHashedAnnotation()));

    // When
    var result = repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH_2));

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  void testArchiveAnnotation() throws FailedProcessingException {
    // Given
    var annotation = givenAnnotationProcessed(BARE_ID);
    repository.createAnnotationRecord(annotation);

    // When
    repository.archiveAnnotation(givenAnnotationProcessed());

    // Then
    var deletedTimestamp = context.select(ANNOTATION.TOMBSTONED_ON).from(ANNOTATION)
        .where(ANNOTATION.ID.eq(annotation.getId())).fetchOne(Record1::value1);
    assertThat(deletedTimestamp).isNotNull();
  }

  @Test
  void testRollbackAnnotation() {
    // Given
    var annotation = givenAnnotationProcessed();
    repository.createAnnotationRecord(annotation);

    // When
    repository.rollbackAnnotation(annotation.getId());

    // Then
    var result = repository.getAnnotationById(annotation.getId());
    assertThat(result).isEmpty();
  }

  @Test
  void testRollbackAnnotationList() {
    // Given
    var annotation = givenAnnotationProcessed();
    repository.createAnnotationRecord(annotation);

    // When
    repository.rollbackAnnotations(List.of(annotation.getId()));

    // Then
    var result = repository.getAnnotationById(annotation.getId());
    assertThat(result).isEmpty();
  }

  private Annotation getAnnotation(String annotationId) {
    var dbRecord = context.select(ANNOTATION.asterisk())
        .from(ANNOTATION)
        .where(ANNOTATION.ID.eq(annotationId.replace(HANDLE_PROXY, "")))
        .fetchOne();
    if (dbRecord == null) {
      return null;
    }
    return dbRecord.map(this::mapAnnotation);
  }

  private Annotation mapAnnotation(Record dbRecord) {
    try {
      return mapper.readValue(dbRecord.get(ANNOTATION.DATA).data(), Annotation.class);
    } catch (JsonProcessingException ignored) {
      return null;
    }
  }

}
