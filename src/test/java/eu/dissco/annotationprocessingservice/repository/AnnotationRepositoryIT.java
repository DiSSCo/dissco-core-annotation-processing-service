package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.TestUtils.ANNOTATION_HASH;
import static eu.dissco.annotationprocessingservice.TestUtils.ANNOTATION_HASH_2;
import static eu.dissco.annotationprocessingservice.TestUtils.CREATOR;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenHashedAnnotation;
import static eu.dissco.annotationprocessingservice.database.jooq.Tables.ANNOTATION;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.domain.HashedAnnotation;
import eu.dissco.annotationprocessingservice.domain.annotation.AggregateRating;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.domain.annotation.Body;
import eu.dissco.annotationprocessingservice.domain.annotation.Creator;
import eu.dissco.annotationprocessingservice.domain.annotation.Generator;
import eu.dissco.annotationprocessingservice.domain.annotation.Motivation;
import eu.dissco.annotationprocessingservice.domain.annotation.Target;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
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
    var expected = givenAnnotationProcessed();

    // When
    repository.createAnnotationRecord(expected);
    var actual = getAnnotation(expected.getOdsId());

    // Then
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  void testCreateAnnotationRecordWithHash() throws DataBaseException {
    // Given
    var expected = givenHashedAnnotation();

    // When
    repository.createAnnotationRecord(List.of(expected));
    var result = getAnnotation(expected.annotation().getOdsId());

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
    var updatedAnnotation = givenAnnotationProcessed().withOaMotivation(Motivation.EDITING);

    // When
    repository.createAnnotationRecord(updatedAnnotation);
    var actual = getAnnotation(annotation.getOdsId());

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
  void testGetAnnotationFromHash() {
    // Given
    var altHashedAnnotation = new HashedAnnotation(givenAnnotationProcessed().withOdsId("alt id"),
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

  @Test
  void testRollbackAnnotationList() {
    // Given
    var annotation = givenAnnotationProcessed();
    repository.createAnnotationRecord(annotation);

    // When
    repository.rollbackAnnotations(List.of(annotation.getOdsId()));

    // Then
    var result = repository.getAnnotationById(annotation.getOdsId());
    assertThat(result).isEmpty();
  }

  private Annotation getAnnotation(String annotationId) {
    var dbRecord = context.select(ANNOTATION.asterisk())
        .from(ANNOTATION)
        .where(ANNOTATION.ID.eq(annotationId))
        .fetchOne();
    if (dbRecord == null) {
      return null;
    }
    return dbRecord.map(this::mapAnnotation);
  }

  private Annotation mapAnnotation(Record dbRecord) {
    try {
      return new Annotation()
          .withOdsId(dbRecord.get(ANNOTATION.ID))
          .withRdfType(dbRecord.get(ANNOTATION.TYPE))
          .withOdsVersion(dbRecord.get(ANNOTATION.VERSION))
          .withOaMotivation(Motivation.fromString(dbRecord.get(ANNOTATION.MOTIVATION)))
          .withOaMotivatedBy(dbRecord.get(ANNOTATION.MOTIVATED_BY))
          .withOaTarget(mapper.readValue(dbRecord.get(ANNOTATION.TARGET).data(), Target.class))
          .withOaBody(mapper.readValue(dbRecord.get(ANNOTATION.BODY).data(), Body.class))
          .withOaCreator(mapper.readValue(dbRecord.get(ANNOTATION.CREATOR).data(), Creator.class))
          .withDcTermsCreated(dbRecord.get(ANNOTATION.CREATED))
          .withOdsDeletedOn(dbRecord.get(ANNOTATION.DELETED_ON))
          .withAsGenerator(
              mapper.readValue(dbRecord.get(ANNOTATION.GENERATOR).data(), Generator.class))
          .withOaGenerated(dbRecord.get(ANNOTATION.GENERATED))
          .withOdsAggregateRating(mapper.readValue(dbRecord.get(ANNOTATION.AGGREGATE_RATING).data(),
              AggregateRating.class))
          .withOdsJobId(dbRecord.get(ANNOTATION.MJR_JOB_ID));
    } catch (JsonProcessingException ignored) {
      return null;
    }
  }

}
