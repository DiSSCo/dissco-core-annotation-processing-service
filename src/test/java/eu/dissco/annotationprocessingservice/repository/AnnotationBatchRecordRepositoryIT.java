package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.TestUtils.BATCH_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.BATCH_ID_ALT;
import static eu.dissco.annotationprocessingservice.TestUtils.CREATED;
import static eu.dissco.annotationprocessingservice.TestUtils.CREATOR;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.JOB_ID;
import static eu.dissco.annotationprocessingservice.database.jooq.Tables.ANNOTATION_BATCH_RECORD;
import static eu.dissco.annotationprocessingservice.database.jooq.Tables.MAS_JOB_RECORD;
import static org.assertj.core.api.Assertions.assertThat;

import eu.dissco.annotationprocessingservice.domain.AnnotationBatchRecord;
import java.util.List;
import java.util.Set;
import org.jooq.Record;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AnnotationBatchRecordRepositoryIT extends BaseRepositoryIT {

  private AnnotationBatchRecordRepository repository;

  @BeforeEach
  void setup() {
    repository = new AnnotationBatchRecordRepository(context);
  }

  @AfterEach
  void destroy() {
    context.truncate(ANNOTATION_BATCH_RECORD).cascade().execute();
    context.truncate(MAS_JOB_RECORD).cascade().execute();
  }


  @Test
  void testCreateAnnotationBatchRecord() {
    // Given
    postMjr(JOB_ID);

    // When
    repository.createAnnotationBatchRecord(List.of(givenAnnotationBatchRecord()));
    var result = context
        .select(ANNOTATION_BATCH_RECORD.asterisk())
        .from(ANNOTATION_BATCH_RECORD)
        .fetchOne(this::toAnnotationBatchRecord);

    // Then
    assertThat(result).isEqualTo(givenAnnotationBatchRecord());
  }

  @Test
  void testUpdateAnnotationBatchRecord() {
    // Given
    var batchCount = 4L;
    postMjr(JOB_ID);
    repository.createAnnotationBatchRecord(List.of(givenAnnotationBatchRecord()));

    // When
    repository.updateAnnotationBatchRecord(BATCH_ID, batchCount);
    var result = context.select(ANNOTATION_BATCH_RECORD.BATCH_QUANTITY)
        .from(ANNOTATION_BATCH_RECORD)
        .fetchOne(ANNOTATION_BATCH_RECORD.BATCH_QUANTITY, Long.class);

    // Then
    assertThat(result).isEqualTo(batchCount + 1L);
  }

  @Test
  void testRollbackAnnotationBatchRecord() {
    // Given
    postMjr(JOB_ID);
    repository.createAnnotationBatchRecord(List.of(givenAnnotationBatchRecord()));

    // When
    repository.rollbackAnnotationBatchRecord(Set.of(BATCH_ID));
    var result = context.select(ANNOTATION_BATCH_RECORD.asterisk())
        .from(ANNOTATION_BATCH_RECORD)
        .fetchOptional();

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  void testRollbackAnnotationBatchRecordWontDeleteUpdatedBatches() {
    // Given
    postMjr(JOB_ID);
    var updatedAnnotationBatchRecord = new AnnotationBatchRecord(BATCH_ID_ALT, CREATOR, ID, CREATED, JOB_ID);
    repository.createAnnotationBatchRecord(List.of(givenAnnotationBatchRecord(), updatedAnnotationBatchRecord));
    repository.updateAnnotationBatchRecord(BATCH_ID_ALT, 5L);

    // When
    repository.rollbackAnnotationBatchRecord(Set.of(BATCH_ID));
    repository.rollbackAnnotationBatchRecord(Set.of(BATCH_ID_ALT));
    var result = context.select(ANNOTATION_BATCH_RECORD.asterisk())
        .from(ANNOTATION_BATCH_RECORD)
        .fetch(this::toAnnotationBatchRecord);
    // Then
    assertThat(result).isEqualTo(List.of(updatedAnnotationBatchRecord));
  }

  private static AnnotationBatchRecord givenAnnotationBatchRecord() {
    return new AnnotationBatchRecord(
        BATCH_ID,
        CREATOR,
        ID,
        CREATED,
        JOB_ID
    );
  }

  private AnnotationBatchRecord toAnnotationBatchRecord(Record dbRecord) {
    return new AnnotationBatchRecord(
        dbRecord.get(ANNOTATION_BATCH_RECORD.ID),
        dbRecord.get(ANNOTATION_BATCH_RECORD.CREATOR),
        dbRecord.get(ANNOTATION_BATCH_RECORD.PARENT_ANNOTATION_ID),
        dbRecord.get(ANNOTATION_BATCH_RECORD.CREATED),
        dbRecord.get(ANNOTATION_BATCH_RECORD.JOB_ID));
  }

}
