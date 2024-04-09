package eu.dissco.annotationprocessingservice.repository;


import static eu.dissco.annotationprocessingservice.TestUtils.ANNOTATION_JSONB;
import static eu.dissco.annotationprocessingservice.TestUtils.CREATED;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.ID_ALT;
import static eu.dissco.annotationprocessingservice.TestUtils.JOB_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.TARGET_ID;
import static eu.dissco.annotationprocessingservice.database.jooq.Tables.MAS_JOB_RECORD;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import eu.dissco.annotationprocessingservice.database.jooq.enums.ErrorCode;
import eu.dissco.annotationprocessingservice.database.jooq.enums.JobState;
import eu.dissco.annotationprocessingservice.database.jooq.enums.MjrTargetType;
import eu.dissco.annotationprocessingservice.domain.MasJobRecord;
import org.jooq.JSONB;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MasJobRecordRepositoryIT extends BaseRepositoryIT {

  private MasJobRecordRepository repository;

  @BeforeEach
  void setup() {
    repository = new MasJobRecordRepository(context, MAPPER);
  }

  @AfterEach
  void destroy() {
    context.truncate(MAS_JOB_RECORD).execute();
  }

  @Test
  void testMarkMasJobRecordAsFailed() {
    // Given
    postMjr(JOB_ID);
    postMjr(ID_ALT);

    // When
    repository.markMasJobRecordAsFailed(JOB_ID);
    var result = context.select(MAS_JOB_RECORD.JOB_ID, MAS_JOB_RECORD.JOB_STATE,
            MAS_JOB_RECORD.TIME_COMPLETED)
        .from(MAS_JOB_RECORD)
        .where(MAS_JOB_RECORD.JOB_ID.eq(JOB_ID))
        .fetchSingle();

    // Then
    assertThat(result.value2()).isEqualTo(JobState.FAILED);
    assertThat(result.value3()).isNotNull();
  }

  @Test
  void testMarkMasJobRecordAsComplete() throws Exception {
    // Given
    context.insertInto(MAS_JOB_RECORD, MAS_JOB_RECORD.JOB_ID, MAS_JOB_RECORD.JOB_STATE,
            MAS_JOB_RECORD.MAS_ID, MAS_JOB_RECORD.TARGET_ID, MAS_JOB_RECORD.TARGET_TYPE,
            MAS_JOB_RECORD.TIME_STARTED, MAS_JOB_RECORD.BATCHING_REQUESTED, MAS_JOB_RECORD.ERROR)
        .values(JOB_ID, JobState.SCHEDULED, ID, TARGET_ID, MjrTargetType.DIGITAL_SPECIMEN,
            CREATED, false, ErrorCode.TIMEOUT)
        .execute();
    postMjr(ID_ALT);
    var annotations = MAPPER.readTree(ANNOTATION_JSONB);

    // When
    repository.markMasJobRecordAsComplete(JOB_ID, annotations);
    var result = context.select(MAS_JOB_RECORD.JOB_ID, MAS_JOB_RECORD.JOB_STATE,
            MAS_JOB_RECORD.TIME_COMPLETED, MAS_JOB_RECORD.ANNOTATIONS, MAS_JOB_RECORD.ERROR)
        .from(MAS_JOB_RECORD)
        .where(MAS_JOB_RECORD.JOB_ID.eq(JOB_ID))
        .fetchSingle();

    // Then
    assertThat(result.value2()).isEqualTo(JobState.COMPLETED);
    assertThat(result.value3()).isNotNull();
    assertThat(result.value4()).isEqualTo(JSONB.jsonb(ANNOTATION_JSONB));
    assertThat(result.value5()).isNull();
  }

  @Test
  void testGetBatchingRequested() {
    // Given
    postMjr(JOB_ID);
    var expected = new MasJobRecord(false, null);

    // When
    var result = repository.getMasJobRecord(JOB_ID);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  private void postMjr(String jobId) {
    context.insertInto(MAS_JOB_RECORD, MAS_JOB_RECORD.JOB_ID, MAS_JOB_RECORD.JOB_STATE,
            MAS_JOB_RECORD.MAS_ID, MAS_JOB_RECORD.TARGET_ID, MAS_JOB_RECORD.TARGET_TYPE,
            MAS_JOB_RECORD.TIME_STARTED, MAS_JOB_RECORD.BATCHING_REQUESTED)
        .values(jobId, JobState.SCHEDULED, ID, TARGET_ID, MjrTargetType.DIGITAL_SPECIMEN,
            CREATED, false)
        .execute();
  }
}
