package eu.dissco.annotationprocessingservice.repository;


import static eu.dissco.annotationprocessingservice.TestUtils.ANNOTATION_JSONB;
import static eu.dissco.annotationprocessingservice.TestUtils.CREATED;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.ID_ALT;
import static eu.dissco.annotationprocessingservice.TestUtils.JOB_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.TARGET_ID;
import static eu.dissco.annotationprocessingservice.database.jooq.Tables.MAS_JOB_RECORD_TMP;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import eu.dissco.annotationprocessingservice.database.jooq.enums.MjrJobState;
import eu.dissco.annotationprocessingservice.database.jooq.enums.MjrTargetType;
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
    context.truncate(MAS_JOB_RECORD_TMP).execute();
  }

  @Test
  void testMarkMasJobRecordAsFailed() {
    // Given
    postMjr(JOB_ID);
    postMjr(ID_ALT);

    // When
    repository.markMasJobRecordAsFailed(JOB_ID);
    var result = context.select(MAS_JOB_RECORD_TMP.JOB_ID, MAS_JOB_RECORD_TMP.JOB_STATE,
            MAS_JOB_RECORD_TMP.TIME_COMPLETED)
        .from(MAS_JOB_RECORD_TMP)
        .where(MAS_JOB_RECORD_TMP.JOB_ID.eq(JOB_ID))
        .fetchSingle();

    // Then
    assertThat(result.value2()).isEqualTo(MjrJobState.FAILED);
    assertThat(result.value3()).isNotNull();
  }

  @Test
  void testMarkMasJobRecordAsComplete() throws Exception {
    // Given
    postMjr(JOB_ID);
    postMjr(ID_ALT);
    var annotations = MAPPER.readTree(ANNOTATION_JSONB);

    // When
    repository.markMasJobRecordAsComplete(JOB_ID, annotations);
    var result = context.select(MAS_JOB_RECORD_TMP.JOB_ID, MAS_JOB_RECORD_TMP.JOB_STATE,
            MAS_JOB_RECORD_TMP.TIME_COMPLETED, MAS_JOB_RECORD_TMP.ANNOTATIONS)
        .from(MAS_JOB_RECORD_TMP)
        .where(MAS_JOB_RECORD_TMP.JOB_ID.eq(JOB_ID))
        .fetchSingle();

    // Then
    assertThat(result.value2()).isEqualTo(MjrJobState.COMPLETED);
    assertThat(result.value3()).isNotNull();
    assertThat(result.value4()).isEqualTo(JSONB.jsonb(ANNOTATION_JSONB));
  }

  private void postMjr(String jobId) {
    context.insertInto(MAS_JOB_RECORD_TMP, MAS_JOB_RECORD_TMP.JOB_ID, MAS_JOB_RECORD_TMP.JOB_STATE,
            MAS_JOB_RECORD_TMP.MAS_ID, MAS_JOB_RECORD_TMP.TARGET_ID, MAS_JOB_RECORD_TMP.TARGET_TYPE,
            MAS_JOB_RECORD_TMP.TIME_STARTED, MAS_JOB_RECORD_TMP.BATCHING_REQUESTED)
        .values(jobId, MjrJobState.SCHEDULED, ID, TARGET_ID, MjrTargetType.DIGITAL_SPECIMEN,
            CREATED, false)
        .execute();
  }
}
