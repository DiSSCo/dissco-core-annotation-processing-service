package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.database.jooq.Tables.MAS_JOB_RECORD;

import eu.dissco.annotationprocessingservice.database.jooq.enums.ErrorCode;
import eu.dissco.annotationprocessingservice.database.jooq.enums.JobState;
import eu.dissco.annotationprocessingservice.domain.MasJobRecord;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.Record3;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@Repository
@RequiredArgsConstructor
@Slf4j
public class MasJobRecordRepository {

  private final DSLContext context;
  private final JsonMapper mapper;

  public void markMasJobRecordAsFailed(String jobId, ErrorCode errorCode, String errorMessage) {
    context.update(MAS_JOB_RECORD)
        .set(MAS_JOB_RECORD.JOB_STATE, JobState.FAILED)
        .set(MAS_JOB_RECORD.TIME_COMPLETED, Instant.now())
        .set(MAS_JOB_RECORD.ERROR, errorCode)
        .set(MAS_JOB_RECORD.ERROR_MESSAGE, errorMessage)
        .where(MAS_JOB_RECORD.JOB_ID.eq(jobId))
        .execute();
  }

  public MasJobRecord getMasJobRecord(String jobId) {
    return context.select(MAS_JOB_RECORD.JOB_ID, MAS_JOB_RECORD.BATCHING_REQUESTED,
            MAS_JOB_RECORD.ERROR)
        .from(MAS_JOB_RECORD)
        .where(MAS_JOB_RECORD.JOB_ID.eq(jobId))
        .fetchSingle(this::mapToMjr);
  }

  public void markMasJobRecordAsComplete(String jobId, JsonNode annotations) {
    context.update(MAS_JOB_RECORD).set(MAS_JOB_RECORD.JOB_STATE, JobState.COMPLETED)
        .set(MAS_JOB_RECORD.TIME_COMPLETED, Instant.now())
        .set(MAS_JOB_RECORD.ANNOTATIONS, JSONB.jsonb(mapper.writeValueAsString(annotations)))
        .set(MAS_JOB_RECORD.ERROR, (ErrorCode) null)
        .where(MAS_JOB_RECORD.JOB_ID.eq(jobId)).execute();

  }

  private MasJobRecord mapToMjr(Record3<String, Boolean, ErrorCode> mjr) {
    return new MasJobRecord(
        mjr.value1(),
        Boolean.TRUE.equals(mjr.value2()),
        mjr.value3()
    );
  }

}
