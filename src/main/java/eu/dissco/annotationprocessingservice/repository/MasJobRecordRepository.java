package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.database.jooq.Tables.MAS_JOB_RECORD;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.database.jooq.enums.ErrorCode;
import eu.dissco.annotationprocessingservice.database.jooq.enums.JobState;
import eu.dissco.annotationprocessingservice.domain.MasJobRecord;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.Record3;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
@Slf4j
public class MasJobRecordRepository {

  private final DSLContext context;
  private final ObjectMapper mapper;

  public void markMasJobRecordAsFailed(String jobId) {
    context.update(MAS_JOB_RECORD)
        .set(MAS_JOB_RECORD.JOB_STATE, JobState.FAILED)
        .set(MAS_JOB_RECORD.TIME_COMPLETED, Instant.now())
        .where(MAS_JOB_RECORD.JOB_ID.eq(jobId))
        .execute();
  }

  public MasJobRecord getMasJobRecord(String jobId) {
    return context.select(MAS_JOB_RECORD.JOB_ID, MAS_JOB_RECORD.BATCHING_REQUESTED, MAS_JOB_RECORD.ERROR)
        .from(MAS_JOB_RECORD)
        .where(MAS_JOB_RECORD.JOB_ID.eq(jobId))
        .fetchSingle(this::mapToMjr);
  }

  public void markMasJobRecordAsComplete(String jobId, JsonNode annotations) {
    try {
      context.update(MAS_JOB_RECORD).set(MAS_JOB_RECORD.JOB_STATE, JobState.COMPLETED)
          .set(MAS_JOB_RECORD.TIME_COMPLETED, Instant.now())
          .set(MAS_JOB_RECORD.ANNOTATIONS, JSONB.jsonb(mapper.writeValueAsString(annotations)))
          .set(MAS_JOB_RECORD.ERROR, (ErrorCode) null)
          .where(MAS_JOB_RECORD.JOB_ID.eq(jobId)).execute();
    } catch (JsonProcessingException e) {
      log.error("Unable to write annotations json node to db");
      throw new DataBaseException("Unable to write annotations json node to db");
    }
  }

  private MasJobRecord mapToMjr(Record3<String, Boolean, ErrorCode> mjr) {
    return new MasJobRecord(
        mjr.value1(),
        Boolean.TRUE.equals(mjr.value2()),
        mjr.value3()
    );
  }

}
