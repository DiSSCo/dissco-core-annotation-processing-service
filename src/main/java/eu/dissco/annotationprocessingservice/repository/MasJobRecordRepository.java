package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.database.jooq.Tables.MAS_JOB_RECORD;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.database.jooq.enums.MjrJobState;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.Record1;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
@Slf4j
public class MasJobRecordRepository {

  private final DSLContext context;
  private final ObjectMapper mapper;

  public void markMasJobRecordAsFailed(String jobId) {
    context.update(MAS_JOB_RECORD)
        .set(MAS_JOB_RECORD.JOB_STATE, MjrJobState.FAILED)
        .set(MAS_JOB_RECORD.TIME_COMPLETED, Instant.now())
        .where(MAS_JOB_RECORD.JOB_ID.eq(jobId))
        .execute();
  }

  public boolean getBatchingRequested(String jobId){
    return Boolean.TRUE.equals(context.select(MAS_JOB_RECORD.BATCHING_REQUESTED)
        .from(MAS_JOB_RECORD)
        .where(MAS_JOB_RECORD.JOB_ID.eq(jobId))
        .fetchSingle(MAS_JOB_RECORD.BATCHING_REQUESTED));
  }

  public void markMasJobRecordAsComplete(String jobId, JsonNode annotations) {
    try {
      context.update(MAS_JOB_RECORD).set(MAS_JOB_RECORD.JOB_STATE, MjrJobState.COMPLETED)
          .set(MAS_JOB_RECORD.TIME_COMPLETED, Instant.now())
          .set(MAS_JOB_RECORD.ANNOTATIONS, JSONB.jsonb(mapper.writeValueAsString(annotations)))
          .where(MAS_JOB_RECORD.JOB_ID.eq(jobId)).execute();
    } catch (JsonProcessingException e) {
      log.error("Unable to write annotations json node to db");
      throw new DataBaseException("Unable to write annotations json node to db");
    }
  }

}
