package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.database.jooq.Tables.MAS_JOB_RECORD_TMP;

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
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
@Slf4j
public class MasJobRecordRepository {

  private final DSLContext context;
  private final ObjectMapper mapper;

  public void markMasJobRecordAsFailed(String jobId) {
    context.update(MAS_JOB_RECORD_TMP)
        .set(MAS_JOB_RECORD_TMP.JOB_STATE, MjrJobState.FAILED)
        .set(MAS_JOB_RECORD_TMP.TIME_COMPLETED, Instant.now())
        .where(MAS_JOB_RECORD_TMP.JOB_ID.eq(jobId))
        .execute();
  }

  public void markMasJobRecordAsComplete(String jobId, JsonNode annotations) {
    try {
      context.update(MAS_JOB_RECORD_TMP).set(MAS_JOB_RECORD_TMP.JOB_STATE, MjrJobState.COMPLETED)
          .set(MAS_JOB_RECORD_TMP.TIME_COMPLETED, Instant.now())
          .set(MAS_JOB_RECORD_TMP.ANNOTATIONS, JSONB.jsonb(mapper.writeValueAsString(annotations)))
          .where(MAS_JOB_RECORD_TMP.JOB_ID.eq(jobId)).execute();
    } catch (JsonProcessingException e) {
      log.error("Unable to write annotations json node to db");
      throw new DataBaseException("Unable to write annotations json node to db");
    }
  }

}
