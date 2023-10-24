package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.database.jooq.Tables.MAS_JOB_RECORD;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.domain.AnnotationState;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import java.time.Instant;
import java.util.UUID;
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

  public void markMasJobRecordAsFailed(UUID jobId) {
    context.update(MAS_JOB_RECORD)
        .set(MAS_JOB_RECORD.STATE, AnnotationState.FAILED.getState())
        .set(MAS_JOB_RECORD.TIME_COMPLETED, Instant.now())
        .where(MAS_JOB_RECORD.JOB_ID.eq(jobId))
        .execute();
  }

  public void markMasJobRecordAsComplete(UUID jobId, JsonNode annotations) {
    try {
      context.update(MAS_JOB_RECORD).set(MAS_JOB_RECORD.STATE, AnnotationState.COMPLETED.getState())
          .set(MAS_JOB_RECORD.TIME_COMPLETED, Instant.now())
          .set(MAS_JOB_RECORD.ANNOTATIONS, JSONB.jsonb(mapper.writeValueAsString(annotations)))
          .where(MAS_JOB_RECORD.JOB_ID.eq(jobId)).execute();
    } catch (JsonProcessingException e) {
      log.error("Unable to write annotation json node to db");
      throw new DataBaseException("Unable to write annotation json node to db");
    }
  }

}
