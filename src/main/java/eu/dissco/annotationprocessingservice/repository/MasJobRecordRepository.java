package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.database.jooq.Tables.MAS_JOB_RECORD;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class MasJobRecordRepository {
  private final DSLContext context;

  public void updateMasJobRecord(List<JsonNode> annotationBody, UUID jobId){
    context.update(MAS_JOB_RECORD)
        .set(MAS_JOB_RECORD.TIME_COMPLETED, Instant.now())
        .set(MAS_JOB_RECORD.ANNOTATIONS, JSONB.jsonb(annotationBody.toString()))
        .where(MAS_JOB_RECORD.JOB_ID.eq(jobId));
  }


}
