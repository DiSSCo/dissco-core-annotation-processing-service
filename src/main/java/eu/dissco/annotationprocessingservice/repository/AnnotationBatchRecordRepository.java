package eu.dissco.annotationprocessingservice.repository;

import eu.dissco.annotationprocessingservice.domain.AnnotationBatchRecord;

import static eu.dissco.annotationprocessingservice.database.jooq.Tables.ANNOTATION_BATCH_RECORD;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.springframework.stereotype.Repository;

@Slf4j
@RequiredArgsConstructor
@Repository
public class AnnotationBatchRecordRepository {

  private final DSLContext context;

  public void createAnnotationBatchRecord(AnnotationBatchRecord annotationBatchRecord) {
    context.insertInto(ANNOTATION_BATCH_RECORD)
        .set(ANNOTATION_BATCH_RECORD.BATCH_ID, annotationBatchRecord.batchId())
        .set(ANNOTATION_BATCH_RECORD.CREATOR_USER, annotationBatchRecord.userId())
        .set(ANNOTATION_BATCH_RECORD.CREATOR_MAS, annotationBatchRecord.masId())
        .set(ANNOTATION_BATCH_RECORD.PARENT_ANNOTATION_ID,
            annotationBatchRecord.parentAnnotationId())
        .set(ANNOTATION_BATCH_RECORD.CREATED_ON, annotationBatchRecord.createdOn())
        .set(ANNOTATION_BATCH_RECORD.LAST_UPDATED, annotationBatchRecord.createdOn())
        .set(ANNOTATION_BATCH_RECORD.JOB_ID, annotationBatchRecord.jobId())
        .set(ANNOTATION_BATCH_RECORD.BATCH_QUANTITY, 1L)
        .execute();
  }

  public void updateAnnotationBatchRecord(UUID batchId, Long qty) {
    context.update(ANNOTATION_BATCH_RECORD)
        .set(ANNOTATION_BATCH_RECORD.BATCH_QUANTITY,
            ANNOTATION_BATCH_RECORD.BATCH_QUANTITY.plus(qty))
        .set(ANNOTATION_BATCH_RECORD.LAST_UPDATED, Instant.now())
        .where(ANNOTATION_BATCH_RECORD.BATCH_ID.eq(batchId))
        .execute();
  }

  // To be used only if the parent annotation fails
  public void rollbackAnnotationBatchRecord(UUID batchId) {
    context.deleteFrom(ANNOTATION_BATCH_RECORD)
        .where(ANNOTATION_BATCH_RECORD.BATCH_ID.eq(batchId))
        .execute();
  }

  public Optional<UUID> getBatchIdFromMasJobId(String jobId) {
    return context.select(ANNOTATION_BATCH_RECORD.BATCH_ID)
        .from(ANNOTATION_BATCH_RECORD)
        .where(ANNOTATION_BATCH_RECORD.JOB_ID.eq(jobId))
        .fetchOptional(Record1::value1);
  }

}
