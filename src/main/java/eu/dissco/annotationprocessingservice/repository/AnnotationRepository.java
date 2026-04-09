package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.database.jooq.Tables.ANNOTATION;
import static eu.dissco.annotationprocessingservice.utils.HandleUtils.removeProxy;

import eu.dissco.annotationprocessingservice.database.jooq.enums.AnnotationStatusEnum;
import eu.dissco.annotationprocessingservice.database.jooq.tables.records.AnnotationRecord;
import eu.dissco.annotationprocessingservice.domain.HashedAnnotation;
import eu.dissco.annotationprocessingservice.schema.Annotation;
import eu.dissco.annotationprocessingservice.utils.HandleUtils;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.InsertOnDuplicateSetMoreStep;
import org.jooq.InsertSetMoreStep;
import org.jooq.JSONB;
import org.jooq.Query;
import org.jooq.Record;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.json.JsonMapper;

@Slf4j
@Repository
@RequiredArgsConstructor
public class AnnotationRepository {

  private final JsonMapper mapper;
  private final DSLContext context;

  public List<HashedAnnotation> getAnnotationFromHash(Set<UUID> annotationHashes) {
    var dbRecord = context.select(ANNOTATION.asterisk())
        .from(ANNOTATION)
        .where(ANNOTATION.ANNOTATION_HASH.in(annotationHashes))
        .fetch();
    return dbRecord.map(this::mapHashedAnnotation);
  }

  public Optional<Annotation> getAnnotationForUser(String annotationId, String creatorId) {
    return context.select(ANNOTATION.asterisk())
        .from(ANNOTATION)
        .where(ANNOTATION.ID.eq(removeProxy(annotationId))
            .and(ANNOTATION.CREATOR.eq(creatorId)))
        .fetchOptional()
        .map(this::mapAnnotation);
  }

  public Annotation getAnnotation(String annotationId) {
    return context.select(ANNOTATION.asterisk())
        .from(ANNOTATION)
        .where(ANNOTATION.ID.eq(removeProxy(annotationId)))
        .fetchSingle(this::mapAnnotation);
  }

  private Annotation mapAnnotation(Record dbRecord) {
    return mapper.readValue(dbRecord.get(ANNOTATION.DATA).data(),
        Annotation.class);
  }

  private HashedAnnotation mapHashedAnnotation(Record dbRecord) {
    return new HashedAnnotation(
        mapAnnotation(dbRecord),
        dbRecord.get(ANNOTATION.ANNOTATION_HASH)
    );
  }

  public void createAnnotationRecord(Annotation annotation) {
    var insertQuery = insertAnnotation(annotation, false);
    var fullQuery = onConflict(annotation, insertQuery, false);
    fullQuery.execute();
  }

  public void createMergedAnnotationRecords(Map<Annotation, Boolean> annotationMap) {
    var queryList = new ArrayList<Query>();
    // Entry has key of annotation, value of isDataFromSourceSystem (i.e. if we should mark as merge)
    for (var entry : annotationMap.entrySet()) {
      var insertQuery = insertAnnotation(entry.getKey(), entry.getValue());
      var fullQuery = onConflict(entry.getKey(), insertQuery, entry.getValue());
      queryList.add(fullQuery);
    }
    context.batch(queryList).execute();
  }

  public void createAnnotationRecordsHashed(List<HashedAnnotation> hashedAnnotations) {
    var queryList = new ArrayList<Query>();
    for (var hashedAnnotation : hashedAnnotations) {
      var insertQuery = insertAnnotation(hashedAnnotation.annotation(), false)
          .set(ANNOTATION.ANNOTATION_HASH, hashedAnnotation.hash());
      var fullQuery = onConflict(hashedAnnotation.annotation(), insertQuery, false)
          .set(ANNOTATION.ANNOTATION_HASH, hashedAnnotation.hash());
      queryList.add(fullQuery);
    }
    context.batch(queryList).execute();
  }

  private InsertSetMoreStep<AnnotationRecord> insertAnnotation(Annotation annotation,
      boolean isMerged) {
    return context.insertInto(ANNOTATION)
        .set(ANNOTATION.ID, removeProxy(annotation))
        .set(ANNOTATION.VERSION, annotation.getOdsVersion())
        .set(ANNOTATION.TYPE, annotation.getOdsFdoType())
        .set(ANNOTATION.MOTIVATION, annotation.getOaMotivation().value())
        .set(ANNOTATION.MJR_JOB_ID, annotation.getOdsJobID())
        .set(ANNOTATION.BATCH_ID, annotation.getOdsBatchID())
        .set(ANNOTATION.CREATOR, annotation.getDctermsCreator().getId())
        .set(ANNOTATION.CREATED, annotation.getDctermsCreated().toInstant())
        .set(ANNOTATION.MODIFIED, annotation.getDctermsModified().toInstant())
        .set(ANNOTATION.LAST_CHECKED, annotation.getDctermsCreated().toInstant())
        .set(ANNOTATION.TARGET_ID, annotation.getOaHasTarget().getId())
        .set(ANNOTATION.ANNOTATION_STATUS, getAnnotationStatus(annotation, isMerged))
        .set(ANNOTATION.DATA, mapToJSONB(annotation));
  }

  private static AnnotationStatusEnum getAnnotationStatus(Annotation annotation, boolean isMerged) {
    if (isMerged) {
      return AnnotationStatusEnum.MERGED;
    }
    switch (annotation.getOdsMergingDecisionStatus()) {
      case APPROVED -> {
        return AnnotationStatusEnum.ACCEPTED;
      }
      case PENDING -> {
        return AnnotationStatusEnum.PENDING;
      }
      case REJECTED -> {
        return AnnotationStatusEnum.REJECTED;
      }
      case null, default -> {
        return null;
      }
    }
  }

  private @NotNull InsertOnDuplicateSetMoreStep<AnnotationRecord> onConflict(
      Annotation annotation,
      InsertSetMoreStep<AnnotationRecord> query, boolean isMerged) {
    return query.onConflict(ANNOTATION.ID).doUpdate()
        .set(ANNOTATION.VERSION, annotation.getOdsVersion())
        .set(ANNOTATION.TYPE, annotation.getOdsFdoType())
        .set(ANNOTATION.MOTIVATION, annotation.getOaMotivation().value())
        .set(ANNOTATION.MJR_JOB_ID, annotation.getOdsJobID())
        .set(ANNOTATION.BATCH_ID, annotation.getOdsBatchID())
        .set(ANNOTATION.CREATOR, annotation.getDctermsCreator().getId())
        .set(ANNOTATION.CREATED, annotation.getDctermsCreated().toInstant())
        .set(ANNOTATION.MODIFIED, annotation.getDctermsModified().toInstant())
        .set(ANNOTATION.LAST_CHECKED, annotation.getDctermsCreated().toInstant())
        .set(ANNOTATION.TARGET_ID, annotation.getOaHasTarget().getId())
        .set(ANNOTATION.DATA, mapToJSONB(annotation))
        .set(ANNOTATION.ANNOTATION_STATUS, getAnnotationStatus(annotation, isMerged));
  }

  public void updateLastChecked(List<String> idList) {
    var list = idList.stream().map(HandleUtils::removeProxy).toList();
    context.update(ANNOTATION)
        .set(ANNOTATION.LAST_CHECKED, Instant.now())
        .where(ANNOTATION.ID.in(list))
        .execute();
  }

  public void archiveAnnotation(Annotation annotation) {
    var timestamp = annotation.getOdsHasTombstoneMetadata().getOdsTombstoneDate().toInstant();
    context.update(ANNOTATION)
        .set(ANNOTATION.TOMBSTONED, timestamp)
        .set(ANNOTATION.MODIFIED, timestamp)
        .set(ANNOTATION.LAST_CHECKED, timestamp)
        .set(ANNOTATION.DATA, mapToJSONB(annotation))
        .set(ANNOTATION.VERSION, annotation.getOdsVersion())
        .setNull(ANNOTATION.ANNOTATION_STATUS)
        .where(ANNOTATION.ID.eq(removeProxy(annotation)))
        .execute();
  }

  public void rollbackAnnotation(String id) {
    context.delete(ANNOTATION).where(ANNOTATION.ID.eq(removeProxy(id))).execute();
  }

  public void rollbackAnnotations(List<String> idList) {
    var list = idList.stream().map(HandleUtils::removeProxy).toList();
    context.delete(ANNOTATION).where(ANNOTATION.ID.in(list)).execute();
  }

  private JSONB mapToJSONB(Annotation annotation) {
    return JSONB.valueOf(mapper.writeValueAsString(annotation));
  }
}
