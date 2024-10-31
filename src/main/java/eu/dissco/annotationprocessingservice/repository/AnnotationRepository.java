package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.database.jooq.Tables.ANNOTATION;
import static eu.dissco.annotationprocessingservice.utils.HandleUtils.removeProxy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.database.jooq.tables.records.AnnotationRecord;
import eu.dissco.annotationprocessingservice.domain.HashedAnnotation;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import eu.dissco.annotationprocessingservice.schema.Annotation;
import eu.dissco.annotationprocessingservice.utils.HandleUtils;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
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

@Slf4j
@Repository
@RequiredArgsConstructor
public class AnnotationRepository {

  private final ObjectMapper mapper;
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

  private Annotation mapAnnotation(Record dbRecord) {
    try {
      return mapper.readValue(dbRecord.get(ANNOTATION.DATA).data(),
          Annotation.class);
    } catch (JsonProcessingException e) {
      log.error("Failed to get data from database, Unable to parse JSONB to JSON", e);
      throw new DataBaseException(e.getMessage());
    }
  }

  private HashedAnnotation mapHashedAnnotation(Record dbRecord) {
    return new HashedAnnotation(
        mapAnnotation(dbRecord),
        dbRecord.get(ANNOTATION.ANNOTATION_HASH)
    );
  }

  public void createAnnotationRecord(Annotation annotation) {
    var insertQuery = insertAnnotation(annotation);
    var fullQuery = onConflict(annotation, insertQuery);
    fullQuery.execute();
  }

  public void createAnnotationRecord(List<HashedAnnotation> hashedAnnotations) {
    var queryList = new ArrayList<Query>();
    for (var hashedAnnotation : hashedAnnotations) {
      var insertQuery = insertAnnotation(hashedAnnotation.annotation())
          .set(ANNOTATION.ANNOTATION_HASH, hashedAnnotation.hash());
      var fullQuery = onConflict(hashedAnnotation.annotation(), insertQuery)
          .set(ANNOTATION.ANNOTATION_HASH, hashedAnnotation.hash());
      queryList.add(fullQuery);
    }
    context.batch(queryList).execute();
  }

  private InsertSetMoreStep<AnnotationRecord> insertAnnotation(Annotation annotation) {
    try {
      return context.insertInto(ANNOTATION)
          .set(ANNOTATION.ID, removeProxy(annotation))
          .set(ANNOTATION.VERSION, annotation.getOdsVersion())
          .set(ANNOTATION.TYPE, annotation.getRdfType())
          .set(ANNOTATION.MOTIVATION, annotation.getOaMotivation().value())
          .set(ANNOTATION.MJR_JOB_ID, annotation.getOdsJobID())
          .set(ANNOTATION.BATCH_ID, annotation.getOdsBatchID())
          .set(ANNOTATION.CREATOR, annotation.getDctermsCreator().getId())
          .set(ANNOTATION.CREATED, annotation.getDctermsCreated().toInstant())
          .set(ANNOTATION.MODIFIED, annotation.getDctermsModified().toInstant())
          .set(ANNOTATION.LAST_CHECKED, annotation.getDctermsCreated().toInstant())
          .set(ANNOTATION.TARGET_ID, annotation.getOaHasTarget().getId())
          .set(ANNOTATION.DATA, mapToJSONB(annotation));
    } catch (JsonProcessingException e) {
      log.error("Failed to post data to database, unable to parse JSON to JSONB", e);
      throw new DataBaseException(e.getMessage());
    }
  }

  private @NotNull InsertOnDuplicateSetMoreStep<AnnotationRecord> onConflict(
      Annotation annotation,
      InsertSetMoreStep<AnnotationRecord> query) {
    try {
      return query.onConflict(ANNOTATION.ID).doUpdate()
          .set(ANNOTATION.VERSION, annotation.getOdsVersion())
          .set(ANNOTATION.TYPE, annotation.getRdfType())
          .set(ANNOTATION.MOTIVATION, annotation.getOaMotivation().value())
          .set(ANNOTATION.MJR_JOB_ID, annotation.getOdsJobID())
          .set(ANNOTATION.BATCH_ID, annotation.getOdsBatchID())
          .set(ANNOTATION.CREATOR, annotation.getDctermsCreator().getId())
          .set(ANNOTATION.CREATED, annotation.getDctermsCreated().toInstant())
          .set(ANNOTATION.MODIFIED, annotation.getDctermsModified().toInstant())
          .set(ANNOTATION.LAST_CHECKED, annotation.getDctermsCreated().toInstant())
          .set(ANNOTATION.TARGET_ID, annotation.getOaHasTarget().getId())
          .set(ANNOTATION.DATA, mapToJSONB(annotation));
    } catch (JsonProcessingException e) {
      log.error("Failed to post data to database, unable to parse JSON to JSONB", e);
      throw new DataBaseException(e.getMessage());
    }
  }

  public void updateLastChecked(List<String> idList) {
    var list = idList.stream().map(HandleUtils::removeProxy).toList();
    context.update(ANNOTATION)
        .set(ANNOTATION.LAST_CHECKED, Instant.now())
        .where(ANNOTATION.ID.in(list))
        .execute();
  }

  public void archiveAnnotation(Annotation annotation) {
    var timestamp = annotation.getOdsTombstoneMetadata().getOdsTombstoneDate().toInstant();
    try {
      context.update(ANNOTATION)
          .set(ANNOTATION.TOMBSTONED, timestamp)
          .set(ANNOTATION.MODIFIED, timestamp)
          .set(ANNOTATION.LAST_CHECKED, timestamp)
          .set(ANNOTATION.DATA, mapToJSONB(annotation))
          .set(ANNOTATION.VERSION, annotation.getOdsVersion())
          .where(ANNOTATION.ID.eq(removeProxy(annotation)))
          .execute();
    } catch (JsonProcessingException e) {
      log.error("Unable to map data mapping to jsonb. Need to archive annotation {} manually",
          annotation.getId(), e);
      throw new DataBaseException(e.getMessage());
    }
  }

  public void rollbackAnnotation(String id) {
    context.delete(ANNOTATION).where(ANNOTATION.ID.eq(removeProxy(id))).execute();
  }

  public void rollbackAnnotations(List<String> idList) {
    var list = idList.stream().map(HandleUtils::removeProxy).toList();
    context.delete(ANNOTATION).where(ANNOTATION.ID.in(list)).execute();
  }

  private JSONB mapToJSONB(Annotation annotation) throws JsonProcessingException {
    return JSONB.valueOf(mapper.writeValueAsString(annotation));
  }
}
