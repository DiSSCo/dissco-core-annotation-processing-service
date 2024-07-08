package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.database.jooq.Tables.NEW_ANNOTATION;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.database.jooq.tables.records.NewAnnotationRecord;
import eu.dissco.annotationprocessingservice.domain.HashedAnnotation;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import eu.dissco.annotationprocessingservice.schema.Annotation;
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
import org.jooq.Record1;
import org.springframework.stereotype.Repository;

@Slf4j
@Repository
@RequiredArgsConstructor
public class AnnotationRepository {

  private final ObjectMapper mapper;
  private final DSLContext context;

  public List<HashedAnnotation> getAnnotationFromHash(Set<UUID> annotationHashes) {
    var dbRecord = context.select(NEW_ANNOTATION.asterisk())
        .from(NEW_ANNOTATION)
        .where(NEW_ANNOTATION.ANNOTATION_HASH.in(annotationHashes))
        .fetch();
    return dbRecord.map(this::mapHashedAnnotation);
  }

  public Optional<Annotation> getAnnotationForUser(String annotationId, String creatorId) {
    return context.select(NEW_ANNOTATION.asterisk())
        .from(NEW_ANNOTATION)
        .where(NEW_ANNOTATION.ID.eq(annotationId))
        .and(NEW_ANNOTATION.CREATOR_ID.eq(creatorId))
        .fetchOptional()
        .map(this::mapAnnotation);
  }

  private Annotation mapAnnotation(Record dbRecord) {
    try {
      return mapper.readValue(dbRecord.get(NEW_ANNOTATION.DATA).data(),
          Annotation.class);
    } catch (JsonProcessingException e) {
      log.error("Failed to get data from database, Unable to parse JSONB to JSON", e);
      throw new DataBaseException(e.getMessage());
    }
  }

  private HashedAnnotation mapHashedAnnotation(Record dbRecord) {
    return new HashedAnnotation(
        mapAnnotation(dbRecord),
        dbRecord.get(NEW_ANNOTATION.ANNOTATION_HASH)
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
          .set(NEW_ANNOTATION.ANNOTATION_HASH, hashedAnnotation.hash());
      var fullQuery = onConflict(hashedAnnotation.annotation(), insertQuery)
          .set(NEW_ANNOTATION.ANNOTATION_HASH, hashedAnnotation.hash());
      queryList.add(fullQuery);
    }
    context.batch(queryList).execute();
  }

  private InsertSetMoreStep<NewAnnotationRecord> insertAnnotation(Annotation annotation) {
    try {
      return context.insertInto(NEW_ANNOTATION)
          .set(NEW_ANNOTATION.ID, annotation.getId())
          .set(NEW_ANNOTATION.VERSION, annotation.getOdsVersion())
          .set(NEW_ANNOTATION.TYPE, annotation.getRdfType())
          .set(NEW_ANNOTATION.MOTIVATION, annotation.getOaMotivation().value())
          .set(NEW_ANNOTATION.MJR_JOB_ID, annotation.getOdsJobID())
          .set(NEW_ANNOTATION.BATCH_ID, annotation.getOdsBatchID())
          .set(NEW_ANNOTATION.CREATOR_ID, annotation.getDctermsCreator().getId())
          .set(NEW_ANNOTATION.CREATED, annotation.getDctermsCreated().toInstant())
          .set(NEW_ANNOTATION.MODIFIED, annotation.getDctermsModified().toInstant())
          .set(NEW_ANNOTATION.LAST_CHECKED, annotation.getDctermsCreated().toInstant())
          .set(NEW_ANNOTATION.TARGET_ID, annotation.getOaHasTarget().getId())
          .set(NEW_ANNOTATION.DATA, JSONB.jsonb(mapper.writeValueAsString(annotation)));
    } catch (JsonProcessingException e) {
      log.error("Failed to post data to database, unable to parse JSON to JSONB", e);
      throw new DataBaseException(e.getMessage());
    }
  }

  private @NotNull InsertOnDuplicateSetMoreStep<NewAnnotationRecord> onConflict(
      Annotation annotation,
      InsertSetMoreStep<NewAnnotationRecord> query) {
    try {
      return query.onConflict(NEW_ANNOTATION.ID).doUpdate()
          .set(NEW_ANNOTATION.VERSION, annotation.getOdsVersion())
          .set(NEW_ANNOTATION.TYPE, annotation.getRdfType())
          .set(NEW_ANNOTATION.MOTIVATION, annotation.getOaMotivation().value())
          .set(NEW_ANNOTATION.MJR_JOB_ID, annotation.getOdsJobID())
          .set(NEW_ANNOTATION.BATCH_ID, annotation.getOdsBatchID())
          .set(NEW_ANNOTATION.CREATOR_ID, annotation.getDctermsCreator().getId())
          .set(NEW_ANNOTATION.CREATED, annotation.getDctermsCreated().toInstant())
          .set(NEW_ANNOTATION.MODIFIED, annotation.getDctermsModified().toInstant())
          .set(NEW_ANNOTATION.LAST_CHECKED, annotation.getDctermsCreated().toInstant())
          .set(NEW_ANNOTATION.TARGET_ID, annotation.getOaHasTarget().getId())
          .set(NEW_ANNOTATION.DATA, JSONB.jsonb(mapper.writeValueAsString(annotation)));
    } catch (JsonProcessingException e) {
      log.error("Failed to post data to database, unable to parse JSON to JSONB", e);
      throw new DataBaseException(e.getMessage());
    }
  }

  public void updateLastChecked(List<String> idList) {
    context.update(NEW_ANNOTATION)
        .set(NEW_ANNOTATION.LAST_CHECKED, Instant.now())
        .where(NEW_ANNOTATION.ID.in(idList))
        .execute();
  }

  public Optional<String> getAnnotationById(String id) {
    return context.select(NEW_ANNOTATION.ID)
        .from(NEW_ANNOTATION)
        .where(NEW_ANNOTATION.ID.eq(id))
        .and(NEW_ANNOTATION.TOMBSTONED_ON.isNull())
        .fetchOptional(Record1::value1);
  }

  public void archiveAnnotation(String id) {
    context.update(NEW_ANNOTATION)
        .set(NEW_ANNOTATION.TOMBSTONED_ON, Instant.now())
        .where(NEW_ANNOTATION.ID.eq(id))
        .execute();
  }

  public void rollbackAnnotation(String id) {
    context.delete(NEW_ANNOTATION).where(NEW_ANNOTATION.ID.eq(id)).execute();
  }

  public void rollbackAnnotations(List<String> idList) {
    context.delete(NEW_ANNOTATION).where(NEW_ANNOTATION.ID.in(idList)).execute();
  }
}
