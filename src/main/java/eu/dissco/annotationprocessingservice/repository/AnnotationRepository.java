package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.database.jooq.Tables.ANNOTATION_TMP;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.database.jooq.tables.records.AnnotationTmpRecord;
import eu.dissco.annotationprocessingservice.domain.HashedAnnotation;
import eu.dissco.annotationprocessingservice.domain.annotation.AggregateRating;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.domain.annotation.Body;
import eu.dissco.annotationprocessingservice.domain.annotation.Creator;
import eu.dissco.annotationprocessingservice.domain.annotation.Generator;
import eu.dissco.annotationprocessingservice.domain.annotation.Motivation;
import eu.dissco.annotationprocessingservice.domain.annotation.Target;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
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
    var dbRecord = context.select(ANNOTATION_TMP.asterisk())
        .from(ANNOTATION_TMP)
        .where(ANNOTATION_TMP.ANNOTATION_HASH.in(annotationHashes))
        .fetch();
    return dbRecord.map(this::mapHashedAnnotation);
  }

  public Optional<Annotation> getAnnotationForUser(String annotationId, String creatorId) {
    return context.select(ANNOTATION_TMP.asterisk())
        .from(ANNOTATION_TMP)
        .where(ANNOTATION_TMP.ID.eq(annotationId))
        .and(ANNOTATION_TMP.CREATOR_ID.eq(creatorId))
        .fetchOptional()
        .map(this::mapAnnotation);
  }

  private Annotation mapAnnotation(Record dbRecord) {
    try {
      return new Annotation()
          .withOdsId(dbRecord.get(ANNOTATION_TMP.ID))
          .withRdfType(dbRecord.get(ANNOTATION_TMP.TYPE))
          .withOdsVersion(dbRecord.get(ANNOTATION_TMP.VERSION))
          .withOaMotivation(Motivation.fromString(dbRecord.get(ANNOTATION_TMP.MOTIVATION)))
          .withOaMotivatedBy(dbRecord.get(ANNOTATION_TMP.MOTIVATED_BY))
          .withOaTarget(mapper.readValue(dbRecord.get(ANNOTATION_TMP.TARGET).data(), Target.class))
          .withOaBody(mapper.readValue(dbRecord.get(ANNOTATION_TMP.BODY).data(), Body.class))
          .withOaCreator(mapper.readValue(dbRecord.get(ANNOTATION_TMP.CREATOR).data(), Creator.class))
          .withDcTermsCreated(dbRecord.get(ANNOTATION_TMP.CREATED))
          .withOdsDeletedOn(dbRecord.get(ANNOTATION_TMP.DELETED_ON))
          .withAsGenerator(
              mapper.readValue(dbRecord.get(ANNOTATION_TMP.GENERATOR).data(), Generator.class))
          .withOaGenerated(dbRecord.get(ANNOTATION_TMP.GENERATED))
          .withOdsAggregateRating(mapper.readValue(dbRecord.get(ANNOTATION_TMP.AGGREGATE_RATING).data(),
              AggregateRating.class))
          .withOdsJobId(dbRecord.get(ANNOTATION_TMP.MJR_JOB_ID));
    } catch (JsonProcessingException e) {
      log.error("Failed to get data from database, Unable to parse JSONB to JSON", e);
      throw new DataBaseException(e.getMessage());
    }
  }

  private HashedAnnotation mapHashedAnnotation(Record dbRecord) {
    return new HashedAnnotation(
        mapAnnotation(dbRecord),
        dbRecord.get(ANNOTATION_TMP.ANNOTATION_HASH)
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
          .set(ANNOTATION_TMP.ANNOTATION_HASH, hashedAnnotation.hash());
      var fullQuery = onConflict(hashedAnnotation.annotation(), insertQuery)
          .set(ANNOTATION_TMP.ANNOTATION_HASH, hashedAnnotation.hash());
      queryList.add(fullQuery);
    }
    context.batch(queryList).execute();
  }

  private InsertSetMoreStep<AnnotationTmpRecord> insertAnnotation(Annotation annotation) {
    try {
      return context.insertInto(ANNOTATION_TMP).set(ANNOTATION_TMP.ID, annotation.getOdsId())
          .set(ANNOTATION_TMP.VERSION, annotation.getOdsVersion())
          .set(ANNOTATION_TMP.TYPE, annotation.getRdfType())
          .set(ANNOTATION_TMP.MOTIVATION, annotation.getOaMotivation().toString())
          .set(ANNOTATION_TMP.MOTIVATED_BY, annotation.getOaMotivatedBy())
          .set(ANNOTATION_TMP.TARGET_ID, annotation.getOaTarget().getOdsId())
          .set(ANNOTATION_TMP.TARGET, JSONB.jsonb(mapper.writeValueAsString(annotation.getOaTarget())))
          .set(ANNOTATION_TMP.BODY, JSONB.jsonb(mapper.writeValueAsString(annotation.getOaBody())))
          .set(ANNOTATION_TMP.AGGREGATE_RATING,
              JSONB.jsonb(mapper.writeValueAsString(annotation.getOdsAggregateRating())))
          .set(ANNOTATION_TMP.CREATOR,
              JSONB.jsonb(mapper.writeValueAsString(annotation.getOaCreator())))
          .set(ANNOTATION_TMP.CREATOR_ID, annotation.getOaCreator().getOdsId())
          .set(ANNOTATION_TMP.CREATED, annotation.getDcTermsCreated())
          .set(ANNOTATION_TMP.GENERATOR,
              JSONB.jsonb(mapper.writeValueAsString(annotation.getAsGenerator())))
          .set(ANNOTATION_TMP.GENERATED, annotation.getOaGenerated())
          .set(ANNOTATION_TMP.LAST_CHECKED, annotation.getDcTermsCreated())
          .set(ANNOTATION_TMP.MJR_JOB_ID, annotation.getOdsJobId());
    } catch (JsonProcessingException e) {
      log.error("Failed to post data to database, unable to parse JSON to JSONB", e);
      throw new DataBaseException(e.getMessage());
    }
  }

  private @NotNull InsertOnDuplicateSetMoreStep<AnnotationTmpRecord> onConflict(Annotation annotation,
      InsertSetMoreStep<AnnotationTmpRecord> query) {
    try {
      return query.onConflict(ANNOTATION_TMP.ID).doUpdate()
          .set(ANNOTATION_TMP.VERSION, annotation.getOdsVersion())
          .set(ANNOTATION_TMP.TYPE, annotation.getRdfType())
          .set(ANNOTATION_TMP.MOTIVATION, annotation.getOaMotivation().toString())
          .set(ANNOTATION_TMP.MOTIVATED_BY, annotation.getOaMotivatedBy())
          .set(ANNOTATION_TMP.TARGET_ID, annotation.getOaTarget().getOdsId())
          .set(ANNOTATION_TMP.TARGET, JSONB.jsonb(mapper.writeValueAsString(annotation.getOaTarget())))
          .set(ANNOTATION_TMP.BODY, JSONB.jsonb(mapper.writeValueAsString(annotation.getOaBody())))
          .set(ANNOTATION_TMP.AGGREGATE_RATING,
              JSONB.jsonb(mapper.writeValueAsString(annotation.getOdsAggregateRating())))
          .set(ANNOTATION_TMP.CREATOR,
              JSONB.jsonb(mapper.writeValueAsString(annotation.getOaCreator())))
          .set(ANNOTATION_TMP.CREATOR_ID, annotation.getOaCreator().getOdsId())
          .set(ANNOTATION_TMP.CREATED, annotation.getDcTermsCreated())
          .set(ANNOTATION_TMP.GENERATOR,
              JSONB.jsonb(mapper.writeValueAsString(annotation.getAsGenerator())))
          .set(ANNOTATION_TMP.GENERATED, annotation.getOaGenerated())
          .set(ANNOTATION_TMP.LAST_CHECKED, annotation.getDcTermsCreated())
          .set(ANNOTATION_TMP.MJR_JOB_ID, annotation.getOdsJobId());
    } catch (JsonProcessingException e) {
      log.error("Failed to post data to database, unable to parse JSON to JSONB", e);
      throw new DataBaseException(e.getMessage());
    }
  }

  public void updateLastChecked(List<String> idList) {
    context.update(ANNOTATION_TMP)
        .set(ANNOTATION_TMP.LAST_CHECKED, Instant.now())
        .where(ANNOTATION_TMP.ID.in(idList))
        .execute();
  }

  public Optional<String> getAnnotationById(String id) {
    return context.select(ANNOTATION_TMP.ID)
        .from(ANNOTATION_TMP)
        .where(ANNOTATION_TMP.ID.eq(id))
        .and(ANNOTATION_TMP.DELETED_ON.isNull())
        .fetchOptional(Record1::value1);
  }

  public void archiveAnnotation(String id) {
    context.update(ANNOTATION_TMP)
        .set(ANNOTATION_TMP.DELETED_ON, Instant.now())
        .where(ANNOTATION_TMP.ID.eq(id))
        .execute();
  }

  public void rollbackAnnotation(String id) {
    context.delete(ANNOTATION_TMP).where(ANNOTATION_TMP.ID.eq(id)).execute();
  }

  public void rollbackAnnotations(List<String> idList) {
    context.delete(ANNOTATION_TMP).where(ANNOTATION_TMP.ID.in(idList)).execute();
  }
}
