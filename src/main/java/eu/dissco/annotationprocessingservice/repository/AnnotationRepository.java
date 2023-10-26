package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.database.jooq.Tables.ANNOTATION;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.database.jooq.tables.records.AnnotationRecord;
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

  public List<Annotation> getAnnotation(Annotation annotation) {
    try {
      return context.select(ANNOTATION.asterisk())
          .from(ANNOTATION)
          .where(ANNOTATION.TARGET.eq(
              JSONB.jsonb(mapper.writeValueAsString(annotation.getOaTarget()))))
          .and(ANNOTATION.CREATOR_ID.eq(annotation.getOaCreator().getOdsId()))
          .and(ANNOTATION.MOTIVATION.eq(annotation.getOaMotivation().toString()))
          .and(ANNOTATION.DELETED_ON.isNull())
          .fetch().map(this::mapAnnotation);
    } catch (JsonProcessingException e) {
      log.error("Unable to parse target {} to JSONB", annotation.getOaTarget());
      throw new DataBaseException("Unable to parse target to JSONB");
    }
  }

  public Annotation getAnnotation(String annotationId) {
    var dbRecord = context.select(ANNOTATION.asterisk())
        .from(ANNOTATION)
        .where(ANNOTATION.ID.eq(annotationId))
        .fetchOne();
    if (dbRecord == null) {
      return null;
    }
    return dbRecord.map(this::mapAnnotation);
  }

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
        .where(ANNOTATION.ID.eq(annotationId))
        .and(ANNOTATION.CREATOR_ID.eq(creatorId))
        .fetchOptional()
        .map(this::mapAnnotation);
  }

  private Annotation mapAnnotation(Record dbRecord) {
    try {
      return new Annotation()
          .withOdsId(dbRecord.get(ANNOTATION.ID))
          .withRdfType(dbRecord.get(ANNOTATION.TYPE))
          .withOdsVersion(dbRecord.get(ANNOTATION.VERSION))
          .withOaMotivation(Motivation.fromString(dbRecord.get(ANNOTATION.MOTIVATION)))
          .withOaMotivatedBy(dbRecord.get(ANNOTATION.MOTIVATED_BY))
          .withOaTarget(mapper.readValue(dbRecord.get(ANNOTATION.TARGET).data(), Target.class))
          .withOaBody(mapper.readValue(dbRecord.get(ANNOTATION.BODY).data(), Body.class))
          .withOaCreator(mapper.readValue(dbRecord.get(ANNOTATION.CREATOR).data(), Creator.class))
          .withDcTermsCreated(dbRecord.get(ANNOTATION.CREATED))
          .withOdsDeletedOn(dbRecord.get(ANNOTATION.DELETED_ON))
          .withAsGenerator(
              mapper.readValue(dbRecord.get(ANNOTATION.GENERATOR).data(), Generator.class))
          .withOaGenerated(dbRecord.get(ANNOTATION.GENERATED))
          .withOdsAggregateRating(mapper.readValue(dbRecord.get(ANNOTATION.AGGREGATE_RATING).data(),
              AggregateRating.class));
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

  private @NotNull InsertSetMoreStep<AnnotationRecord> insertAnnotation(Annotation annotation) {
    try {
      return context.insertInto(ANNOTATION).set(ANNOTATION.ID, annotation.getOdsId())
          .set(ANNOTATION.VERSION, annotation.getOdsVersion())
          .set(ANNOTATION.TYPE, annotation.getRdfType())
          .set(ANNOTATION.MOTIVATION, annotation.getOaMotivation().toString())
          .set(ANNOTATION.MOTIVATED_BY, annotation.getOaMotivatedBy())
          .set(ANNOTATION.TARGET_ID, annotation.getOaTarget().getOdsId())
          .set(ANNOTATION.TARGET, JSONB.jsonb(mapper.writeValueAsString(annotation.getOaTarget())))
          .set(ANNOTATION.BODY, JSONB.jsonb(mapper.writeValueAsString(annotation.getOaBody())))
          .set(ANNOTATION.AGGREGATE_RATING,
              JSONB.jsonb(mapper.writeValueAsString(annotation.getOdsAggregateRating())))
          .set(ANNOTATION.CREATOR,
              JSONB.jsonb(mapper.writeValueAsString(annotation.getOaCreator())))
          .set(ANNOTATION.CREATOR_ID, annotation.getOaCreator().getOdsId())
          .set(ANNOTATION.CREATED, annotation.getDcTermsCreated())
          .set(ANNOTATION.GENERATOR,
              JSONB.jsonb(mapper.writeValueAsString(annotation.getAsGenerator())))
          .set(ANNOTATION.GENERATED, annotation.getOaGenerated())
          .set(ANNOTATION.LAST_CHECKED, annotation.getDcTermsCreated());
    } catch (JsonProcessingException e) {
      log.error("Failed to post data to database, unable to parse JSON to JSONB", e);
      throw new DataBaseException(e.getMessage());
    }
  }

  private @NotNull InsertOnDuplicateSetMoreStep<AnnotationRecord> onConflict(Annotation annotation,
      InsertSetMoreStep<AnnotationRecord> query) {
    try {
      return query.onConflict(ANNOTATION.ID).doUpdate()
          .set(ANNOTATION.VERSION, annotation.getOdsVersion())
          .set(ANNOTATION.TYPE, annotation.getRdfType())
          .set(ANNOTATION.MOTIVATION, annotation.getOaMotivation().toString())
          .set(ANNOTATION.MOTIVATED_BY, annotation.getOaMotivatedBy())
          .set(ANNOTATION.TARGET_ID, annotation.getOaTarget().getOdsId())
          .set(ANNOTATION.TARGET, JSONB.jsonb(mapper.writeValueAsString(annotation.getOaTarget())))
          .set(ANNOTATION.BODY, JSONB.jsonb(mapper.writeValueAsString(annotation.getOaBody())))
          .set(ANNOTATION.AGGREGATE_RATING,
              JSONB.jsonb(mapper.writeValueAsString(annotation.getOdsAggregateRating())))
          .set(ANNOTATION.CREATOR,
              JSONB.jsonb(mapper.writeValueAsString(annotation.getOaCreator())))
          .set(ANNOTATION.CREATOR_ID, annotation.getOaCreator().getOdsId())
          .set(ANNOTATION.CREATED, annotation.getDcTermsCreated())
          .set(ANNOTATION.GENERATOR,
              JSONB.jsonb(mapper.writeValueAsString(annotation.getAsGenerator())))
          .set(ANNOTATION.GENERATED, annotation.getOaGenerated())
          .set(ANNOTATION.LAST_CHECKED, annotation.getDcTermsCreated());
    } catch (JsonProcessingException e) {
      log.error("Failed to post data to database, unable to parse JSON to JSONB", e);
      throw new DataBaseException(e.getMessage());
    }
  }

  public int updateLastChecked(List<String> idList) {
    return context.update(ANNOTATION)
        .set(ANNOTATION.LAST_CHECKED, Instant.now())
        .where(ANNOTATION.ID.in(idList))
        .execute();
  }

  public Optional<String> getAnnotationById(String id) {
    return context.select(ANNOTATION.ID)
        .from(ANNOTATION)
        .where(ANNOTATION.ID.eq(id))
        .and(ANNOTATION.DELETED_ON.isNull())
        .fetchOptional(Record1::value1);
  }

  public void archiveAnnotation(String id) {
    context.update(ANNOTATION)
        .set(ANNOTATION.DELETED_ON, Instant.now())
        .where(ANNOTATION.ID.eq(id))
        .execute();
  }

  public void rollbackAnnotation(String id) {
    context.delete(ANNOTATION).where(ANNOTATION.ID.eq(id)).execute();
  }

  public void rollbackAnnotations(List<String> idList) {
    context.delete(ANNOTATION).where(ANNOTATION.ID.in(idList)).execute();
  }
}
