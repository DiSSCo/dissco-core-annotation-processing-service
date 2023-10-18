package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.database.jooq.Tables.ANNOTATION;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.domain.annotation.AggregateRating;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.domain.annotation.Body;
import eu.dissco.annotationprocessingservice.domain.annotation.Creator;
import eu.dissco.annotationprocessingservice.domain.annotation.Generator;
import eu.dissco.annotationprocessingservice.domain.annotation.Motivation;
import eu.dissco.annotationprocessingservice.domain.annotation.Target;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.JSONB;
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
    return context.select(ANNOTATION.asterisk())
        .from(ANNOTATION)
        .where(ANNOTATION.TARGET_ID.eq(annotation.getOaTarget().getOdsId())
            .and(ANNOTATION.CREATOR_ID.eq(annotation.getOaCreator().getOdsId())
                .and(ANNOTATION.MOTIVATED_BY.eq(annotation.getOaMotivatedBy()))
                .and(ANNOTATION.DELETED_ON.isNull())))
        .fetch().map(this::mapAnnotation);
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

  public void createAnnotationRecord(Annotation annotation) {
    try {
      context.insertInto(ANNOTATION).set(ANNOTATION.ID, annotation.getOdsId())
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
          .set(ANNOTATION.LAST_CHECKED, annotation.getDcTermsCreated())
          .onConflict(ANNOTATION.ID).doUpdate()
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
          .set(ANNOTATION.LAST_CHECKED, annotation.getDcTermsCreated())
          .execute();
    } catch (JsonProcessingException e) {
      log.error("Failed to post data to database, unable to parse JSON to JSONB", e);
      throw new DataBaseException(e.getMessage());
    }
  }

  public int updateLastChecked(Annotation currentAnnotation) {
    return context.update(ANNOTATION)
        .set(ANNOTATION.LAST_CHECKED, Instant.now())
        .where(ANNOTATION.ID.eq(currentAnnotation.getOdsId()))
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
}
