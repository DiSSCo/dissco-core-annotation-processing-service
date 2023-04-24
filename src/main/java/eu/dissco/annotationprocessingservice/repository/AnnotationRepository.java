package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.database.jooq.Tables.NEW_ANNOTATION;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.domain.Annotation;
import eu.dissco.annotationprocessingservice.domain.AnnotationRecord;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import java.time.Instant;
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

  private static final String FIELD_SET = "fieldSet";
  private static final String INDV_PROP = "indvProp";

  private final ObjectMapper mapper;
  private final DSLContext context;

  public Optional<AnnotationRecord> getAnnotation(JsonNode targetId, String creator,
      String motivation) throws DataBaseException {
    var query = context.select(NEW_ANNOTATION.asterisk())
        .from(NEW_ANNOTATION)
        .where(NEW_ANNOTATION.TARGET_ID.eq(targetId.get("id").asText()))
        .and(NEW_ANNOTATION.CREATOR.eq(creator))
        .and(NEW_ANNOTATION.MOTIVATION.eq(motivation))
        .and(NEW_ANNOTATION.DELETED.isNull());
    if (targetId.get(INDV_PROP) != null) {
      query.and(NEW_ANNOTATION.TARGET_FIELD.eq(targetId.get(INDV_PROP).asText()));
    } else if (targetId.get(FIELD_SET) != null) {
      query.and(NEW_ANNOTATION.TARGET_FIELD.eq(targetId.get(FIELD_SET).asText()));
    } else {
      query.and(NEW_ANNOTATION.TARGET_FIELD.isNull());
    }
    var dbRecord = query.fetchOptional();
    if (dbRecord.isPresent()) {
      return Optional.of(mapAnnotationRecord(dbRecord.get()));
    } else {
      return Optional.empty();
    }
  }

  private AnnotationRecord mapAnnotationRecord(Record dbRecord) throws DataBaseException {
    Annotation annotation;
    try {
      annotation = new Annotation(
          dbRecord.get(NEW_ANNOTATION.TYPE),
          dbRecord.get(NEW_ANNOTATION.MOTIVATION),
          mapper.readTree(dbRecord.get(NEW_ANNOTATION.TARGET_BODY).data()),
          mapper.readTree(dbRecord.get(NEW_ANNOTATION.BODY).data()),
          dbRecord.get(NEW_ANNOTATION.PREFERENCE_SCORE),
          dbRecord.get(NEW_ANNOTATION.CREATOR),
          dbRecord.get(NEW_ANNOTATION.CREATED),
          mapper.readTree(dbRecord.get(NEW_ANNOTATION.GENERATOR_BODY).data()),
          dbRecord.get(NEW_ANNOTATION.GENERATED)
      );
    } catch (JsonProcessingException e) {
      log.error("Failed to get data from database, Unable to parse JSONB to JSON", e);
      throw new DataBaseException(e.getMessage());
    }
    return new AnnotationRecord(
        dbRecord.get(NEW_ANNOTATION.ID),
        dbRecord.get(NEW_ANNOTATION.VERSION),
        dbRecord.get(NEW_ANNOTATION.CREATED),
        annotation);
  }

  public int createAnnotationRecord(AnnotationRecord annotationRecord) {
    String targetField = null;
    if (annotationRecord.annotation().target().get(FIELD_SET) != null) {
      targetField = annotationRecord.annotation().target().get(FIELD_SET).asText();
    }
    if (annotationRecord.annotation().target().get(INDV_PROP) != null) {
      targetField = annotationRecord.annotation().target().get(INDV_PROP).asText();
    }
    return context.insertInto(NEW_ANNOTATION)
        .set(NEW_ANNOTATION.ID, annotationRecord.id())
        .set(NEW_ANNOTATION.VERSION, annotationRecord.version())
        .set(NEW_ANNOTATION.TYPE, annotationRecord.annotation().type())
        .set(NEW_ANNOTATION.MOTIVATION, annotationRecord.annotation().motivation())
        .set(NEW_ANNOTATION.TARGET_ID, annotationRecord.annotation().target().get("id").asText())
        .set(NEW_ANNOTATION.TARGET_FIELD, targetField)
        .set(NEW_ANNOTATION.TARGET_BODY,
            JSONB.jsonb(annotationRecord.annotation().target().toString()))
        .set(NEW_ANNOTATION.BODY, JSONB.jsonb(annotationRecord.annotation().body().toString()))
        .set(NEW_ANNOTATION.PREFERENCE_SCORE, annotationRecord.annotation().preferenceScore())
        .set(NEW_ANNOTATION.CREATOR, annotationRecord.annotation().creator())
        .set(NEW_ANNOTATION.CREATED, annotationRecord.annotation().created())
        .set(NEW_ANNOTATION.GENERATOR_ID,
            annotationRecord.annotation().generator().get("id").asText())
        .set(NEW_ANNOTATION.GENERATOR_BODY,
            JSONB.jsonb(annotationRecord.annotation().generator().toString()))
        .set(NEW_ANNOTATION.GENERATED, annotationRecord.annotation().generated())
        .set(NEW_ANNOTATION.LAST_CHECKED, Instant.now())
        .onConflict(NEW_ANNOTATION.ID).doUpdate()
        .set(NEW_ANNOTATION.VERSION, annotationRecord.version())
        .set(NEW_ANNOTATION.TYPE, annotationRecord.annotation().type())
        .set(NEW_ANNOTATION.MOTIVATION, annotationRecord.annotation().motivation())
        .set(NEW_ANNOTATION.TARGET_ID, annotationRecord.annotation().target().get("id").asText())
        .set(NEW_ANNOTATION.TARGET_FIELD, targetField)
        .set(NEW_ANNOTATION.TARGET_BODY,
            JSONB.jsonb(annotationRecord.annotation().target().toString()))
        .set(NEW_ANNOTATION.BODY, JSONB.jsonb(annotationRecord.annotation().body().toString()))
        .set(NEW_ANNOTATION.PREFERENCE_SCORE, annotationRecord.annotation().preferenceScore())
        .set(NEW_ANNOTATION.CREATOR, annotationRecord.annotation().creator())
        .set(NEW_ANNOTATION.CREATED, annotationRecord.annotation().created())
        .set(NEW_ANNOTATION.GENERATOR_ID,
            annotationRecord.annotation().generator().get("id").asText())
        .set(NEW_ANNOTATION.GENERATOR_BODY,
            JSONB.jsonb(annotationRecord.annotation().generator().toString()))
        .set(NEW_ANNOTATION.GENERATED, annotationRecord.annotation().generated())
        .set(NEW_ANNOTATION.LAST_CHECKED, Instant.now())
        .execute();
  }

  public int updateLastChecked(AnnotationRecord currentAnnotationRecord) {
    return context.update(NEW_ANNOTATION)
        .set(NEW_ANNOTATION.LAST_CHECKED, Instant.now())
        .where(NEW_ANNOTATION.ID.eq(currentAnnotationRecord.id()))
        .execute();
  }

  public Optional<String> getAnnotationById(String id) {
    return context.select(NEW_ANNOTATION.ID)
        .from(NEW_ANNOTATION)
        .where(NEW_ANNOTATION.ID.eq(id))
        .and(NEW_ANNOTATION.DELETED.isNull())
        .fetchOptional(Record1::value1);
  }

  public void archiveAnnotation(String id) {
    context.update(NEW_ANNOTATION)
        .set(NEW_ANNOTATION.DELETED, Instant.now())
        .where(NEW_ANNOTATION.ID.eq(id))
        .execute();
  }

  public void rollbackAnnotation(String id) {
    context.delete(NEW_ANNOTATION).where(NEW_ANNOTATION.ID.eq(id)).execute();
  }
}
