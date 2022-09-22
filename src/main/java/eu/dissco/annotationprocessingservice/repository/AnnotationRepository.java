package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.database.jooq.Tables.NEW_ANNOTATION;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.domain.Annotation;
import eu.dissco.annotationprocessingservice.domain.AnnotationRecord;
import java.time.Instant;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.Record1;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class AnnotationRepository {

  private final ObjectMapper mapper;
  private final DSLContext context;

  public Optional<AnnotationRecord> getAnnotation(JsonNode targetId, JsonNode generator,
      String motivation) {
    var query = context.select(NEW_ANNOTATION.asterisk())
        .distinctOn(NEW_ANNOTATION.ID)
        .from(NEW_ANNOTATION)
        .where(NEW_ANNOTATION.TARGET_ID.eq(targetId.get("id").asText()))
        .and(NEW_ANNOTATION.GENERATOR_ID.eq(generator.get("id").asText()))
        .and(NEW_ANNOTATION.MOTIVATION.eq(motivation))
        .and(NEW_ANNOTATION.DELETED.isNull());
    if (targetId.get("fieldSet") != null) {
      query.and(NEW_ANNOTATION.TARGET_FIELD.eq(targetId.get("fieldSet").asText()));
    }
    if (targetId.get("indvProp") != null) {
      query.and(NEW_ANNOTATION.TARGET_FIELD.eq(targetId.get("indvProp").asText()));
    }
    return query.orderBy(NEW_ANNOTATION.ID, NEW_ANNOTATION.VERSION.desc())
        .fetchOptional(this::mapAnnotationRecord);
  }

  private AnnotationRecord mapAnnotationRecord(Record record) {
    Annotation annotation = null;
    try {
      annotation = new Annotation(
          record.get(NEW_ANNOTATION.TYPE),
          record.get(NEW_ANNOTATION.MOTIVATION),
          mapper.readTree(record.get(NEW_ANNOTATION.TARGET_BODY).data()),
          mapper.readTree(record.get(NEW_ANNOTATION.BODY).data()),
          record.get(NEW_ANNOTATION.PREFERENCE_SCORE),
          record.get(NEW_ANNOTATION.CREATOR),
          record.get(NEW_ANNOTATION.CREATED),
          mapper.readTree(record.get(NEW_ANNOTATION.GENERATOR_BODY).data()),
          record.get(NEW_ANNOTATION.GENERATED)
      );
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    return new AnnotationRecord(
        record.get(NEW_ANNOTATION.ID),
        record.get(NEW_ANNOTATION.VERSION),
        record.get(NEW_ANNOTATION.CREATED),
        annotation);
  }

  public int createAnnotationRecord(AnnotationRecord annotationRecord) {
    String targetField = null;
    if (annotationRecord.annotation().target().get("fieldSet") != null) {
      targetField = annotationRecord.annotation().target().get("fieldSet").asText();
    }
    if (annotationRecord.annotation().target().get("indvProp") != null) {
      targetField = annotationRecord.annotation().target().get("indvProp").asText();
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
        .distinctOn(NEW_ANNOTATION.ID)
        .from(NEW_ANNOTATION)
        .where(NEW_ANNOTATION.ID.eq(id))
        .and(NEW_ANNOTATION.DELETED.isNull())
        .orderBy(NEW_ANNOTATION.ID, NEW_ANNOTATION.VERSION.desc())
        .fetchOptional(Record1::value1);
  }

  public int archiveAnnotation(String id) {
    return context.update(NEW_ANNOTATION)
        .set(NEW_ANNOTATION.DELETED, Instant.now())
        .where(NEW_ANNOTATION.ID.eq(id))
        .execute();
  }
}
