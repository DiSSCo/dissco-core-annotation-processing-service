package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.database.jooq.Tables.DIGITAL_SPECIMEN;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import io.github.dissco.core.annotationlogic.schema.DigitalSpecimen;
import java.util.List;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.exception.DataAccessException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

@Slf4j
@Repository
@RequiredArgsConstructor
public class DigitalSpecimenRepository {

  private final DSLContext context;
  @Qualifier("objectMapper")
  private final ObjectMapper mapper;


  public List<DigitalSpecimen> getDigitalSpecimenTargets(Set<String> specimenIds) {
    try {
      return context.select(DIGITAL_SPECIMEN.asterisk())
          .from(DIGITAL_SPECIMEN)
          .where(DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_ID.in(specimenIds))
          .fetch(dbRecord -> mapToDigitalSpecimen(dbRecord.get(DIGITAL_SPECIMEN.DATA)));
    } catch (DataAccessException ex) {
      log.error("Unable to get specimen from repository", ex);
      throw new DataBaseException(
          "Failed to get specimen from repository: " + specimenIds);
    }
  }

  private DigitalSpecimen mapToDigitalSpecimen(JSONB jsonb) {
    try {
      return mapper.readValue(jsonb.data(), DigitalSpecimen.class);
    } catch (JsonProcessingException e) {
      log.warn("Unable to map jsonb to digital specimen: {}", jsonb.data(), e);
      throw new DataBaseException(
          "Unable to map jsonb to digital specimen. Validation not possible.");
    }
  }

}
