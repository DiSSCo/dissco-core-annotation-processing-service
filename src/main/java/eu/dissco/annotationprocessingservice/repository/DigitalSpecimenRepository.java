package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.configuration.ApplicationConfiguration.DOI_PROXY;
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
import org.jooq.Record;
import org.jooq.exception.DataAccessException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

@Slf4j
@Repository
@RequiredArgsConstructor
public class DigitalSpecimenRepository {

  private final DSLContext context;

  private final ObjectMapper mapper;


  public List<DigitalSpecimen> getDigitalSpecimenTargets(Set<String> specimenIds) {
    try {
      return context.select(DIGITAL_SPECIMEN.asterisk())
          .from(DIGITAL_SPECIMEN)
          .where(DIGITAL_SPECIMEN.ID.in(specimenIds))
          .fetch(this::mapToDigitalSpecimen);
    } catch (DataAccessException ex) {
      log.error("Unable to get specimen from repository", ex);
      throw new DataBaseException(
          "Failed to get specimen from repository: " + specimenIds);
    }
  }

  private DigitalSpecimen mapToDigitalSpecimen(Record dbRecord) {
    try {
      return mapper.readValue(dbRecord.get(DIGITAL_SPECIMEN.DATA).data(), DigitalSpecimen.class)
          .withDctermsIdentifier(DOI_PROXY + dbRecord.get(DIGITAL_SPECIMEN.ID))
          .withId(DOI_PROXY + dbRecord.get(DIGITAL_SPECIMEN.ID));
    } catch (JsonProcessingException e) {
      log.warn("Unable to map jsonb to digital specimen: {}", dbRecord.get(DIGITAL_SPECIMEN.DATA).data(), e);
      throw new DataBaseException(
          "Unable to map jsonb to digital specimen. Validation not possible.");
    }
  }

}
