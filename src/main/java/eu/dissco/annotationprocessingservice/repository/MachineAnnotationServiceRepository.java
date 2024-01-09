package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.database.jooq.Tables.MACHINE_ANNOTATION_SERVICES;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.domain.MasInput;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class MachineAnnotationServiceRepository {
  private final DSLContext context;
  private final ObjectMapper mapper;

  public MasInput getMasInput(String masId){
    return context.select(MACHINE_ANNOTATION_SERVICES.asterisk())
        .from(MACHINE_ANNOTATION_SERVICES)
        .where(MACHINE_ANNOTATION_SERVICES.ID.eq(masId))
        .fetchOne(this::mapToMasInput);
  }

  private MasInput mapToMasInput(Record masRecord) {
    try {
      return mapper.readValue(masRecord.get(MACHINE_ANNOTATION_SERVICES.MAS_INPUT).data(), MasInput.class);
    } catch (JsonProcessingException e){
      throw new DataBaseException("Unable to parse MasInput: " + masRecord.get(MACHINE_ANNOTATION_SERVICES.MAS_INPUT).data());
    }

  }

}
