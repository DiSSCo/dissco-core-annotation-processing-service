package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.database.jooq.Tables.MACHINE_ANNOTATION_SERVICES;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.domain.MasInput;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class MachineAnnotationServiceRepository {
  private final DSLContext context;
  private final ObjectMapper mapper;

  public Optional<MasInput> getMasInput(String masId){
    return context.select(MACHINE_ANNOTATION_SERVICES.asterisk())
        .from(MACHINE_ANNOTATION_SERVICES)
        .where(MACHINE_ANNOTATION_SERVICES.ID.eq(masId))
        .fetchSingle(this::mapToMasInput);
  }

  private Optional<MasInput> mapToMasInput(Record masRecord) {
    var masInput = masRecord.get(MACHINE_ANNOTATION_SERVICES.MAS_INPUT);
    try {
      return masInput ==  null ? Optional.empty() : Optional.of(mapper.readValue(masInput.data(), MasInput.class));
    } catch (JsonProcessingException e){
      throw new DataBaseException("Unable to parse MasInput: " + masRecord.get(MACHINE_ANNOTATION_SERVICES.MAS_INPUT).data());
    }

  }

}
