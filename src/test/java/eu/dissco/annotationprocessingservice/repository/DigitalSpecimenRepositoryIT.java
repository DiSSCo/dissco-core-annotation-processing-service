package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.TestUtils.DOI_PROXY;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenDigitalSpecimen;
import static eu.dissco.annotationprocessingservice.database.jooq.Tables.DIGITAL_SPECIMEN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import io.github.dissco.core.annotationlogic.schema.DigitalSpecimen;
import java.util.List;
import java.util.Set;
import org.jooq.JSONB;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DigitalSpecimenRepositoryIT extends BaseRepositoryIT {

  private DigitalSpecimenRepository digitalSpecimenRepository;

  @BeforeEach
  void setup() {
    digitalSpecimenRepository = new DigitalSpecimenRepository(context, MAPPER);
  }

  @AfterEach
  void destroy() {
    context.truncate(DIGITAL_SPECIMEN).cascade().execute();
  }

  @Test
  void testGetDigitalSpecimenTargets() throws Exception {
    // Given
    var specimen = givenDigitalSpecimen();
    var id = specimen.getDctermsIdentifier().replace(DOI_PROXY, "");
    insertIntoDb(specimen, MAPPER.writeValueAsString(specimen));

    // When
    var result = digitalSpecimenRepository.getDigitalSpecimenTargets(Set.of(id));

    // Then
    assertThat(result).isEqualTo(List.of(specimen));
  }

  @Test
  void testGetSpecimenJsonException() throws Exception {
    // Given
    var specimen = givenDigitalSpecimen();
    insertIntoDb(specimen, MAPPER.writeValueAsString(givenAnnotationProcessed()));
    var id = specimen.getDctermsIdentifier().replace(DOI_PROXY, "");

    // When / Then
    assertThrowsExactly(DataBaseException.class,
        () -> digitalSpecimenRepository.getDigitalSpecimenTargets(Set.of(id)));
  }

  private void insertIntoDb(DigitalSpecimen specimen, String specimenStr) {
    context.insertInto(DIGITAL_SPECIMEN)
        .set(DIGITAL_SPECIMEN.ID, specimen.getId().replace(DOI_PROXY, ""))
        .set(DIGITAL_SPECIMEN.VERSION, specimen.getOdsVersion())
        .set(DIGITAL_SPECIMEN.TYPE, specimen.getOdsFdoType())
        .set(DIGITAL_SPECIMEN.MIDSLEVEL,
            specimen.getOdsMidsLevel().shortValue())
        .set(DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_ID,
            specimen.getOdsPhysicalSpecimenID())
        .set(DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_TYPE,
            specimen.getOdsPhysicalSpecimenIDType().value())
        .set(DIGITAL_SPECIMEN.SPECIMEN_NAME,
            specimen.getOdsSpecimenName())
        .set(DIGITAL_SPECIMEN.ORGANIZATION_ID,
            specimen.getOdsOrganisationID())
        .set(DIGITAL_SPECIMEN.SOURCE_SYSTEM_ID,
            specimen.getOdsSourceSystemID())
        .set(DIGITAL_SPECIMEN.CREATED,
            specimen.getDctermsCreated().toInstant())
        .set(DIGITAL_SPECIMEN.LAST_CHECKED,
            specimen.getDctermsCreated().toInstant())
        .set(DIGITAL_SPECIMEN.DATA, JSONB.jsonb(specimenStr))
        .execute();

  }

}
