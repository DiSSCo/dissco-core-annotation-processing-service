package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.TestUtils.CREATED;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.givenMasInput;
import static eu.dissco.annotationprocessingservice.database.jooq.Tables.MACHINE_ANNOTATION_SERVICES;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import eu.dissco.annotationprocessingservice.database.jooq.tables.records.MachineAnnotationServicesRecord;
import eu.dissco.annotationprocessingservice.domain.MasInput;
import java.util.List;
import org.jooq.JSONB;
import org.jooq.exception.NoDataFoundException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MachineAnnotationServiceRepositoryIT extends BaseRepositoryIT {

  private MachineAnnotationServiceRepository repository;


  @BeforeEach
  void setup() {
    repository = new MachineAnnotationServiceRepository(context, MAPPER);
  }

  @AfterEach
  void destroy() {
    context.truncate(MACHINE_ANNOTATION_SERVICES).execute();
  }

  @Test
  void testMachineAnnotationServiceHasMasInput() throws Exception{
    // Given
    populateDatabase(true);

    // When
    var result = repository.getMasInput(ID);

    // Then
    assertThat(result).contains(givenMasInput());
  }

  @Test
  void testMachineAnnotationServiceHasNoMasInput() throws Exception{
    // Given
    populateDatabase(false);

    // When
    var result = repository.getMasInput(ID);

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  void testMachineAnnotationServiceMasDoesntExist() {
    // Then
    assertThrows(NoDataFoundException.class, () -> repository.getMasInput(ID));
  }

  void populateDatabase(boolean hasMasInput) throws Exception {
    var masInput = hasMasInput ? JSONB.jsonb(MAPPER.writeValueAsString(givenMasInput())) : null;
    context.insertInto(MACHINE_ANNOTATION_SERVICES)
        .set(MACHINE_ANNOTATION_SERVICES.ID, ID)
        .set(MACHINE_ANNOTATION_SERVICES.VERSION, 1)
        .set(MACHINE_ANNOTATION_SERVICES.NAME, "Name")
        .set(MACHINE_ANNOTATION_SERVICES.CREATED, CREATED)
        .set(MACHINE_ANNOTATION_SERVICES.ADMINISTRATOR, "ADMIN")
        .set(MACHINE_ANNOTATION_SERVICES.CONTAINER_IMAGE, "IMG")
        .set(MACHINE_ANNOTATION_SERVICES.CONTAINER_IMAGE_TAG, "Tag")
        .set(MACHINE_ANNOTATION_SERVICES.MAS_INPUT, masInput)
        .execute();
  }
}
