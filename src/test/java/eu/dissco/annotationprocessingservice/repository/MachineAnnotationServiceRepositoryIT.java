package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.TestUtils.CREATED;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.ID_ALT;
import static eu.dissco.annotationprocessingservice.TestUtils.JOB_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.givenMasInput;
import static eu.dissco.annotationprocessingservice.database.jooq.Tables.MACHINE_ANNOTATION_SERVICES;
import static eu.dissco.annotationprocessingservice.database.jooq.Tables.MAS_JOB_RECORD_NEW;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import eu.dissco.annotationprocessingservice.database.jooq.enums.MjrJobState;
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
    context.truncate(MAS_JOB_RECORD_NEW).execute();
    context.truncate(MACHINE_ANNOTATION_SERVICES).execute();
  }

  @Test
  void testMachineAnnotationServiceHasMasInput() throws Exception{
    // Given
    populateDatabase(true);

    // When
    var result = repository.getMasInput(JOB_ID);

    // Then
    assertThat(result).contains(givenMasInput());
  }

  @Test
  void testMachineAnnotationServiceHasNoMasInput() throws Exception{
    // Given
    populateDatabase(false);

    // When
    var result = repository.getMasInput(JOB_ID);

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  void testMachineAnnotationServiceMasDoesntExist() {
    // Then
    assertThrows(NoDataFoundException.class, () -> repository.getMasInput(JOB_ID));
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

    context.insertInto(MAS_JOB_RECORD_NEW)
        .set(MAS_JOB_RECORD_NEW.JOB_ID, JOB_ID)
        .set(MAS_JOB_RECORD_NEW.JOB_STATE, MjrJobState.SCHEDULED)
        .set(MAS_JOB_RECORD_NEW.MAS_ID, ID)
        .set(MAS_JOB_RECORD_NEW.TIME_STARTED, CREATED)
        .set(MAS_JOB_RECORD_NEW.TARGET_ID, ID_ALT)
        .execute();
  }
}
