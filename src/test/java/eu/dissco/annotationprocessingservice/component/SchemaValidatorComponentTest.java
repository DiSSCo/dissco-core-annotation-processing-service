package eu.dissco.annotationprocessingservice.component;

import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.JOB_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRequest;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingEvent;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SchemaValidatorComponentTest {

  private SchemaValidatorComponent schemaValidator;

  @BeforeEach
  void setup() {
    schemaValidator = new SchemaValidatorComponent();
  }

  @Test
  void testValidateProcessResults() {
    // Given

    //String id, String type, String odsID, String jobId, List<AnnotationProcessingRequest> annotations, List<AnnotationBatchMetadata> batchMetadata, UUID batchId
    var event = new AnnotationProcessingEvent()
        .withAnnotations(List.of(givenAnnotationRequest()))
        .withJobId(JOB_ID);

    // Then
    assertDoesNotThrow(() -> schemaValidator.validateEvent(event));
  }

  @ParameterizedTest
  @MethodSource("invalidAnnotations")
  void testInvalidAnnotations(AnnotationProcessingRequest annotationRequest) {
    // Then
    assertThrows(AnnotationValidationException.class,
        () -> schemaValidator.validateAnnotationRequest(annotationRequest,
            true));
  }

  @Test
  void testUpdateMissingId() {
    // Given
    var annotationRequest = givenAnnotationRequest();

    // Then
    assertThrows(AnnotationValidationException.class,
        () -> schemaValidator.validateAnnotationRequest(annotationRequest,
            false));
  }

  @Test
  void testCreateMissingCreated() {
    // Given
    var annotationRequest = givenAnnotationRequest().withDctermsCreated(null);

    // Then
    assertThrows(AnnotationValidationException.class,
        () -> schemaValidator.validateAnnotationRequest(annotationRequest,
            false));
  }

  @ParameterizedTest
  @MethodSource("validAnnotations")
  void testValidAnnotation(AnnotationProcessingRequest annotationRequest, Boolean isNew) {

    // Then
    assertDoesNotThrow(() ->
        schemaValidator.validateAnnotationRequest(annotationRequest, isNew));
  }

  private static Stream<Arguments> validAnnotations() {
    return Stream.of(
        Arguments.of(givenAnnotationRequest(), true),
        Arguments.of(givenAnnotationRequest().withId(ID), false)
    );
  }

  private static Stream<Arguments> invalidAnnotations() {
    return Stream.of(
        Arguments.of(givenAnnotationRequest().withId(ID)),
        Arguments.of(givenAnnotationRequest().withDctermsCreator(null)),
        Arguments.of(givenAnnotationRequest().withDctermsCreated(null))
    );
  }

}
