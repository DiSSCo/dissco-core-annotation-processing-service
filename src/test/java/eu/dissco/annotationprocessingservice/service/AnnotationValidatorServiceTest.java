package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.JOB_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRequest;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import eu.dissco.annotationprocessingservice.properties.FdoProperties;
import eu.dissco.annotationprocessingservice.repository.DigitalSpecimenRepository;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingEvent;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;
import io.github.dissco.annotationlogic.validator.AnnotationValidator;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AnnotationValidatorServiceTest {

  private AnnotationValidatorService annotationValidatorComponent;
  @Mock
  private AnnotationValidator annotationValidator;
  @Mock
  private DigitalSpecimenRepository digitalSpecimenRepository;

  @BeforeEach
  void setup() {
    annotationValidatorComponent = new AnnotationValidatorService(annotationValidator,
        digitalSpecimenRepository, new FdoProperties());
  }

  @Test
  void testValidateProcessResults() {
    // Given

    var event = new AnnotationProcessingEvent()
        .withAnnotations(List.of(givenAnnotationRequest()))
        .withJobId(JOB_ID);

    // Then
    assertDoesNotThrow(() -> annotationValidatorComponent.validateEvent(event));
  }

  @ParameterizedTest
  @MethodSource("invalidAnnotations")
  void testInvalidIdAnnotations(AnnotationProcessingRequest annotationRequest, boolean isNew) {
    // Then
    assertThrows(AnnotationValidationException.class,
        () -> annotationValidatorComponent.validateAnnotationRequest(List.of(annotationRequest),
            isNew));
  }

  @Test
  void testUpdateMissingId() {
    // Given
    var annotationRequest = givenAnnotationRequest();

    // Then
    assertThrows(AnnotationValidationException.class,
        () -> annotationValidatorComponent.validateAnnotationRequest(List.of(annotationRequest),
            false));
  }

  @ParameterizedTest
  @MethodSource("validAnnotations")
  void testValidAnnotation(AnnotationProcessingRequest annotationRequest, Boolean isNew) {

    // Then
    assertDoesNotThrow(
        () -> annotationValidatorComponent.validateAnnotationRequest(List.of(annotationRequest),
            isNew));
  }

  private static Stream<Arguments> validAnnotations() {
    return Stream.of(
        Arguments.of(givenAnnotationRequest(), true),
        Arguments.of(givenAnnotationRequest().withId(ID), false)
    );
  }

  private static Stream<Arguments> invalidAnnotations() {
    return Stream.of(
        Arguments.of(givenAnnotationRequest().withId(ID), true),
        Arguments.of(givenAnnotationRequest(), false));
  }

}
