package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRequest;
import static eu.dissco.annotationprocessingservice.TestUtils.givenDigitalSpecimen;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.BDDMockito.given;

import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import eu.dissco.annotationprocessingservice.properties.FdoProperties;
import eu.dissco.annotationprocessingservice.repository.DigitalSpecimenRepository;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;
import io.github.dissco.annotationlogic.configuration.AnnotationLogicLibraryConfiguration;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {AnnotationValidatorService.class,
    AnnotationLogicLibraryConfiguration.class})
class AnnotationValidatorIT {

  @Autowired
  private AnnotationValidatorService annotationValidatorService;

  @MockitoBean
  private DigitalSpecimenRepository digitalSpecimenRepository;

  @MockitoBean
  private FdoProperties fdoProperties;


  @Test
  void testValidateAnnotationRequest() throws AnnotationValidationException {
    // Given
    given(digitalSpecimenRepository.getDigitalSpecimenTargets(anySet())).willReturn(
        List.of(givenDigitalSpecimen()));

    // When
    annotationValidatorService.validateAnnotationRequest(List.of(givenAnnotationRequest()),
        true);
  }

  @ParameterizedTest
  @MethodSource("invalidAnnotationId")
  void testValidateAnnotationsInvalidId(AnnotationProcessingRequest annotationRequest,
      boolean isNew) {
    // Given
    var request = List.of(annotationRequest);

    // When / Then
    assertThrowsExactly(AnnotationValidationException.class,
        () -> annotationValidatorService.validateAnnotationRequest(request, isNew));

  }


  private static Stream<Arguments> invalidAnnotationId() {
    return Stream.of(
        Arguments.of(givenAnnotationRequest().withId(ID), true),
        Arguments.of(givenAnnotationRequest(), false));
  }

}
