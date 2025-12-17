package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.COMMENT;
import static eu.dissco.annotationprocessingservice.TestUtils.CREATED;
import static eu.dissco.annotationprocessingservice.TestUtils.CREATOR;
import static eu.dissco.annotationprocessingservice.TestUtils.DOI_PROXY;
import static eu.dissco.annotationprocessingservice.TestUtils.FDO_TYPE;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.JOB_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.PROCESSOR_HANDLE;
import static eu.dissco.annotationprocessingservice.TestUtils.PROCESSOR_NAME;
import static eu.dissco.annotationprocessingservice.TestUtils.TARGET_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRequest;
import static eu.dissco.annotationprocessingservice.TestUtils.givenDigitalSpecimen;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;

import eu.dissco.annotationprocessingservice.domain.AnnotationTargetType;
import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.properties.FdoProperties;
import eu.dissco.annotationprocessingservice.repository.DigitalSpecimenRepository;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingEvent;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest.OaMotivation;
import io.github.dissco.annotationlogic.exception.InvalidAnnotationException;
import io.github.dissco.annotationlogic.validator.AnnotationValidator;
import io.github.dissco.core.annotationlogic.schema.Agent.Type;
import io.github.dissco.core.annotationlogic.schema.Annotation.OdsStatus;
import io.github.dissco.core.annotationlogic.schema.Event;
import io.github.dissco.core.annotationlogic.schema.Identifier.DctermsType;
import io.github.dissco.core.annotationlogic.schema.Identifier.OdsGupriLevel;
import io.github.dissco.core.annotationlogic.schema.Identifier.OdsIdentifierStatus;
import io.github.dissco.core.annotationlogic.schema.Location;
import io.github.dissco.core.annotationlogic.schema.OdsHasRole;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AnnotationValidatorServiceTest {

  private static final String PLACEHOLDER_HANDLE = "https://hdl.handle.net/20.5000.1025/AAA-BBB-CCC";

  private AnnotationValidatorService annotationValidatorService;
  @Mock
  private AnnotationValidator annotationValidator;
  @Mock
  private DigitalSpecimenRepository digitalSpecimenRepository;
  @Mock
  ApplicationProperties applicationProperties;
  private final Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
  private final Instant instant = Instant.now(clock);


  @BeforeEach
  void setup() {
    annotationValidatorService = new AnnotationValidatorService(annotationValidator,
        digitalSpecimenRepository, new FdoProperties(), applicationProperties);
  }

  @Test
  void testValidateProcessResults() {
    // Given

    var event = new AnnotationProcessingEvent()
        .withAnnotations(List.of(givenAnnotationRequest()))
        .withJobId(JOB_ID);

    // Then
    assertDoesNotThrow(() -> annotationValidatorService.validateEvent(event));
  }

  @ParameterizedTest
  @MethodSource("invalidAnnotations")
  void testInvalidIdAnnotations(AnnotationProcessingRequest annotationRequest, boolean isNew) {
    // Then
    assertThrows(AnnotationValidationException.class,
        () -> annotationValidatorService.validateAnnotationRequest(List.of(annotationRequest),
            isNew));
  }

  @Test
  void testUpdateMissingId() {
    // Given
    var annotationRequest = givenAnnotationRequest();

    // Then
    assertThrows(AnnotationValidationException.class,
        () -> annotationValidatorService.validateAnnotationRequest(List.of(annotationRequest),
            false));
  }

  @ParameterizedTest
  @MethodSource("validAnnotations")
  void testValidAnnotation(AnnotationProcessingRequest annotationRequest, Boolean isNew) {

    // Then
    assertDoesNotThrow(
        () -> annotationValidatorService.validateAnnotationRequest(List.of(annotationRequest),
            isNew));
  }

  @Test
  void testValidateAnnotationsWithLibrary() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest().withOaMotivation(OaMotivation.OA_EDITING);
    var digitalSpecimen = givenDigitalSpecimen().withOdsHasEvents(List.of(givenEvent()));
    given(digitalSpecimenRepository.getDigitalSpecimenTargets(anySet())).willReturn(
        List.of(digitalSpecimen));
    given(applicationProperties.isValidateAnnotations()).willReturn(true);
    given(applicationProperties.getProcessorHandle()).willReturn(PROCESSOR_HANDLE);
    given(applicationProperties.getProcessorName()).willReturn(PROCESSOR_NAME);
    try (
        MockedStatic<Clock> mockedClock = mockStatic(Clock.class);
        MockedStatic<Instant> mockedInstant = mockStatic(Instant.class)
    ) {
      initTime(mockedClock, mockedInstant);

      // When
      annotationValidatorService.validateAnnotationRequest(List.of(annotationRequest), true);

      // Then
      then(annotationValidator).should()
          .applyAnnotation(digitalSpecimen, givenTransformedAnnotation());
    }
  }

  @Test
  void testValidateAnnotationsTargetNotFound() {
    // Given
    var annotationRequest = givenAnnotationRequest().withOaMotivation(OaMotivation.OA_EDITING);
    given(applicationProperties.isValidateAnnotations()).willReturn(true);

    // When / Then
    assertThrows(AnnotationValidationException.class,
        () -> annotationValidatorService.validateAnnotationRequest(List.of(annotationRequest),
            true));

  }

  @Test
  void testValidateAnnotationsWithLibraryValidationException() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest().withOaMotivation(OaMotivation.OA_EDITING);
    var digitalSpecimen = givenDigitalSpecimen().withOdsHasEvents(List.of(givenEvent()));
    given(digitalSpecimenRepository.getDigitalSpecimenTargets(anySet())).willReturn(
        List.of(digitalSpecimen));
    given(applicationProperties.isValidateAnnotations()).willReturn(true);
    given(applicationProperties.getProcessorHandle()).willReturn(PROCESSOR_HANDLE);
    given(applicationProperties.getProcessorName()).willReturn(PROCESSOR_NAME);
    try (
        MockedStatic<Clock> mockedClock = mockStatic(Clock.class);
        MockedStatic<Instant> mockedInstant = mockStatic(Instant.class)
    ) {
      initTime(mockedClock, mockedInstant);
      doThrow(InvalidAnnotationException.class).when(annotationValidator).applyAnnotation(digitalSpecimen, givenTransformedAnnotation());

      // When / Then
      assertThrows(AnnotationValidationException.class,
          () -> annotationValidatorService.validateAnnotationRequest(List.of(annotationRequest),
              true));
    }
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

  private static Event givenEvent() {
    return new Event()
        .withDwcEventDate("2022-11-01T09:59:24.000Z")
        .withType("ods:Event")
        .withOdsHasLocation(new Location()
            .withType("ods:Location")
            .withDwcCountry("England")
            .withDwcLocality("London"));
  }

  private void initTime(MockedStatic<Clock> mockedClock, MockedStatic<Instant> mockedInstant) {
    mockedClock.when(Clock::systemUTC).thenReturn(clock);
    mockedInstant.when(Instant::now).thenReturn(instant);
    mockedInstant.when(() -> Instant.from(any())).thenReturn(instant);
  }

  private static io.github.dissco.core.annotationlogic.schema.Annotation givenTransformedAnnotation() {
    return new io.github.dissco.core.annotationlogic.schema.Annotation()
        .withId(PLACEHOLDER_HANDLE)
        .withDctermsIdentifier(PLACEHOLDER_HANDLE)
        .withOdsVersion(1)
        .withType("ods:Annotation")
        .withOdsFdoType(FDO_TYPE)
        .withOdsStatus(OdsStatus.ACTIVE)
        .withOaHasBody(
            new io.github.dissco.core.annotationlogic.schema.AnnotationBody()
                .withType("oa:TextualBody")
                .withOaValue(new ArrayList<>(List.of(COMMENT)))
                .withDctermsReferences(
                    "https://medialib.naturalis.nl/file/id/ZMA.UROCH.P.1555/format/large")
                .withOdsScore(0.99)
        )
        .withOaMotivation(
            io.github.dissco.core.annotationlogic.schema.Annotation.OaMotivation.OA_EDITING)
        .withOaHasTarget(
            new io.github.dissco.core.annotationlogic.schema.AnnotationTarget()
                .withId(DOI_PROXY + TARGET_ID)
                .withType("ods:DigitalSpecimen")
                .withOdsFdoType(AnnotationTargetType.DIGITAL_SPECIMEN.getFdoType())
                .withDctermsIdentifier(DOI_PROXY + TARGET_ID)
                .withOaHasSelector(new io.github.dissco.core.annotationlogic.schema.OaHasSelector()
                    .withAdditionalProperty("ods:term",
                        "$['ods:hasEvents'][0]['ods:hasLocation']['dwc:locality']")
                    .withAdditionalProperty("@type", "ods:TermSelector")
                ))
        .withDctermsCreated(Date.from(CREATED))
        .withDctermsModified(Date.from(CREATED))
        .withDctermsIssued(Date.from(CREATED))
        .withDctermsCreator(new io.github.dissco.core.annotationlogic.schema.Agent()
            .withSchemaName("Test User")
            .withId(CREATOR)
            .withType(io.github.dissco.core.annotationlogic.schema.Agent.Type.SCHEMA_PERSON))
        .withAsGenerator(new io.github.dissco.core.annotationlogic.schema.Agent()
            .withId(PROCESSOR_HANDLE)
            .withSchemaName(PROCESSOR_NAME)
            .withOdsHasIdentifiers(List.of(
                new io.github.dissco.core.annotationlogic.schema.Identifier()
                    .withType("ods:Identifier")
                    .withId(PROCESSOR_HANDLE)
                    .withDctermsIdentifier(PROCESSOR_HANDLE)
                    .withOdsIsPartOfLabel(false)
                    .withDctermsTitle("DOI")
                    .withDctermsType(DctermsType.DOI)
                    .withOdsIdentifierStatus(OdsIdentifierStatus.PREFERRED)
                    .withOdsGupriLevel(
                        OdsGupriLevel.GLOBALLY_UNIQUE_STABLE_PERSISTENT_RESOLVABLE_FDO_COMPLIANT)
            ))
            .withOdsHasRoles(List.of(
                new OdsHasRole().withType("schema:Role").withSchemaRoleName("processing-service")))
            .withType(Type.SCHEMA_SOFTWARE_APPLICATION)
        );
  }


}
