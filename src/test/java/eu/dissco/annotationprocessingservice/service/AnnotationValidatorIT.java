package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.JOB_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.TARGET_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRequest;
import static eu.dissco.annotationprocessingservice.TestUtils.givenDigitalSpecimen;
import static eu.dissco.annotationprocessingservice.TestUtils.givenRequestOaTarget;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.BDDMockito.given;

import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import eu.dissco.annotationprocessingservice.properties.FdoProperties;
import eu.dissco.annotationprocessingservice.repository.DigitalSpecimenRepository;
import eu.dissco.annotationprocessingservice.schema.Agent;
import eu.dissco.annotationprocessingservice.schema.Agent.Type;
import eu.dissco.annotationprocessingservice.schema.AnnotationBody;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingEvent;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest.OaMotivation;
import eu.dissco.annotationprocessingservice.schema.AnnotationTarget;
import eu.dissco.annotationprocessingservice.schema.Identifier;
import eu.dissco.annotationprocessingservice.schema.Identifier.OdsGupriLevel;
import eu.dissco.annotationprocessingservice.schema.Identifier.OdsIdentifierStatus;
import eu.dissco.annotationprocessingservice.schema.OaHasSelector;
import eu.dissco.annotationprocessingservice.schema.OdsHasRole;
import io.github.dissco.annotationlogic.configuration.AnnotationLogicLibraryConfiguration;
import io.github.dissco.core.annotationlogic.schema.DigitalSpecimen;
import io.github.dissco.core.annotationlogic.schema.Event;
import io.github.dissco.core.annotationlogic.schema.Location;
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

  @ParameterizedTest
  @MethodSource("validAnnotations")
  void testValidateAnnotations(AnnotationProcessingRequest annotation, DigitalSpecimen digitalSpecimen) {

    // Given
    given(digitalSpecimenRepository.getDigitalSpecimenTargets(anySet())).willReturn(List.of(digitalSpecimen));

    // When / Then
    assertDoesNotThrow(
        () -> annotationValidatorService.validateAnnotationRequest(List.of(annotation), true));
  }

  @Test
  void testValidateAnnotationEvent() {
    // Given
    var digitalSpecimen = givenDigitalSpecimen().withOdsHasEvents(List.of(givenEvent()));
    given(digitalSpecimenRepository.getDigitalSpecimenTargets(anySet())).willReturn(List.of(digitalSpecimen));
    var event = new AnnotationProcessingEvent()
        .withJobId(JOB_ID)
            .withAnnotations(List.of(givenAnnotationRequest().withOaMotivation(OaMotivation.OA_EDITING)));

    // When / Then
    assertDoesNotThrow(() -> annotationValidatorService.validateEvent(event));
  }

  @Test
  void testInvalidAnnotationContents() {
    var digitalSpecimen = givenDigitalSpecimen();
    given(digitalSpecimenRepository.getDigitalSpecimenTargets(anySet())).willReturn(List.of(digitalSpecimen));
    var event = new AnnotationProcessingEvent()
        .withJobId(JOB_ID)
        .withAnnotations(List.of(givenAnnotationRequest().withOaMotivation(OaMotivation.OA_EDITING)));

    // When / Then
    assertThrowsExactly(AnnotationValidationException.class, () -> annotationValidatorService.validateEvent(event));
  }


  private static Stream<Arguments> invalidAnnotationId() {
    return Stream.of(
        Arguments.of(givenAnnotationRequest().withId(ID), true),
        Arguments.of(givenAnnotationRequest(), false));
  }

  private static Stream<Arguments> validAnnotations() {
    return Stream.of(
        Arguments.of(
            givenAnnotationRequest().withOaMotivation(OaMotivation.OA_EDITING),
            givenDigitalSpecimen().withOdsHasEvents(List.of(givenEvent()))
        ),
        Arguments.of(
            givenAnnotationRequest().withOaMotivation(OaMotivation.OA_EDITING)
                .withDctermsCreator(new Agent()
                    .withOdsHasIdentifiers(List.of(
                        new Identifier()
                            .withType("ods:Identifier")
                            .withDctermsTitle("Title")
                            .withDctermsType(Identifier.DctermsType.ARK)
                            .withDctermsFormat(List.of("format"))
                            .withDctermsSubject(List.of("subject"))
                            .withOdsIsPartOfLabel(false)
                            .withOdsGupriLevel(OdsGupriLevel.GLOBALLY_UNIQUE_STABLE_PERSISTENT_RESOLVABLE)
                            .withOdsIdentifierStatus(OdsIdentifierStatus.PREFERRED)
                            .withDctermsIdentifier(JOB_ID)
                    ))
                    .withId(JOB_ID)
                    .withType(Type.SCHEMA_PERSON)
                    .withSchemaName("schemaName")
                    .withOdsHasRoles(List.of(new OdsHasRole()
                        .withSchemaRoleName("creator")))
                ),
            givenDigitalSpecimen().withOdsHasEvents(List.of(givenEvent()))
        ),
        Arguments.of(
            givenAnnotationRequest().withOaMotivation(OaMotivation.ODS_ADDING)
                .withOaHasTarget(givenRequestOaTarget(TARGET_ID)
                    .withOaHasSelector(new OaHasSelector()
                        .withAdditionalProperty("ods:term",
                            "$['dwc:organismQuantityType']")
                        .withAdditionalProperty("@type", "ods:TermSelector"))),
            givenDigitalSpecimen()
        ),
        Arguments.of(
            givenAnnotationRequest().withOaMotivation(OaMotivation.OA_EDITING)
                .withOaHasTarget(localityTargetEdit())
                .withOaHasBody(localityBody()),
            givenDigitalSpecimen().withOdsHasEvents(List.of(givenEvent()))
        ),
        Arguments.of(
            givenAnnotationRequest().withOaMotivation(OaMotivation.ODS_ADDING)
                .withOaHasTarget(geologicalContextAdd())
                .withOaHasBody(geologicalContext()),
            givenDigitalSpecimen()
        ),
        Arguments.of(
            givenAnnotationRequest().withOaMotivation(OaMotivation.ODS_DELETING).withOaHasBody(new AnnotationBody().withOaValue(List.of())),
            givenDigitalSpecimen().withOdsHasEvents(List.of(givenEvent()))
        )
    );
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

  private static AnnotationTarget localityTargetEdit() {
    return new AnnotationTarget()
        .withId(TARGET_ID)
        .withDctermsIdentifier(TARGET_ID)
        .withType("ods:DigitalSpecimen")
        .withOaHasSelector(
            new OaHasSelector()
                .withAdditionalProperty("@type", "ods:ClassSelector")
                .withAdditionalProperty("ods:class", "$['ods:hasEvents'][0]['ods:hasLocation']"));
  }

  private static AnnotationTarget geologicalContextAdd() {
    return new AnnotationTarget()
        .withId(TARGET_ID)
        .withDctermsIdentifier(TARGET_ID)
        .withType("ods:DigitalSpecimen")
        .withOaHasSelector(
            new OaHasSelector()
                .withAdditionalProperty("@type", "ods:ClassSelector")
                .withAdditionalProperty("ods:class",
                    "$['ods:hasEvents'][0]['ods:hasLocation']['ods:hasGeologicalContext']"));
  }

  private static AnnotationBody localityBody() {
    return new AnnotationBody().withOaValue(List.of("""
        {
          "dwc:country": "Some new value!",
          "dwc:locality" : "Some new value!"
        }
        """));
  }

  private static AnnotationBody geologicalContext() {
    return new AnnotationBody().withOaValue(List.of(
        """
            {
              "dwc:lithostratigraphicTerms" : "Some new value!"
            }
            """
    ));
  }


}
