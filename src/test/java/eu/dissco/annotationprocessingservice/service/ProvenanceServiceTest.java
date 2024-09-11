package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.CREATOR;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.UPDATED;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenCreator;
import static eu.dissco.annotationprocessingservice.TestUtils.givenGenerator;
import static eu.dissco.annotationprocessingservice.TestUtils.givenTombstoneAnnotation;
import static eu.dissco.annotationprocessingservice.TestUtils.givenTombstoneMetadata;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.schema.Agent;
import eu.dissco.annotationprocessingservice.schema.Annotation.OdsStatus;
import eu.dissco.annotationprocessingservice.schema.OdsChangeValue;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ProvenanceServiceTest {

  @Mock
  private ApplicationProperties properties;

  private ProvenanceService service;

  private static List<Agent> givenExpectedAgents() {
    return List.of(
        givenCreator(CREATOR),
        givenGenerator()
    );
  }

  @BeforeEach
  void setup() {
    this.service = new ProvenanceService(MAPPER, properties);
  }

  @Test
  void testGenerateCreateEvent() {
    // Given
    var annotation = givenAnnotationProcessed();

    // When
    var event = service.generateCreateEvent(annotation);

    // Then
    assertThat(event.getOdsID()).isEqualTo(ID + "/" + 1);
    assertThat(event.getProvActivity().getOdsChangeValue()).isNull();
    assertThat(event.getProvEntity().getProvValue()).isNotNull();
    assertThat(event.getProvActivity().getRdfsComment()).isEqualTo("Annotation newly created");
    assertThat(event.getOdsHasProvAgent()).isEqualTo(givenExpectedAgents());
  }

  @Test
  void testGenerateUpdateEvent() {
    // Given
    var currentAnnotation = givenAnnotationProcessed();
    var annotation = givenAnnotationProcessed();
    annotation.setOaMotivatedBy("An updated motivation");

    // When
    var event = service.generateUpdateEvent(annotation, currentAnnotation);

    // Then
    assertThat(event.getOdsID()).isEqualTo(ID + "/" + 1);
    assertThat(event.getProvActivity().getOdsChangeValue()).isEqualTo(givenChangeValue());
    assertThat(event.getProvEntity().getProvValue()).isNotNull();
    assertThat(event.getProvActivity().getRdfsComment()).isEqualTo("Annotation updated");
    assertThat(event.getOdsHasProvAgent()).isEqualTo(givenExpectedAgents());
  }

  @Test
  void testGenerateTombstoneEvent() {
    // Given
    var annotation = givenAnnotationProcessed();
    var tombstoneAnnotation = givenTombstoneAnnotation();

    // When
    var event = service.generateTombstoneEvent(tombstoneAnnotation, annotation);

    // Then
    assertThat(event.getOdsID()).isEqualTo(ID + "/" + 1);
    assertThat(event.getProvActivity().getOdsChangeValue()).isEqualTo(givenTombstoneChangeValue());
    assertThat(event.getProvEntity().getProvValue()).isNotNull();
    assertThat(event.getProvActivity().getRdfsComment()).isEqualTo("Annotation tombstoned");
    assertThat(event.getOdsHasProvAgent()).isEqualTo(givenExpectedAgents());
  }

  private static List<OdsChangeValue> givenChangeValue() {
    return List.of(
        givenOdsChangeValue("add", "/oa:motivatedBy", "An updated motivation")
    );
  }

  private static List<OdsChangeValue> givenTombstoneChangeValue() {
    return List.of(
        givenOdsChangeValue("add", "/ods:TombstoneMetadata", givenTombstoneMetadata()),
        givenOdsChangeValue("replace", "/dcterms:modified", UPDATED),
        givenOdsChangeValue("replace", "/ods:version", 2),
        givenOdsChangeValue("replace", "/ods:status", OdsStatus.ODS_TOMBSTONE)
    );
  }

  private static OdsChangeValue givenOdsChangeValue(String op, String path, Object value) {
    return new OdsChangeValue()
        .withAdditionalProperty("op", op)
        .withAdditionalProperty("path", path)
        .withAdditionalProperty("value", MAPPER.convertValue(value, new TypeReference<>() {
        }));
  }

}
