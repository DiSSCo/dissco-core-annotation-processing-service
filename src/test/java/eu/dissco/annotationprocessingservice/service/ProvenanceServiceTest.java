package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.CREATOR;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenCreator;
import static eu.dissco.annotationprocessingservice.TestUtils.givenGenerator;
import static org.assertj.core.api.Assertions.assertThat;

import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.schema.Agent;
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
    assertThat(event.getOdsHasProvAgent()).isEqualTo(givenExpectedAgents());
  }

  @Test
  void testGenerateUpdateEvent() {
    // Given
    var annotation = givenAnnotationProcessed();
    var anotherAnnotation = givenAnnotationProcessed();
    anotherAnnotation.setOaMotivatedBy("An updated motivation");

    // When
    var event = service.generateUpdateEvent(annotation, anotherAnnotation);

    // Then
    assertThat(event.getOdsID()).isEqualTo(ID + "/" + 1);
    assertThat(event.getProvActivity().getOdsChangeValue()).isEqualTo(givenChangeValue());
    assertThat(event.getProvEntity().getProvValue()).isNotNull();
    assertThat(event.getOdsHasProvAgent()).isEqualTo(givenExpectedAgents());
  }

  List<OdsChangeValue> givenChangeValue() {
    return List.of(new OdsChangeValue()
        .withAdditionalProperty("op", "add")
        .withAdditionalProperty("path", "/oa:motivatedBy")
        .withAdditionalProperty("value", "An updated motivation")
    );
  }
}
