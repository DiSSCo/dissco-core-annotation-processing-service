package eu.dissco.annotationprocessingservice.component;

import static eu.dissco.annotationprocessingservice.TestUtils.DOI_PROXY;
import static eu.dissco.annotationprocessingservice.TestUtils.HANDLE_PROXY;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.givenBatchMetadataLatitudeSearch;
import static eu.dissco.annotationprocessingservice.TestUtils.givenElasticDocument;
import static eu.dissco.annotationprocessingservice.TestUtils.givenOaTarget;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.Option;
import eu.dissco.annotationprocessingservice.domain.annotation.AnnotationTargetType;
import eu.dissco.annotationprocessingservice.domain.annotation.ClassSelector;
import eu.dissco.annotationprocessingservice.domain.annotation.FieldSelector;
import eu.dissco.annotationprocessingservice.domain.annotation.FragmentSelector;
import eu.dissco.annotationprocessingservice.domain.annotation.Target;
import eu.dissco.annotationprocessingservice.exception.BatchingException;
import java.util.List;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


@Slf4j
class JsonPathComponentTest {

  private JsonPathComponent jsonPathComponent;

  @BeforeEach
  void init() {
    var jsonPathConfiguration = Configuration.builder().options(Option.AS_PATH_LIST).build();
    var lastKeyMatcher = Pattern.compile("[^.]+(?=\\.$)|([^.]+$)");
    jsonPathComponent = new JsonPathComponent(MAPPER, jsonPathConfiguration, lastKeyMatcher);
  }


  @Test
  void testGetAnnotationTargetPathsClassSelector()
      throws JsonProcessingException, BatchingException {
    // Given
    var expected = List.of(
        new Target()
            .withOdsId(DOI_PROXY + ID)
            .withOdsType(AnnotationTargetType.DIGITAL_SPECIMEN)
            .withSelector(new ClassSelector("digitalSpecimenWrapper.occurrences[0].locality")),
        new Target()
            .withOdsId(DOI_PROXY + ID)
            .withOdsType(AnnotationTargetType.DIGITAL_SPECIMEN)
            .withSelector(new ClassSelector("digitalSpecimenWrapper.occurrences[2].locality")));

    var baseTargetClassSelector = new Target()
        .withOdsId(DOI_PROXY + ID)
        .withOdsType(AnnotationTargetType.DIGITAL_SPECIMEN)
        .withSelector(new ClassSelector("digitalSpecimenWrapper.occurrences[1].locality"));

    // When
    var result = jsonPathComponent.getAnnotationTargets(givenBatchMetadataLatitudeSearch(),
        givenElasticDocument(),
        baseTargetClassSelector);

    // Then
    assertThat(result).hasSameElementsAs(expected);
  }

  @Test
  void testGetAnnotationTargetPathsFieldSelector()
      throws JsonProcessingException, BatchingException {
    // Given
    var baseTargetClassSelector = givenOaTarget(ID);
    var expected = List.of(new Target()
            .withOdsId(DOI_PROXY + ID)
            .withOdsType(AnnotationTargetType.DIGITAL_SPECIMEN)
            .withSelector(new FieldSelector("digitalSpecimenWrapper.occurrences[0].locality")),
        new Target()
            .withOdsId(DOI_PROXY + ID)
            .withOdsType(AnnotationTargetType.DIGITAL_SPECIMEN)
            .withSelector(new FieldSelector("digitalSpecimenWrapper.occurrences[2].locality")));

    // When
    var result = jsonPathComponent.getAnnotationTargets(givenBatchMetadataLatitudeSearch(),
        givenElasticDocument(),
        baseTargetClassSelector);

    // Then
    assertThat(result).hasSameElementsAs(expected);
  }

  @Test
  void testGetAnnotationTargetPathsBadBatchMetadata()
      throws JsonProcessingException {
    // Given
    var baseTargetClassSelector = new Target()
        .withOdsId(HANDLE_PROXY + ID)
        .withOdsType(AnnotationTargetType.DIGITAL_SPECIMEN)
        .withSelector(new ClassSelector()
            .withOaClass("digitalSpecimenWrapper.occurrences[1].locality"));
    // Path is incorrect
    var batchMetadata = MAPPER.readTree("""
        {
          "digitalSpecimenWrapper.occurrences[*].location.georeference.dwc:decimalLatitude":11
        }
        """);

    //Then
    assertThrows(BatchingException.class,
        () -> jsonPathComponent.getAnnotationTargets(batchMetadata, givenElasticDocument(ID,
                "Netherlands"),
            baseTargetClassSelector));
  }

  @Test
  void testBadSelectorType() {
    // Given
    var baseTargetClassSelector = new Target()
        .withOdsId(ID)
        .withOdsType(AnnotationTargetType.DIGITAL_SPECIMEN)
        .withSelector(new FragmentSelector());

    // When
    assertThrows(BatchingException.class, () ->
        jsonPathComponent.getAnnotationTargets(givenBatchMetadataLatitudeSearch(),
            givenElasticDocument(),
            baseTargetClassSelector));
  }

  @Test
  void testBadJsonpathFormat() throws JsonProcessingException {
    // Given
    var batchMetadata = MAPPER.readTree("""
        {
          "[digitalSpecimenWrapper][occurrences][*][location][georeference]['dwc:decimalLatitude']['dwc:value']":11
        }
        """);
    var baseTargetClassSelector = new Target()
        .withOdsId(ID)
        .withOdsType(AnnotationTargetType.DIGITAL_SPECIMEN)
        .withSelector(new FieldSelector()
            .withOdsField("digitalSpecimenWrapper.occurrences[1].locality"));

    // When
    assertThrows(BatchingException.class, () ->
        jsonPathComponent.getAnnotationTargets(batchMetadata,
            givenElasticDocument(),
            baseTargetClassSelector));
  }
}
