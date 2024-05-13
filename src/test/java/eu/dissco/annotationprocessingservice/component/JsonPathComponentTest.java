package eu.dissco.annotationprocessingservice.component;

import static com.jayway.jsonpath.JsonPath.using;
import static eu.dissco.annotationprocessingservice.TestUtils.DOI_PROXY;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.givenBatchMetadataLatitudeSearch;
import static eu.dissco.annotationprocessingservice.TestUtils.givenElasticDocument;
import static eu.dissco.annotationprocessingservice.TestUtils.givenOaTarget;
import static eu.dissco.annotationprocessingservice.TestUtils.givenSelector;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.in;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.Option;
import eu.dissco.annotationprocessingservice.domain.BatchMetadata;
import eu.dissco.annotationprocessingservice.domain.BatchMetadataExtended;
import eu.dissco.annotationprocessingservice.domain.BatchMetadataSearchParam;
import eu.dissco.annotationprocessingservice.domain.annotation.AnnotationTargetType;
import eu.dissco.annotationprocessingservice.domain.annotation.ClassSelector;
import eu.dissco.annotationprocessingservice.domain.annotation.FieldSelector;
import eu.dissco.annotationprocessingservice.domain.annotation.FragmentSelector;
import eu.dissco.annotationprocessingservice.domain.annotation.Target;
import eu.dissco.annotationprocessingservice.exception.BatchingException;
import eu.dissco.annotationprocessingservice.exception.BatchingRuntimeException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


@Slf4j
class JsonPathComponentTest {

  private JsonPathComponent jsonPathComponent;
  Configuration jsonPathConfiguration = Configuration.builder()
      .options(Option.AS_PATH_LIST, Option.SUPPRESS_EXCEPTIONS, Option.ALWAYS_RETURN_LIST)
      .build();

  @BeforeEach
  void init() {
    jsonPathComponent = new JsonPathComponent(MAPPER, jsonPathConfiguration);
  }

  @Test
  void testGetAnnotationTargetPathsClassSelector()
      throws JsonProcessingException, BatchingException {
    // Given
    var expected = List.of(
        Target.builder()
            .odsId(DOI_PROXY + ID)
            .odsType(AnnotationTargetType.DIGITAL_SPECIMEN)
            .oaSelector(new ClassSelector("digitalSpecimenWrapper.occurrences[0].locality"))
            .build(),
        Target.builder()
            .odsId(DOI_PROXY + ID)
            .odsType(AnnotationTargetType.DIGITAL_SPECIMEN)
            .oaSelector(new ClassSelector("digitalSpecimenWrapper.occurrences[2].locality"))
            .build());

    var baseTargetClassSelector = Target.builder()
        .odsId(DOI_PROXY + ID)
        .odsType(AnnotationTargetType.DIGITAL_SPECIMEN)
        .oaSelector(new ClassSelector("digitalSpecimenWrapper.occurrences[1].locality"))
        .build();

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
    var expected = List.of(Target.builder()
            .odsId(DOI_PROXY + ID)
            .odsType(AnnotationTargetType.DIGITAL_SPECIMEN)
            .oaSelector(new FieldSelector("digitalSpecimenWrapper.occurrences[0].locality"))
            .build(),
        Target.builder()
            .odsId(DOI_PROXY + ID)
            .odsType(AnnotationTargetType.DIGITAL_SPECIMEN)
            .oaSelector(new FieldSelector("digitalSpecimenWrapper.occurrences[2].locality"))
            .build());

    // When
    var result = jsonPathComponent.getAnnotationTargets(givenBatchMetadataLatitudeSearch(),
        givenElasticDocument(),
        baseTargetClassSelector);

    // Then
    assertThat(result).hasSameElementsAs(expected);
  }

  @Test
  void testGetAnnotationTargetsExtended() {
    // Given
    var baseTarget = givenOaTarget(givenSelector("digitalSpecimenWrapper.occurrences[1].location"));
    var expected = List.of(givenOaTarget(givenSelector("digitalSpecimenWrapper.occurrences[0].location")));
    var batchMetadata = new BatchMetadataExtended(1, List.of(
        new BatchMetadataSearchParam(
            "digitalSpecimenWrapper.occurrences[*].location.dwc:country",
            "Netherlands"),
        new BatchMetadataSearchParam(
            "digitalSpecimenWrapper.occurrences[*].dwc:occurrenceRemarks",
            "Correct"
        )
    ));

    // When
    var result = jsonPathComponent.getAnnotationTargetsExtended(batchMetadata,
        givenElasticDocument(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testGetAnnotationTargetsExtendedOneBatchMetadata() {
    // Given
    var baseTarget = givenOaTarget(givenSelector("digitalSpecimenWrapper.occurrences[1].location"));
    var expected = List.of(givenOaTarget(givenSelector("digitalSpecimenWrapper.occurrences[0].location")),
        givenOaTarget(givenSelector("digitalSpecimenWrapper.occurrences[2].location")));
    var batchMetadata = new BatchMetadataExtended(1, List.of(
        new BatchMetadataSearchParam(
            "digitalSpecimenWrapper.occurrences[*].location.dwc:country",
            "Netherlands")));

    // When
    var result = jsonPathComponent.getAnnotationTargetsExtended(batchMetadata,
        givenElasticDocument(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testGetAnnotationTargetsExtendedFalsePositive() {
    // Given
    var baseTarget = givenOaTarget(givenSelector("digitalSpecimenWrapper.occurrences[1].location"));
    var batchMetadata = new BatchMetadataExtended(1, List.of(
        new BatchMetadataSearchParam(
            "digitalSpecimenWrapper.occurrences[*].location.dwc:country",
            "Netherlands"),
        new BatchMetadataSearchParam(
            "digitalSpecimenWrapper.occurrences[*].dwc:occurrenceRemarks",
            "Incorrect"
        )
    ));

    // When
    var result = jsonPathComponent.getAnnotationTargetsExtended(batchMetadata,
        givenElasticDocument(),
        baseTarget);

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  void testGetAnnotationTargetsExtendedOneSharedIndexWithTarget() {
    // Given
    var baseTarget = givenOaTarget(givenSelector("digitalSpecimenWrapper.occurrences[1].location"));
    var expected = List.of(
        givenOaTarget(givenSelector("digitalSpecimenWrapper.occurrences[0].location")),
        givenOaTarget(givenSelector("digitalSpecimenWrapper.occurrences[2].location")));
    var batchMetadata = new BatchMetadataExtended(1, List.of(
        new BatchMetadataSearchParam(
            "digitalSpecimenWrapper.occurrences[*].location.dwc:country",
            "Netherlands"),
        new BatchMetadataSearchParam(
            "digitalSpecimenWrapper.fieldNum",
            "1"
        )));

    // When
    var result = jsonPathComponent.getAnnotationTargetsExtended(batchMetadata,
        givenElasticDocument(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testGetAnnotationTargetsExtendedNestedFields() throws Exception {
    // Given
    var baseTarget = givenOaTarget(givenSelector("digitalSpecimenWrapper.occurrences[5].assertions[5].assertionValue"));
    var expected = List.of(
        givenOaTarget(givenSelector("digitalSpecimenWrapper.occurrences[1].assertions[0].assertionValue")),
        givenOaTarget(givenSelector("digitalSpecimenWrapper.occurrences[1].assertions[1].assertionValue")));
    var batchMetadata = new BatchMetadataExtended(1, List.of(
        new BatchMetadataSearchParam(
            "digitalSpecimenWrapper.occurrences[*].assertions[*].assertionType",
            "weight"),
        new BatchMetadataSearchParam(
            "digitalSpecimenWrapper.occurrences[*].eventDate",
            "2001-01-01"
        )));

    // When
    var result = jsonPathComponent.getAnnotationTargetsExtended(batchMetadata,
        givenNestedNode(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testGetAnnotationTargetsExtendedNestedFieldsFalsePositive() throws Exception {
    // Given
    var baseTarget = givenOaTarget(givenSelector("digitalSpecimenWrapper.occurrences[5].assertions[5].assertionValue"));
    var batchMetadata = new BatchMetadataExtended(1, List.of(
        new BatchMetadataSearchParam(
            "digitalSpecimenWrapper.occurrences[*].assertions[*].assertionType",
            "weight"),
        new BatchMetadataSearchParam(
            "digitalSpecimenWrapper.occurrences[*].assertions[*].assertionValue",
            "10cm"
        )));

    // When
    var result = jsonPathComponent.getAnnotationTargetsExtended(batchMetadata,
        givenNestedNode(),
        baseTarget);

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  void testGetAnnotationTargetsExtendedNestedFieldsBothValid() throws Exception {
    // Given
    var baseTarget = givenOaTarget(givenSelector("digitalSpecimenWrapper.occurrences[5].assertions[5].assertionValue"));
    var expected = List.of(
        givenOaTarget(givenSelector("digitalSpecimenWrapper.occurrences[1].assertions[1].assertionValue")));
    var batchMetadata = new BatchMetadataExtended(1, List.of(
        new BatchMetadataSearchParam(
            "digitalSpecimenWrapper.occurrences[*].assertions[*].assertionType",
            "weight"),
        new BatchMetadataSearchParam(
            "digitalSpecimenWrapper.occurrences[*].assertions[*].assertionValue",
            "10.1kilos"
        )));

    // When
    var result = jsonPathComponent.getAnnotationTargetsExtended(batchMetadata,
        givenNestedNode(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testBadSelectorType() {
    // Given
    var baseTargetClassSelector = Target.builder()
        .odsId(ID)
        .odsType(AnnotationTargetType.DIGITAL_SPECIMEN)
        .oaSelector(new FragmentSelector())
        .build();
    var search = givenBatchMetadataLatitudeSearch();
    var doc = givenElasticDocument();

    // When
    assertThrows(BatchingRuntimeException.class, () ->
        jsonPathComponent.getAnnotationTargets(search,
            doc,
            baseTargetClassSelector));
  }

  @Test
  void testBadJsonpathFormat() {
    // Given
    var batchMetadata = new BatchMetadata(1,
        "[digitalSpecimenWrapper][occurrences][*]..[location][georeference]['dwc:decimalLatitude']['dwc:value']",
        "11");
    var baseTargetClassSelector = Target.builder()
        .odsId(ID)
        .odsType(AnnotationTargetType.DIGITAL_SPECIMEN)
        .oaSelector(new FieldSelector()
            .withOdsField("digitalSpecimenWrapper.occurrences[1].locality"))
        .build();

    // When
    assertThrows(BatchingException.class, () ->
        jsonPathComponent.getAnnotationTargets(batchMetadata,
            givenElasticDocument(),
            baseTargetClassSelector));
  }

  private static JsonNode givenNestedNode() throws Exception {
    return MAPPER.readTree("""
        {
          "id": "20.5000.1025/KZL-VC0-ZK2",
          "digitalSpecimenWrapper": {
            "occurrences": [
              {
                "assertions": [
                  {
                    "assertionType": "length",
                    "assertionValue": "10cm"
                  }
                ],
                "eventDate" :"2001-01-01"
              },
              {
                "assertions": [
                 {
                    "assertionType": "weight",
                    "assertionValue": "10kilos"
                  },
                  {
                    "assertionType": "weight",
                    "assertionValue": "10.1kilos"
                  }
                ],
                "eventDate" :"2001-01-01"
              }
            ]
          }
        }
        """);
  }

  private static JsonNode givenJsonNode() throws Exception {
    return MAPPER.readTree("""
        {
          "specimen": {
            "livingOrPreserved": "preserved",
            "institution": "MNH",
            "occurrences": [
              {
                "location": {
                  "city": "Paris"
                },
                "remarks": {
                  "weather": "good"
                }
              },
              {
                "location": {
                  "city": "Marseille"
                },
                "remarks": {
                  "weather": "bad"
                }
              }
            ],
            "identifications": [
              {
                "citation": "Miller 1888",
                "taxonIdentification": [
                  {
                    "scientificName": "bombus bombus",
                    "class": "insecta"
                  },
                  {
                    "scientificName": "Apis mellifera",
                    "class": "insecta"
                  }
                ]
              },
              {
                "citation": "Frank 1900",
                "taxonIdentification": [
                  {
                    "scientificName": "bombus bombus"
                  },
                  {
                    "scientificName": "Apis mellifera"
                  }
                ]
              }
            ]
          }
        }
        """);
  }

}
