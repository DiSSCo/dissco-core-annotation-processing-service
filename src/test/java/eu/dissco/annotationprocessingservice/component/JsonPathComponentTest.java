package eu.dissco.annotationprocessingservice.component;

import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.givenBatchMetadataExtendedOneParam;
import static eu.dissco.annotationprocessingservice.TestUtils.givenBatchMetadataExtendedTwoParam;
import static eu.dissco.annotationprocessingservice.TestUtils.givenBatchMetadataSearchParamCountry;
import static eu.dissco.annotationprocessingservice.TestUtils.givenElasticDocument;
import static eu.dissco.annotationprocessingservice.TestUtils.givenOaTarget;
import static eu.dissco.annotationprocessingservice.TestUtils.givenSelector;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.Option;
import eu.dissco.annotationprocessingservice.domain.BatchMetadataExtended;
import eu.dissco.annotationprocessingservice.domain.BatchMetadataSearchParam;
import eu.dissco.annotationprocessingservice.domain.annotation.ClassSelector;
import eu.dissco.annotationprocessingservice.domain.annotation.FragmentSelector;
import eu.dissco.annotationprocessingservice.exception.BatchingRuntimeException;
import java.util.List;
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
  void testGetAnnotationTargetsExtended() {
    // Given
    var baseTarget = givenOaTarget(givenSelector("digitalSpecimenWrapper.occurrences[1].location"));
    var expected = List.of(
        givenOaTarget(givenSelector("digitalSpecimenWrapper.occurrences[0].location")));

    // When
    var result = jsonPathComponent.getAnnotationTargets(
        givenBatchMetadataExtendedTwoParam(),
        givenElasticDocument(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testGetAnnotationTargetsExtendedOneBatchMetadata() {
    // Given
    var baseTarget = givenOaTarget(givenSelector("digitalSpecimenWrapper.occurrences[1].location"));
    var expected = List.of(
        givenOaTarget(givenSelector("digitalSpecimenWrapper.occurrences[0].location")),
        givenOaTarget(givenSelector("digitalSpecimenWrapper.occurrences[2].location")));

    // When
    var result = jsonPathComponent.getAnnotationTargets(
        givenBatchMetadataExtendedOneParam(),
        givenElasticDocument(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testGetAnnotationTargetsClassSelector() {
    // Given
    var baseTarget = givenOaTarget(new ClassSelector("digitalSpecimenWrapper.occurrences[0]"));
    var expected = List.of(
        givenOaTarget(new ClassSelector("digitalSpecimenWrapper.occurrences[0]")),
        givenOaTarget(new ClassSelector("digitalSpecimenWrapper.occurrences[2]")));

    // When
    var result = jsonPathComponent.getAnnotationTargets(
        givenBatchMetadataExtendedOneParam(),
        givenElasticDocument(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testWrongSelectorType() {
    var baseTarget = givenOaTarget(new FragmentSelector());
    var doc = givenElasticDocument();
    var batchMetadata = givenBatchMetadataExtendedOneParam();

    assertThrows(BatchingRuntimeException.class,
        () -> jsonPathComponent.getAnnotationTargets(batchMetadata,
            doc,
            baseTarget));
  }

  @Test
  void testBadTargetPath() {
    var baseTarget = givenOaTarget(
        givenSelector("[digitalSpecimenWrapper][occurrences][1][location]"));
    var batchMetadata = new BatchMetadataExtended(1, List.of(
        new BatchMetadataSearchParam(
            "[digitalSpecimenWrapper][occurrences][*][location][dwc:country]",
            "Netherlands")));
    var doc = givenElasticDocument();

    assertThrows(BatchingRuntimeException.class,
        () -> jsonPathComponent.getAnnotationTargets(batchMetadata,
            doc,
            baseTarget));
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
    var result = jsonPathComponent.getAnnotationTargets(batchMetadata,
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
        givenBatchMetadataSearchParamCountry(),
        new BatchMetadataSearchParam(
            "digitalSpecimenWrapper.fieldNum",
            "1"
        )));

    // When
    var result = jsonPathComponent.getAnnotationTargets(batchMetadata,
        givenElasticDocument(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testGetAnnotationTargetsExtendedNestedFields() throws Exception {
    // Given
    var baseTarget = givenOaTarget(
        givenSelector("digitalSpecimenWrapper.occurrences[5].assertions[5].assertionValue"));
    var expected = List.of(
        givenOaTarget(
            givenSelector("digitalSpecimenWrapper.occurrences[1].assertions[0].assertionValue")),
        givenOaTarget(
            givenSelector("digitalSpecimenWrapper.occurrences[1].assertions[1].assertionValue")));
    var batchMetadata = new BatchMetadataExtended(1, List.of(
        new BatchMetadataSearchParam(
            "digitalSpecimenWrapper.occurrences[*].assertions[*].assertionType",
            "weight"),
        new BatchMetadataSearchParam(
            "digitalSpecimenWrapper.occurrences[*].eventDate",
            "2001-01-01"
        )));

    // When
    var result = jsonPathComponent.getAnnotationTargets(batchMetadata,
        givenNestedNode(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testGetAnnotationTargetsExtendedNestedFieldsFalsePositive() throws Exception {
    // Given
    var baseTarget = givenOaTarget(
        givenSelector("digitalSpecimenWrapper.occurrences[5].assertions[5].assertionValue"));
    var batchMetadata = new BatchMetadataExtended(1, List.of(
        new BatchMetadataSearchParam(
            "digitalSpecimenWrapper.occurrences[*].assertions[*].assertionType",
            "weight"),
        new BatchMetadataSearchParam(
            "digitalSpecimenWrapper.occurrences[*].assertions[*].assertionValue",
            "10cm"
        )));

    // When
    var result = jsonPathComponent.getAnnotationTargets(batchMetadata,
        givenNestedNode(),
        baseTarget);

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  void testGetAnnotationTargetsExtendedNestedFieldsBothValid() throws Exception {
    // Given
    var baseTarget = givenOaTarget(
        givenSelector("digitalSpecimenWrapper.occurrences[5].assertions[5].assertionValue"));
    var expected = List.of(
        givenOaTarget(
            givenSelector("digitalSpecimenWrapper.occurrences[1].assertions[1].assertionValue")));
    var batchMetadata = new BatchMetadataExtended(1, List.of(
        new BatchMetadataSearchParam(
            "digitalSpecimenWrapper.occurrences[*].assertions[*].assertionType",
            "weight"),
        new BatchMetadataSearchParam(
            "digitalSpecimenWrapper.occurrences[*].assertions[*].assertionValue",
            "10.1kilos"
        )));

    // When
    var result = jsonPathComponent.getAnnotationTargets(batchMetadata,
        givenNestedNode(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }


  // Case 0 -> False Positive
  @Test
  void testCase0() throws Exception {
    // Given
    var batchMetadata = new BatchMetadataExtended(0, List.of(
        new BatchMetadataSearchParam("digitalSpecimenWrapper.occurrences[*].location.city",
            "Paris"),
        new BatchMetadataSearchParam("digitalSpecimenWrapper.occurrences[*].remarks.weather",
            "bad"))
    );

    // When
    var result = jsonPathComponent.getAnnotationTargets(batchMetadata, givenCaseNode(),
        givenOaTarget(ID));

    // Then
    assertThat(result).isEmpty();
  }

  // Case 1a -> No Array In Target, 1 Input
  @Test
  void testCase1a() throws Exception {
    // Given
    var baseTarget = givenOaTarget(
        givenSelector("digitalSpecimenWrapper.livingOrPreserved"));
    var expected = List.of(baseTarget);
    var batchMetadata = new BatchMetadataExtended(0, List.of(
        new BatchMetadataSearchParam("digitalSpecimenWrapper.occurrences[*].location.city",
            "Paris"))
    );

    // When
    var result = jsonPathComponent.getAnnotationTargets(batchMetadata, givenCaseNode(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  // Case 1b -> No Array In Target, 2 Inputs
  @Test
  void testCase1b() throws Exception {
    // Given
    var baseTarget = givenOaTarget(
        givenSelector("digitalSpecimenWrapper.livingOrPreserved"));
    var expected = List.of(baseTarget);
    var batchMetadata = new BatchMetadataExtended(0, List.of(
        new BatchMetadataSearchParam("digitalSpecimenWrapper.occurrences[*].location.city",
            "Paris"),
        new BatchMetadataSearchParam(
            "digitalSpecimenWrapper.occurrences[*].location.remarks.weather", "good"))
    );

    // When
    var result = jsonPathComponent.getAnnotationTargets(batchMetadata, givenCaseNode(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  // Case 2a -> Array In target, no array in input
  @Test
  void testCase2a() throws Exception {
    // Given
    var baseTarget = givenOaTarget(
        givenSelector("digitalSpecimenWrapper.occurrences[5].location"));
    var expected = List.of(givenOaTarget(
            givenSelector("digitalSpecimenWrapper.occurrences[0].location")),
        givenOaTarget(
            givenSelector("digitalSpecimenWrapper.occurrences[1].location")));
    var batchMetadata = new BatchMetadataExtended(0, List.of(
        new BatchMetadataSearchParam("digitalSpecimenWrapper.livingOrPreserved", "preserved"))
    );

    // When
    var result = jsonPathComponent.getAnnotationTargets(batchMetadata, givenCaseNode(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  // Case 2a -> Array In target, different array in input
  @Test
  void testCase2b() throws Exception {
    // Given
    var baseTarget = givenOaTarget(
        givenSelector("digitalSpecimenWrapper.occurrences[5].location"));
    var expected = List.of(givenOaTarget(
            givenSelector("digitalSpecimenWrapper.occurrences[0].location")),
        givenOaTarget(
            givenSelector("digitalSpecimenWrapper.occurrences[1].location")));
    var batchMetadata = new BatchMetadataExtended(0, List.of(
        new BatchMetadataSearchParam("digitalSpecimenWrapper.identifications[*].citation",
            "Miller 1888"))
    );

    // When
    var result = jsonPathComponent.getAnnotationTargets(batchMetadata, givenCaseNode(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }


  // Case 2a -> Array In target, different array in inputs
  @Test
  void testCase2c() throws Exception {
    // Given
    var baseTarget = givenOaTarget(
        givenSelector("digitalSpecimenWrapper.occurrences[5].location"));
    var expected = List.of(givenOaTarget(
            givenSelector("digitalSpecimenWrapper.occurrences[0].location")),
        givenOaTarget(
            givenSelector("digitalSpecimenWrapper.occurrences[1].location")));
    var batchMetadata = new BatchMetadataExtended(0, List.of(
        new BatchMetadataSearchParam("digitalSpecimenWrapper.identifications[*].citation",
            "Miller 1888"),
        new BatchMetadataSearchParam(
            "digitalSpecimenWrapper.identifications[*].taxonIdentification[*].scientificName",
            "bombus bombus"))
    );

    // When
    var result = jsonPathComponent.getAnnotationTargets(batchMetadata, givenCaseNode(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  // Case 3a -> Array In target, same array in 1 input
  @Test
  void testCase3a() throws Exception {
    // Given
    var baseTarget = givenOaTarget(
        givenSelector("digitalSpecimenWrapper.occurrences[5].location"));
    var expected = List.of(givenOaTarget(
        givenSelector("digitalSpecimenWrapper.occurrences[0].location")));
    var batchMetadata = new BatchMetadataExtended(0, List.of(
        new BatchMetadataSearchParam("digitalSpecimenWrapper.occurrences[*].location.city",
            "Paris"))
    );

    // When
    var result = jsonPathComponent.getAnnotationTargets(batchMetadata, givenCaseNode(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  // Case 3b -> Array In target, same array in 2/2 inputs
  @Test
  void testCase3b() throws Exception {
    // Given
    var baseTarget = givenOaTarget(
        givenSelector("digitalSpecimenWrapper.occurrences[5].location"));
    var expected = List.of(givenOaTarget(
        givenSelector("digitalSpecimenWrapper.occurrences[0].location")));
    var batchMetadata = new BatchMetadataExtended(0, List.of(
        new BatchMetadataSearchParam("digitalSpecimenWrapper.occurrences[*].location.city",
            "Paris"),
        new BatchMetadataSearchParam("digitalSpecimenWrapper.occurrences[*].remarks.weather",
            "good"))
    );

    // When
    var result = jsonPathComponent.getAnnotationTargets(batchMetadata, givenCaseNode(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  // Case 4 -> Array in target, Input array nested deeper
  @Test
  void testCase4() throws Exception {
    // Given
    var baseTarget = givenOaTarget(
        givenSelector("digitalSpecimenWrapper.identifications[5].citation"));
    var expected = List.of(givenOaTarget(
        givenSelector("digitalSpecimenWrapper.identifications[0].citation")));
    var batchMetadata = new BatchMetadataExtended(0, List.of(
        new BatchMetadataSearchParam(
            "digitalSpecimenWrapper.identifications[*].taxonIdentification.scientificName",
            "bombus bombus"),
        new BatchMetadataSearchParam(
            "digitalSpecimenWrapper.identifications[*].taxonIdentification.class", "insecta"))
    );

    // When
    var result = jsonPathComponent.getAnnotationTargets(batchMetadata, givenCaseNode(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  // Case 5 -> Array In target, same array in 1/2 inputs
  @Test
  void testCase5() throws Exception {
    // Given
    var baseTarget = givenOaTarget(
        givenSelector("digitalSpecimenWrapper.occurrences[5].location"));
    var expected = List.of(givenOaTarget(
        givenSelector("digitalSpecimenWrapper.occurrences[0].location")));
    var batchMetadata = new BatchMetadataExtended(0, List.of(
        new BatchMetadataSearchParam("digitalSpecimenWrapper.identifications[*].citation",
            "Miller 1888"),
        new BatchMetadataSearchParam("digitalSpecimenWrapper.occurrences[*].remarks.weather",
            "good"))
    );

    // When
    var result = jsonPathComponent.getAnnotationTargets(batchMetadata, givenCaseNode(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  // Case 6a -> Nested Array in target, 1 input with no arrays
  @Test
  void testCase6a() throws Exception {
    // Given
    var baseTarget = givenOaTarget(
        givenSelector(
            "digitalSpecimenWrapper.identifications[5].taxonIdentification[5].scientificName"));
    var expected = List.of(givenOaTarget(
            givenSelector(
                "digitalSpecimenWrapper.identifications[0].taxonIdentification[0].scientificName")),
        givenOaTarget(
            givenSelector(
                "digitalSpecimenWrapper.identifications[0].taxonIdentification[1].scientificName")),
        givenOaTarget(
            givenSelector(
                "digitalSpecimenWrapper.identifications[1].taxonIdentification[0].scientificName")),
        givenOaTarget(
            givenSelector(
                "digitalSpecimenWrapper.identifications[1].taxonIdentification[1].scientificName"))
    );
    var batchMetadata = new BatchMetadataExtended(0, List.of(
        new BatchMetadataSearchParam("digitalSpecimenWrapper.livingOrPreserved", "preserved"))
    );

    // When
    var result = jsonPathComponent.getAnnotationTargets(batchMetadata, givenCaseNode(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  // Case 6b -> Nested Array in target, 1 input with arrays

  @Test
  void testCase6b() throws Exception {
    // Given
    var baseTarget = givenOaTarget(
        givenSelector(
            "digitalSpecimenWrapper.identifications[5].taxonIdentification[5].scientificName"));
    var expected = List.of(givenOaTarget(
            givenSelector(
                "digitalSpecimenWrapper.identifications[0].taxonIdentification[0].scientificName")),
        givenOaTarget(
            givenSelector(
                "digitalSpecimenWrapper.identifications[1].taxonIdentification[0].scientificName"))
    );
    var batchMetadata = new BatchMetadataExtended(0, List.of(
        new BatchMetadataSearchParam(
            "digitalSpecimenWrapper.identifications[*].taxonIdentification[*].scientificName",
            "bombus bombus"))
    );

    // When
    var result = jsonPathComponent.getAnnotationTargets(batchMetadata, givenCaseNode(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  // Case 6c -> Nested Array in target, 2 inputs with arrays

  @Test
  void testCase6c() throws Exception {
    // Given
    var baseTarget = givenOaTarget(
        givenSelector(
            "digitalSpecimenWrapper.identifications[5].taxonIdentification[5].scientificName"));
    var expected = List.of(givenOaTarget(
        givenSelector(
            "digitalSpecimenWrapper.identifications[0].taxonIdentification[0].scientificName"))
    );
    var batchMetadata = new BatchMetadataExtended(0, List.of(
        new BatchMetadataSearchParam(
            "digitalSpecimenWrapper.identifications[*].taxonIdentification[*].scientificName",
            "bombus bombus"),
        new BatchMetadataSearchParam(
            "digitalSpecimenWrapper.identifications[*].taxonIdentification[*].class", "insecta"))
    );

    // When
    var result = jsonPathComponent.getAnnotationTargets(batchMetadata, givenCaseNode(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  // Case 7 -> Nested Array in target, partially ambiguous input
  @Test
  void testCase7() throws Exception {
    // Given
    var baseTarget = givenOaTarget(
        givenSelector(
            "digitalSpecimenWrapper.identifications[5].taxonIdentification[5].scientificName"));
    var expected = List.of(givenOaTarget(
            givenSelector(
                "digitalSpecimenWrapper.identifications[0].taxonIdentification[0].scientificName")),
        givenOaTarget(
            givenSelector(
                "digitalSpecimenWrapper.identifications[0].taxonIdentification[1].scientificName"))
    );
    var batchMetadata = new BatchMetadataExtended(0, List.of(
        new BatchMetadataSearchParam("digitalSpecimenWrapper.identifications[*].citation",
            "Miller 1888"))
    );

    // When
    var result = jsonPathComponent.getAnnotationTargets(batchMetadata, givenCaseNode(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }


  // Given Node
  private JsonNode givenCaseNode() throws Exception {
    return MAPPER.readTree("""
        {
         "id": "20.5000.1025/KZL-VC0-ZK2",
         "digitalSpecimenWrapper": {
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
}
