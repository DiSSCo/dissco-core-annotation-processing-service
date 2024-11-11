package eu.dissco.annotationprocessingservice.component;

import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationBatchMetadataOneParam;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationBatchMetadataTwoParam;
import static eu.dissco.annotationprocessingservice.TestUtils.givenElasticDocument;
import static eu.dissco.annotationprocessingservice.TestUtils.givenOaTarget;
import static eu.dissco.annotationprocessingservice.TestUtils.givenSearchParamCountry;
import static eu.dissco.annotationprocessingservice.TestUtils.givenSelector;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.Option;
import eu.dissco.annotationprocessingservice.exception.BatchingException;
import eu.dissco.annotationprocessingservice.exception.BatchingRuntimeException;
import eu.dissco.annotationprocessingservice.schema.AnnotationBatchMetadata;
import eu.dissco.annotationprocessingservice.schema.OaHasSelector;
import eu.dissco.annotationprocessingservice.schema.SearchParam;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
class JsonPathComponentTest {

  Configuration jsonPathConfiguration = Configuration.builder()
      .options(Option.AS_PATH_LIST, Option.SUPPRESS_EXCEPTIONS, Option.ALWAYS_RETURN_LIST)
      .build();
  private JsonPathComponent jsonPathComponent;

  private static JsonNode givenNestedNode() throws Exception {
    return MAPPER.readTree("""
        {
          "@id": "https://doi.org/20.5000.1025/KZL-VC0-ZK2",
            "ods:hasEvents": [
              {
                "ods:hasAssertions": [
                  {
                    "dwc:measurementType": "length",
                    "dwc:measurementValue": "10cm"
                  }
                ],
                "dwc:eventDate" :"2001-01-01"
              },
              {
                "ods:hasAssertions": [
                 {
                    "dwc:measurementType": "weight",
                    "dwc:measurementValue": "10kilos"
                  },
                  {
                    "dwc:measurementType": "weight",
                    "dwc:measurementValue": "10.1kilos"
                  }
                ],
                "dwc:eventDate" :"2001-01-01"
              }
            ]
        }
        """);
  }

  @BeforeEach
  void init() {
    jsonPathComponent = new JsonPathComponent(MAPPER, jsonPathConfiguration);
  }

  @Test
  void testGetAnnotationTargetsExtended() throws Exception {
    // Given
    var baseTarget = givenOaTarget(givenSelector("$['ods:hasEvents'][0]['ods:hasLocation']"));
    var expected = List.of(
        givenOaTarget(givenSelector("$['ods:hasEvents'][0]['ods:hasLocation']")));

    // When
    var result = jsonPathComponent.getAnnotationTargets(
        givenAnnotationBatchMetadataTwoParam(),
        givenElasticDocument(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testGetAnnotationTargetsExtendedOneBatchMetadata() throws Exception {
    // Given
    var baseTarget = givenOaTarget(givenSelector("$['ods:hasEvents'][1]['ods:hasLocation']"));
    var expected = List.of(
        givenOaTarget(givenSelector("$['ods:hasEvents'][0]['ods:hasLocation']")),
        givenOaTarget(givenSelector("$['ods:hasEvents'][2]['ods:hasLocation']")));

    // When
    var result = jsonPathComponent.getAnnotationTargets(
        givenAnnotationBatchMetadataOneParam(),
        givenElasticDocument(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testGetAnnotationTargetsClassSelector() throws Exception {
    // Given
    var classSelector = new OaHasSelector().withAdditionalProperty("ods:class",
            "$['ods:hasEvents'][0]")
        .withAdditionalProperty("@type", "ods:ClassSelector");
    var baseTarget = givenOaTarget(classSelector);
    var expected = List.of(
        givenOaTarget(classSelector),
        givenOaTarget(new OaHasSelector().withAdditionalProperty("ods:class", "$['ods:hasEvents'][2]")
            .withAdditionalProperty("@type", "ods:ClassSelector")));

    // When
    var result = jsonPathComponent.getAnnotationTargets(
        givenAnnotationBatchMetadataOneParam(),
        givenElasticDocument(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testWrongSelectorType() {
    var baseTarget = givenOaTarget(
        new OaHasSelector().withAdditionalProperty("@type", "oa:FragmentSelector"));
    var doc = givenElasticDocument();
    var batchMetadata = givenAnnotationBatchMetadataOneParam();

    assertThrows(BatchingRuntimeException.class,
        () -> jsonPathComponent.getAnnotationTargets(batchMetadata,
            doc,
            baseTarget));
  }

  @Test
  void testBadTargetPath() {
    var baseTarget = givenOaTarget(
        givenSelector("$[['ods:hasEvents'][1]['ods:hasLocation']"));
    var batchMetadata = new AnnotationBatchMetadata(1, List.of(
        new SearchParam(
            "$['ods:hasEvents'][*]['ods:hasLocation']['dwc:country']",
            "Netherlands")));
    var doc = givenElasticDocument();

    assertThrows(BatchingException.class,
        () -> jsonPathComponent.getAnnotationTargets(batchMetadata,
            doc,
            baseTarget));
  }

  @Test
  void testGetAnnotationTargetsExtendedFalsePositive() throws Exception {
    // Given
    var baseTarget = givenOaTarget(givenSelector("$['ods:hasEvents'][1]['ods:hasLocation']"));
    var batchMetadata = new AnnotationBatchMetadata(1, List.of(
        new SearchParam(
            "$['ods:hasEvents'][*]['ods:hasLocation']['dwc:country']",
            "Netherlands"),
        new SearchParam(
            "$['ods:hasEvents'][*]['dwc:eventRemarks']",
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
  void testGetAnnotationTargetsExtendedOneSharedIndexWithTarget() throws Exception {
    // Given
    var baseTarget = givenOaTarget(givenSelector("$['ods:hasEvents'][1]['ods:hasLocation']"));
    var expected = List.of(
        givenOaTarget(givenSelector("$['ods:hasEvents'][0]['ods:hasLocation']")),
        givenOaTarget(givenSelector("$['ods:hasEvents'][2]['ods:hasLocation']")));
    var batchMetadata = new AnnotationBatchMetadata(1, List.of(
        givenSearchParamCountry()));

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
        givenSelector("$['ods:hasEvents'][5]['ods:hasAssertions'][5]['dwc:measurementValue']"));
    var expected = List.of(
        givenOaTarget(
            givenSelector("$['ods:hasEvents'][1]['ods:hasAssertions'][0]['dwc:measurementValue']")),
        givenOaTarget(
            givenSelector("$['ods:hasEvents'][1]['ods:hasAssertions'][1]['dwc:measurementValue']")));
    var batchMetadata = new AnnotationBatchMetadata(1, List.of(
        new SearchParam(
            "$['ods:hasEvents'][*]['ods:hasAssertions'][*]['dwc:measurementType']",
            "weight"),
        new SearchParam(
            "$['ods:hasEvents'][*]['dwc:eventDate']",
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
        givenSelector("$['ods:hasEvents'][5]['ods:hasAssertions'][5]['dwc:measurementValue']"));
    var batchMetadata = new AnnotationBatchMetadata(1, List.of(
        new SearchParam(
            "$['ods:hasEvents'][*]['ods:hasAssertions'][*]['dwc:measurementType']",
            "weight"),
        new SearchParam(
            "$['ods:hasEvents'][*]['ods:hasAssertions'][*]['dwc:measurementValue']",
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
        givenSelector("$['ods:hasEvents'][5]['ods:hasAssertions'][5]['dwc:measurementValue']"));
    var expected = List.of(
        givenOaTarget(
            givenSelector("$['ods:hasEvents'][1]['ods:hasAssertions'][1]['dwc:measurementValue']")));
    var batchMetadata = new AnnotationBatchMetadata(1, List.of(
        new SearchParam(
            "$['ods:hasEvents'][*]['ods:hasAssertions'][*]['dwc:measurementType']",
            "weight"),
        new SearchParam(
            "$['ods:hasEvents'][*]['ods:hasAssertions'][*]['dwc:measurementValue']",
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
    var batchMetadata = new AnnotationBatchMetadata(0, List.of(
        new SearchParam("$['ods:hasEvents'][*]['ods:hasLocation']['dwc:city']",
            "Paris"),
        new SearchParam("$['ods:hasEvents'][*]['remarks']['weather']",
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
        givenSelector("$['ods:livingOrPreserved']"));
    var expected = List.of(baseTarget);
    var batchMetadata = new AnnotationBatchMetadata(0, List.of(
        new SearchParam("$['ods:hasEvents'][*]['ods:hasLocation']['dwc:city']",
            "Paris"))
    );

    // When
    var result = jsonPathComponent.getAnnotationTargets(batchMetadata, givenCaseNode(), baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  // Case 1b -> No Array In Target, 2 Inputs
  @Test
  void testCase1b() throws Exception {
    // Given
    var baseTarget = givenOaTarget(
        givenSelector("$['ods:livingOrPreserved']"));
    var expected = List.of(baseTarget);
    var batchMetadata = new AnnotationBatchMetadata(0, List.of(
        new SearchParam("$['ods:hasEvents'][*]['ods:hasLocation']['dwc:city']",
            "Paris"),
        new SearchParam("$['ods:hasEvents'][*]['remarks']['weather']", "good"))
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
        givenSelector("$['ods:hasEvents'][5]['ods:hasLocation']"));
    var expected = List.of(givenOaTarget(
            givenSelector("$['ods:hasEvents'][0]['ods:hasLocation']")),
        givenOaTarget(
            givenSelector("$['ods:hasEvents'][1]['ods:hasLocation']")));
    var batchMetadata = new AnnotationBatchMetadata(0, List.of(
        new SearchParam("$['ods:livingOrPreserved']", "Preserved"))
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
        givenSelector("$['ods:hasEvents'][5]['ods:hasLocation']"));
    var expected = List.of(givenOaTarget(
            givenSelector("$['ods:hasEvents'][0]['ods:hasLocation']")),
        givenOaTarget(
            givenSelector("$['ods:hasEvents'][1]['ods:hasLocation']")));
    var batchMetadata = new AnnotationBatchMetadata(0, List.of(
        new SearchParam(
            "$['ods:hasIdentifications'][*]['ods:hasCitations'][*]['dcterms:identifier']",
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
        givenSelector("$['ods:hasEvents'][5]['ods:hasLocation']"));
    var expected = List.of(givenOaTarget(
            givenSelector("$['ods:hasEvents'][0]['ods:hasLocation']")),
        givenOaTarget(
            givenSelector("$['ods:hasEvents'][1]['ods:hasLocation']")));
    var batchMetadata = new AnnotationBatchMetadata(0, List.of(
        new SearchParam(
            "$['ods:hasIdentifications'][*]['ods:hasCitations'][*]['dcterms:identifier']",
            "Miller 1888"),
        new SearchParam(
            "$['ods:hasIdentifications'][*]['ods:hasTaxonIdentifications'][*]['dwc:scientificName']",
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
        givenSelector("$['ods:hasEvents'][5]['ods:hasLocation']"));
    var expected = List.of(givenOaTarget(
        givenSelector("$['ods:hasEvents'][0]['ods:hasLocation']")));
    var batchMetadata = new AnnotationBatchMetadata(0, List.of(
        new SearchParam("$['ods:hasEvents'][*]['ods:hasLocation']['dwc:city']",
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
        givenSelector("$['ods:hasEvents'][5]['ods:hasLocation']"));
    var expected = List.of(givenOaTarget(
        givenSelector("$['ods:hasEvents'][0]['ods:hasLocation']")));
    var batchMetadata = new AnnotationBatchMetadata(0, List.of(
        new SearchParam("$['ods:hasEvents'][*]['ods:hasLocation']['dwc:city']",
            "Paris"),
        new SearchParam("$['ods:hasEvents'][*]['remarks']['weather']",
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
        givenSelector("$['ods:hasIdentifications'][5]['ods:hasCitations'][0]['dcterms:identifier']"));
    var expected = List.of(givenOaTarget(
        givenSelector("$['ods:hasIdentifications'][0]['ods:hasCitations'][0]['dcterms:identifier']")));
    var batchMetadata = new AnnotationBatchMetadata(0, List.of(
        new SearchParam(
            "$['ods:hasIdentifications'][*]['ods:hasTaxonIdentifications'][*]['dwc:scientificName']",
            "bombus bombus"),
        new SearchParam(
            "$['ods:hasIdentifications'][*]['ods:hasTaxonIdentifications'][*]['dwc:class']", "insecta"))
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
        givenSelector("$['ods:hasEvents'][5]['ods:hasLocation']"));
    var expected = List.of(givenOaTarget(
        givenSelector("$['ods:hasEvents'][0]['ods:hasLocation']")));
    var batchMetadata = new AnnotationBatchMetadata(0, List.of(
        new SearchParam(
            "$['ods:hasIdentifications'][*]['ods:hasCitations'][*]['dcterms:identifier']",
            "Miller 1888"),
        new SearchParam("$['ods:hasEvents'][*]['ods:hasLocation']['remarks']['weather']",
            "good"))
    );

    // When
    var result = jsonPathComponent.getAnnotationTargets(batchMetadata, givenCaseNode(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  // Case 6b -> Nested Array in target, 1 input with arrays

  // Case 6a -> Nested Array in target, 1 input with no arrays
  @Test
  void testCase6a() throws Exception {
    // Given
    var baseTarget = givenOaTarget(
        givenSelector(
            "$['ods:hasIdentifications'][5]['ods:hasTaxonIdentifications'][5]['dwc:scientificName']"));
    var expected = List.of(givenOaTarget(
            givenSelector(
                "$['ods:hasIdentifications'][0]['ods:hasTaxonIdentifications'][0]['dwc:scientificName']")),
        givenOaTarget(
            givenSelector(
                "$['ods:hasIdentifications'][0]['ods:hasTaxonIdentifications'][1]['dwc:scientificName']")),
        givenOaTarget(
            givenSelector(
                "$['ods:hasIdentifications'][1]['ods:hasTaxonIdentifications'][0]['dwc:scientificName']")),
        givenOaTarget(
            givenSelector(
                "$['ods:hasIdentifications'][1]['ods:hasTaxonIdentifications'][1]['dwc:scientificName']"))
    );
    var batchMetadata = new AnnotationBatchMetadata(0, List.of(
        new SearchParam("$['ods:livingOrPreserved']", "Preserved"))
    );

    // When
    var result = jsonPathComponent.getAnnotationTargets(batchMetadata, givenCaseNode(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  // Case 6c -> Nested Array in target, 2 inputs with arrays

  @Test
  void testCase6b() throws Exception {
    // Given
    var baseTarget = givenOaTarget(
        givenSelector(
            "$['ods:hasIdentifications'][5]['ods:hasTaxonIdentifications'][5]['dwc:scientificName']"));
    var expected = List.of(givenOaTarget(
            givenSelector(
                "$['ods:hasIdentifications'][0]['ods:hasTaxonIdentifications'][0]['dwc:scientificName']")),
        givenOaTarget(
            givenSelector(
                "$['ods:hasIdentifications'][1]['ods:hasTaxonIdentifications'][0]['dwc:scientificName']"))
    );
    var batchMetadata = new AnnotationBatchMetadata(0, List.of(
        new SearchParam(
            "$['ods:hasIdentifications'][*]['ods:hasTaxonIdentifications'][*]['dwc:scientificName']",
            "bombus bombus"))
    );

    // When
    var result = jsonPathComponent.getAnnotationTargets(batchMetadata, givenCaseNode(),
        baseTarget);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testCase6c() throws Exception {
    // Given
    var baseTarget = givenOaTarget(
        givenSelector(
            "$['ods:hasIdentifications'][5]['ods:hasTaxonIdentifications'][5]['dwc:scientificName']"));
    var expected = List.of(givenOaTarget(
        givenSelector(
            "$['ods:hasIdentifications'][0]['ods:hasTaxonIdentifications'][0]['dwc:scientificName']"))
    );
    var batchMetadata = new AnnotationBatchMetadata(0, List.of(
        new SearchParam(
            "$['ods:hasIdentifications'][*]['ods:hasTaxonIdentifications'][*]['dwc:scientificName']",
            "bombus bombus"),
        new SearchParam(
            "$['ods:hasIdentifications'][*]['ods:hasTaxonIdentifications'][*]['dwc:class']", "insecta"))
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
            "$['ods:hasIdentifications'][5]['ods:hasTaxonIdentifications'][5]['dwc:scientificName']"));
    var expected = List.of(givenOaTarget(
            givenSelector(
                "$['ods:hasIdentifications'][0]['ods:hasTaxonIdentifications'][0]['dwc:scientificName']")),
        givenOaTarget(
            givenSelector(
                "$['ods:hasIdentifications'][0]['ods:hasTaxonIdentifications'][1]['dwc:scientificName']"))
    );
    var batchMetadata = new AnnotationBatchMetadata(0, List.of(
        new SearchParam(
            "$['ods:hasIdentifications'][*]['ods:hasCitations'][*]['dcterms:identifier']",
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
          "@id": "https://doi.org/20.5000.1025/KZL-VC0-ZK2",
          "@type": "ods:DigitalSpecimen",
          "dcterms:identifier": "https://doi.org/20.5000.1025/KZL-VC0-ZK2",
          "ods:fdoType": "https://doi.org/21.T11148/894b1e6cad57e921764e",
          "ods:midsLevel": 0,
          "ods:version": 4,
          "dcterms:created": "2022-11-01T09:59:24.000Z",
          "ods:physicalSpecimenID": "123",
          "ods:physicalSpecimenIDType": "Resolvable",
          "ods:isMarkedAsType": true,
          "ods:isKnownToContainMedia": true,
          "ods:specimenName": "Abyssothyris Thomson, 1927",
          "ods:sourceSystemID": "https://hdl.handle.net/20.5000.1025/3XA-8PT-SAY",
          "dcterms:license": "http://creativecommons.org/licenses/by/4.0/legalcode",
          "dcterms:modified": "03/12/2012",
          "dwc:preparations": "",
          "ods:livingOrPreserved": "Preserved",
          "ods:organisationID": "https://ror.org/0349vqz63",
          "ods:organisationName": "Royal Botanic Garden Edinburgh Herbarium",
          "dwc:datasetName": "Royal Botanic Garden Edinburgh Herbarium",
          "ods:hasEvents": [
            {
              "ods:hasLocation": {
                "dwc:city": "Paris",
                "remarks": {
                  "weather": "good"
                }
              }
            },
            {
              "ods:hasLocation": {
                "dwc:city": "Marseille",
                "remarks": {
                  "weather": "bad"
                }
              }
            }
          ],
          "ods:hasIdentifications": [
            {
              "ods:hasCitations": [
                {
                  "dcterms:identifier": "Miller 1888"
                }
              ],
              "ods:hasTaxonIdentifications": [
                {
                  "dwc:scientificName": "bombus bombus",
                  "dwc:class": "insecta"
                },
                {
                  "dwc:scientificName": "Apis mellifera",
                  "dwc:class": "insecta"
                }
              ]
            },
            {
              "ods:hasCitations": [
                {
                  "dcterms:identifier": "Frank 1900"
                }
              ],
              "ods:hasTaxonIdentifications": [
                {
                  "dwc:scientificName": "bombus bombus"
                },
                {
                  "dwc:scientificName": "Apis mellifera"
                }
              ]
            }
          ]
        }
        """);
  }
}
