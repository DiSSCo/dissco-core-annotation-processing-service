package eu.dissco.annotationprocessingservice.component;

import static eu.dissco.annotationprocessingservice.TestUtils.DOI_PROXY;
import static eu.dissco.annotationprocessingservice.TestUtils.HANDLE_PROXY;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.givenBatchMetadataCountryAndContinent;
import static eu.dissco.annotationprocessingservice.TestUtils.givenBatchMetadataLatitudeSearch;
import static eu.dissco.annotationprocessingservice.TestUtils.givenElasticDocument;
import static eu.dissco.annotationprocessingservice.TestUtils.givenOaTarget;
import static org.assertj.core.api.Assertions.assertThat;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


@Slf4j
class JsonPathComponentTest {

  private JsonPathComponent jsonPathComponent;
  Pattern lastKeyMatcher = Pattern.compile("[^.]+(?=\\.$)|([^.]+$)");
  private GreatestCommonSubstringComponent substringComponent;

  @BeforeEach
  void init() {
    var jsonPathConfiguration = Configuration.builder()
        .options(Option.AS_PATH_LIST, Option.SUPPRESS_EXCEPTIONS, Option.ALWAYS_RETURN_LIST)
        .build();
    substringComponent = new GreatestCommonSubstringComponent();
    jsonPathComponent = new JsonPathComponent(MAPPER, jsonPathConfiguration, lastKeyMatcher,
        substringComponent);
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
  void testGetAnnotationTargetPathsFieldSelectorExtended() throws Exception {
    // Given
    var baseTargetClassSelector = givenOaTarget(ID);
    var expected = List.of(new Target()
        .withOdsId(DOI_PROXY + ID)
        .withOdsType(AnnotationTargetType.DIGITAL_SPECIMEN)
        .withSelector(new FieldSelector("digitalSpecimenWrapper.occurrences[0].locality")));

    // When
    var result = jsonPathComponent.getAnnotationTargetsExtended(
        givenBatchMetadataCountryAndContinent("Netherlands", "Europe"),
        givenElasticDocument("Netherlands", ID),
        baseTargetClassSelector);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testGetAnnotationTargetPathsFieldSelectorExtendedDifferentLevels() throws Exception {
    // Given
    var baseTargetClassSelector = givenOaTarget(ID);
    var expected = List.of(new Target()
        .withOdsId(DOI_PROXY + ID)
        .withOdsType(AnnotationTargetType.DIGITAL_SPECIMEN)
        .withSelector(new FieldSelector("digitalSpecimenWrapper.occurrences[2].locality")));
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
        baseTargetClassSelector);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testGetAnnotationTargetPathsFieldSelectorExtendedEmpty() throws Exception {
    // Given
    var baseTargetClassSelector = givenOaTarget(ID);
    var searchDoc = givenElasticDocument("Netherlands", ID);

    // When
    var result = jsonPathComponent.getAnnotationTargetsExtended(
        givenBatchMetadataCountryAndContinent("Netherlands", "Error"),
        givenElasticDocument("Netherlands", ID),
        baseTargetClassSelector);

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  void testGetAnnotationTargetPathsBadBatchMetadata() {
    // Given
    var baseTargetClassSelector = Target.builder()
        .odsId(HANDLE_PROXY + ID)
        .odsType(AnnotationTargetType.DIGITAL_SPECIMEN)
        .oaSelector(new ClassSelector()
            .withOaClass("digitalSpecimenWrapper.occurrences[1].locality"))
        .build();
    // Path is incorrect
    var batchMetadata = new BatchMetadata(
        1, "digitalSpecimenWrapper.occurrences[*].location.georeference.dwc:decimalLatitude", "11");

    //Then
    assertThrows(BatchingException.class,
        () -> jsonPathComponent.getAnnotationTargets(batchMetadata, givenElasticDocument(ID,
                "Netherlands"),
            baseTargetClassSelector));
  }

  @Test
  void testBadSelectorType() {
    // Given
    var baseTargetClassSelector = Target.builder()
        .odsId(ID)
        .odsType(AnnotationTargetType.DIGITAL_SPECIMEN)
        .oaSelector(new FragmentSelector())
        .build();

    // When
    assertThrows(BatchingException.class, () ->
        jsonPathComponent.getAnnotationTargets(givenBatchMetadataLatitudeSearch(),
            givenElasticDocument(),
            baseTargetClassSelector));
  }

  @Test
  void testBadJsonpathFormat() throws JsonProcessingException {
    // Given
    var batchMetadata = new BatchMetadata(1,
        "[digitalSpecimenWrapper][occurrences][*][location][georeference]['dwc:decimalLatitude']['dwc:value']",
        "11");
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

  @Test
  void testIsTrueMatch() throws Exception {
    var annotatedObject = givenJsonNode();
    var batchMetadata = new BatchMetadataExtended(1, List.of(
        new BatchMetadataSearchParam(
            "specimen.occurrences[*].location.city",
            "Paris"),
        new BatchMetadataSearchParam(
            "specimen.occurrences[*].remarks.weather",
            "good"
        ),
        new BatchMetadataSearchParam(
            "specimen.institution",
            "MNH"
        )));

    var result = jsonPathComponent.isTrueMatch(annotatedObject, batchMetadata);

    assertThat(result).isTrue();
  }

  @Test
  void testIsTrueMatchFalse() throws Exception {
    var annotatedObject = givenJsonNode();
    var batchMetadata = new BatchMetadataExtended(1, List.of(
        new BatchMetadataSearchParam(
            "specimen.occurrences[*].location.city",
            "Paris"),
        new BatchMetadataSearchParam(
            "specimen.occurrences[*].remarks.weather",
            "bad"
        )));

    var result = jsonPathComponent.isTrueMatch(annotatedObject, batchMetadata);
    assertThat(result).isFalse();
  }

  @Test
  void testIsTrueMatch2() throws Exception {
    var annotatedObject = givenJsonNode();
    var batchMetadata = new BatchMetadataExtended(1, List.of(
        new BatchMetadataSearchParam(
            "specimen.occurrences[*].location.city",
            "Paris"),
        new BatchMetadataSearchParam(
            "specimen.institution",
            "MNH"
        )));

    var result = jsonPathComponent.isTrueMatch(annotatedObject, batchMetadata);
    assertThat(result).isTrue();
  }

  @Test
  void testIndexedPathsHaveCommonality() {
    var param1 = new BatchMetadataSearchParam(
        "specimen.occurrences[*].locality[*].city",
        "Paris");
    var param2 = new BatchMetadataSearchParam(
        "specimen.occurrences[*].locality[*].country",
        "France");
    HashMap<List<String>, HashMap<BatchMetadataSearchParam, List<Integer>>> commonPathMap = new HashMap<>();
    commonPathMap.put(List.of("occurrences"),
        makeSubmap(List.of(param1, param2), List.of(List.of(1), List.of(1))));
    commonPathMap.put(List.of("occurrences", "locality"),
        makeSubmap(List.of(param1, param2), List.of(List.of(1,1), List.of(1,1))));

    var result =  indexedPathsHaveCommonality(commonPathMap);

    assertThat(result).isTrue();

  }

  private HashMap<BatchMetadataSearchParam, List<Integer>> makeSubmap(
      List<BatchMetadataSearchParam> params, List<List<Integer>> idx) {
    HashMap<BatchMetadataSearchParam, List<Integer>> map = new HashMap<>();
    for (int i = 0; i < params.size(); i++) {
      map.put(params.get(i), idx.get(i));
    }
    return map;

  }


  private boolean indexedPathsHaveCommonality(
      HashMap<List<String>, HashMap<BatchMetadataSearchParam, List<Integer>>> commonPathMap) {
    for (var fields : commonPathMap.entrySet()) {
      var firstList = fields.getValue().entrySet().iterator().next().getValue();
      HashSet<List<Integer>> commonList = new HashSet<>(List.of(firstList));
      for (var indexMap : fields.getValue().entrySet()) {
        var currentList = indexMap.getValue();
        commonList.retainAll(List.of(indexMap.getValue()));
      }
      if (commonList.isEmpty()) {
        return false;
      }
    }
    return true;
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
