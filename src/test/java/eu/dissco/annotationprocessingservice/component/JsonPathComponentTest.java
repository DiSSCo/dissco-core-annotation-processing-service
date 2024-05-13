package eu.dissco.annotationprocessingservice.component;

import static com.jayway.jsonpath.JsonPath.using;
import static eu.dissco.annotationprocessingservice.TestUtils.DOI_PROXY;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  void testGetAnnotationTargetPathsFieldSelectorExtendedDifferentLevels() throws Exception {
    // Given
    var baseTargetClassSelector = givenOaTarget(ID);
    var expected = List.of(Target.builder()
        .odsId(DOI_PROXY + ID)
        .odsType(AnnotationTargetType.DIGITAL_SPECIMEN)
        .oaSelector(new FieldSelector("digitalSpecimenWrapper.occurrences[2].locality"))
        .build());
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

  @Test
  void testCompoundFieldIndex(){
    var givenList = new ArrayList<FieldIndex>();
    givenList.add(new FieldIndex(List.of("A"), List.of(1)));
    givenList.add(new FieldIndex(List.of("B"), List.of(2)));
    givenList.add(new FieldIndex(List.of("C"), List.of(3)));
    var expected = new ArrayList<FieldIndex>();
    expected.add(new FieldIndex(List.of("A"), List.of(1)));
    expected.add(new FieldIndex(List.of("A", "B"), List.of(1, 2)));
    expected.add(new FieldIndex(List.of("A", "B", "C"), List.of(1, 2, 3)));
    var result = compoundFieldIndex(givenList);
    assertThat(result).isEqualTo(expected);
  }


  private ArrayList<FieldIndex> compoundFieldIndex(ArrayList<FieldIndex> lst) {
    if (lst.size() == 1) {
      return lst;
    }
    var previousCompound = compoundFieldIndex(new ArrayList<>(lst.subList(0, lst.size() - 1)));
    FieldIndex currentFieldIndex = lst.get(lst.size() - 1);
    var compoundedField = new ArrayList<>(previousCompound.get(previousCompound.size() - 1).field());
    var compoundedIndex = new ArrayList<>(previousCompound.get(previousCompound.size() - 1).index());
    compoundedField.addAll(currentFieldIndex.field());
    compoundedIndex.addAll(currentFieldIndex.index());
    previousCompound.add(new FieldIndex(compoundedField, compoundedIndex));
    return previousCompound;
  }

  private ArrayList<ArrayList<String>> compoundList(ArrayList<ArrayList<String>> l) {
    if (l.size() == 1) {
      return l;
    }
    var previousCompound = compoundList(new ArrayList<>(l.subList(0, l.size() - 1)));
    var currentList = l.get(l.size() - 1);
    var previousResult = previousCompound.get(previousCompound.size() - 1);
    var compoundedList = new ArrayList<>(previousResult);
    compoundedList.addAll(currentList);
    previousCompound.add(compoundedList);
    return previousCompound;
  }

  @Test
  void testIsTrueMatch() throws Exception {
    /*
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

    var result = jsonPathComponent.isTrueMatch(batchMetadata, null, null);

    assertThat(result).isTrue(); */
  }

  @Test
  void testIsTrueMatchFalse() throws Exception {
    /*
    var annotatedObject = givenJsonNode();
    var batchMetadata = new BatchMetadataExtended(1, List.of(
        new BatchMetadataSearchParam(
            "specimen.occurrences[*].location.city",
            "Paris"),
        new BatchMetadataSearchParam(
            "specimen.occurrences[*].remarks.weather",
            "bad"
        )));

    var result = jsonPathComponent.isTrueMatch(batchMetadata, null, null);
    assertThat(result).isFalse(); */
  }

  @Test
  void testIsTrueMatch2() throws Exception {
    /*
    var annotatedObject = givenJsonNode();
    var batchMetadata = new BatchMetadataExtended(1, List.of(
        new BatchMetadataSearchParam(
            "specimen.occurrences[*].location.city",
            "Paris"),
        new BatchMetadataSearchParam(
            "specimen.institution",
            "MNH"
        )));

    var result = jsonPathComponent.isTrueMatch(batchMetadata, null, null);
    assertThat(result).isTrue(); */
  }

  private HashMap<BatchMetadataSearchParam, List<Integer>> makeSubmap(
      List<BatchMetadataSearchParam> params, List<List<Integer>> idx) {
    HashMap<BatchMetadataSearchParam, List<Integer>> map = new HashMap<>();
    for (int i = 0; i < params.size(); i++) {
      map.put(params.get(i), idx.get(i));
    }
    return map;

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

  @Test
  void testBuildTargetPaths() throws Exception {
    /*
    var targetPath = "specimen.occurrences.2.location.city";
    var expected = List.of(
        "specimen.occurrences.1.location.city"
    );
    var selector = new FieldSelector(targetPath);
    var target = Target.builder()
        .odsId(ID)
        .odsType(AnnotationTargetType.DIGITAL_SPECIMEN)
        .oaSelector(selector)
        .build();

    var document = givenJsonNode();
    var context = using(jsonPathConfiguration).parse(MAPPER.writeValueAsString(document));
    var commonIndexes = Map.of(
        List.of("occurrences"), List.of(List.of(1)),
        List.of("identifications"), List.of(List.of(1))
    );
    var result = jsonPathComponent.getAnnotationTargetPaths(commonIndexes, target, context);
    assertThat(result).isEqualTo(expected);

     */
  }




  record FieldIndex(
      List<String> field,
      List<Integer> index
  ){}


}
