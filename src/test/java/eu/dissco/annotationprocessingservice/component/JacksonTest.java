package eu.dissco.annotationprocessingservice.component;

import static com.jayway.jsonpath.JsonPath.using;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.Filter;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.Predicate;
import com.jayway.jsonpath.ReadContext;
import com.jayway.jsonpath.internal.function.sequence.Index;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import com.fasterxml.jackson.core.type.TypeReference;
import org.testcontainers.shaded.org.apache.commons.lang3.math.NumberUtils;
import scala.concurrent.impl.FutureConvertersImpl.P;

import static com.jayway.jsonpath.JsonPath.parse;
import static com.jayway.jsonpath.Criteria.where;
import static com.jayway.jsonpath.Filter.filter;


@Slf4j
class JacksonTest {

  @Test
  void testFields() throws Exception {

    var elements = MAPPER.readValue(MAPPER.writeValueAsString(givenSpecimen()),
        new TypeReference<Map<String, Object>>() {
        });
    ArrayList<String> jsonPaths = new ArrayList<>();
    String targetPath = "digitalSpecimenWrapper.occurrences.location.georeference.dwc:decimalLatitude";
    var targetVal = "11";
    var currentPathBuilder = new StringBuilder();
    getJsonPaths(elements, jsonPaths, targetPath, currentPathBuilder, targetVal);
    assertThat(jsonPaths).hasSize(2);
  }

  @Test
  void testJsonPaths() throws Exception {
    var specimen = givenSpecimen();
    var specStr = MAPPER.writeValueAsString(specimen);
    var targetPath = "digitalSpecimenWrapper.occurrences[*].location.georeference.['dwc:decimalLatitude']";
    var targetPathParent = "digitalSpecimenWrapper.occurrences[*].location.georeference";
    var splitKeys = targetPath.split("\\.");
    var lastKey = splitKeys[splitKeys.length - 1];
    var targetVal = 11;

    var annotatedPath = "$..digitalSpecimenWrapper.occurrences[*].annotateTarget";

    var parent = targetPath.replaceAll("([^\\.]+$)", "");
    var config = Configuration.builder().options(Option.AS_PATH_LIST).build();

    Filter filter = filter(where(lastKey).is(targetVal)).and(
        where("['dwc:decimalLatitude']").eq(targetVal));
    var targetPathParentFilterable = targetPathParent + "[?]";
    List<String> filtered = using(config).parse(specStr).read(targetPathParentFilterable, filter);
    var expresison = "[?(@['dwc:decimalLatitude'] == 11)]";

    var context = using(config).parse(specStr);

    var result = context.read(annotatedPath);

    assertThat(filtered).isNotNull();
  }

  @Test
  void testMatcher() throws Exception {
    var config = Configuration.builder().options(Option.AS_PATH_LIST).build();

    var specimen = givenSpecimen();
    var specStr = MAPPER.writeValueAsString(specimen);
    var targetPath = "digitalSpecimenWrapper.occurrences[*].location.georeference.['dwc:decimalLatitude']";
    var splitKeys = targetPath.split("\\.");
    var lastKey = splitKeys[splitKeys.length - 1];
    var targetVal = 11;
    Filter filter = filter(where(lastKey).is(targetVal)).and(
        where("['dwc:decimalLatitude']").eq(targetVal));
    var targetPathParent = "digitalSpecimenWrapper.occurrences[*].location.georeference[?]";
    List<String> filtered = using(config).parse(specStr).read(targetPathParent, filter);

    var annotatedPath = "$..digitalSpecimenWrapper.occurrences[*].annotateTarget";

    iterateOverList(filtered, annotatedPath);
  }


  @Test
  void testInputToTarget() throws Exception {
    var config = Configuration.builder().options(Option.AS_PATH_LIST).build();
    var specimen = givenSpecimen();
    var specStr = MAPPER.writeValueAsString(specimen);
    var targetPath = "digitalSpecimenWrapper.occurrences[*].location.georeference.locality";
    var inputPath = "digitalSpecimenWrapper.occurrences[*].location.georeference.['dwc:decimalLatitude']";
    var bracketPath = "[digitalSpecimenWrapper][occurrences][*][location.georeference]['dwc:decimalLatitude']";

    var splitKeysInput = inputPath.split("\\.");
    var splitKeysTarget = targetPath.split("\\.");

    var lastKey = splitKeysInput[splitKeysInput.length - 1];
    var inputVal = 11;
    Filter filter = filter(where(lastKey).is(inputVal));
    var inputPathParent = "digitalSpecimenWrapper.occurrences[*].location.georeference[?]";
    List<String> filteredBracketNotation = using(config).parse(specStr)
        .read(inputPathParent, filter);
    var filtered = filteredBracketNotation.stream().map(this::toDotNotation).toList();
    iterateOverList(filtered, toDotNotation(targetPath));
  }


  private String toDotNotation(String jsonPath) {
    jsonPath = jsonPath.replace("$", "")
        .replaceAll("\\[(?=\\*])", ".")
        .replaceAll("\\[(?!\\*])", "")
        .replace("]", ".")
        .replace("..", ".")
        .replace("\'", "");
    return jsonPath;
  }


  private String iterateOverList(List<String> correctJsonPaths, String annotatePath) {
    for (var correctJsonPath : correctJsonPaths) {
      var segments = Arrays.asList(correctJsonPath.split("\\."));
      var segmentItr = segments.listIterator();
      while (segmentItr.hasNext()) {
        var segment = segmentItr.next();
        if (segmentItr.hasPrevious()){
          if (NumberUtils.isCreatable(segment)){
            segmentItr.previous();
            var fieldName = segmentItr.previous();
            annotatePath = setIndexOnTargetPath(fieldName, annotatePath, Integer.parseInt(segment));
            segmentItr.next();
            segmentItr.next();
          }

        }
      }
    }
    return annotatePath;
  }

  private String setIndexOnTargetPath(String fieldName, String targetPath, int index){
    var replaceThis = fieldName + ".*.";
    var withThis = fieldName + "." + index + ".";
    return targetPath.replace(replaceThis, withThis);
  }


  private JsonNode givenSpecimen() throws JsonProcessingException {
    return MAPPER.readTree("""
        {
          "digitalSpecimenWrapper": {
            "other": ["a", "10"],
            "occurrences": [
              {
                "dwc:occurrenceRemarks": "Correct",
                "annotateTarget":"this",
                "location": {
                  "georeference": {
                    "dwc:decimalLatitude":"11",
                    "dwc:decimalLongitude": "10",
                    "dwc":["1"]
                  },
                  "locality":"unknown"
                }
              },
              {
                "dwc:occurrenceRemarks": "Incorrect",
                "annotateTarget":"this",
                "location": {
                  "georeference": {
                    "dwc:decimalLatitude":"10",
                    "dwc:decimalLongitude": "10"
                  },
                  "locality":"unknown"
                }
              },
              {
                "dwc:occurrenceRemarks": "Correct",
                "blah":10,
                "annotateTarget":"this",
                "location": {
                  "georeference": {
                    "dwc:decimalLatitude":"11",
                    "dwc:decimalLongitude": "10.1",
                    "test":"hello"
                  },
                  "locality":"unknown"
                }
              }
            ]
          }
        }""");
  }


  private void getJsonPaths(Map<String, Object> jsonElements, List<String> jsonPaths,
      String targetPath, StringBuilder currentPath, String targetVal) {
    for (var entry : jsonElements.entrySet()) {
      var key = entry.getKey();
      var val = entry.getValue();
      currentPath.append(key).append(".");
      if (val instanceof Map) {
        Map<String, Object> map = (Map<String, Object>) val;
        getJsonPaths(map, jsonPaths, targetPath, currentPath, targetVal);
        currentPath
            .deleteCharAt(currentPath.length() - 1)
            .setLength(currentPath.lastIndexOf(".") + 1);
        log.info("path {}", currentPath);
      } else if (val instanceof List<?> list) {
        for (int i = 0; i < list.size(); i++) {
          currentPath.deleteCharAt(currentPath.length() - 1).append("[").append(i).append("].");
          log.info("path {}", currentPath);
          var listEntry = list.get(i);
          if (listEntry instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) listEntry;
            getJsonPaths(map, jsonPaths, targetPath, currentPath, targetVal);
            currentPath.setLength(currentPath.length() - 3);
          } else {
            currentPath.setLength(currentPath.length() - 3);
            log.info("");
          }
        }
        currentPath.setLength(currentPath.lastIndexOf("."));
        if (!String.valueOf(currentPath.charAt(currentPath.length() - 1)).equals(".")) {
          currentPath.append(".");
          log.info(currentPath.toString());
        }
        log.info("path {}", currentPath);
      } else {
        var strippedPath = currentPath.deleteCharAt(currentPath.length() - 1).toString()
            .replaceAll("\\[[^\\]]*\\]", "");
        if (targetPath.equals(strippedPath)) {
          if (val.equals(targetVal)) {
            jsonPaths.add(currentPath.toString());
          }
        }
        currentPath
            .deleteCharAt(currentPath.length() - 1)
            .setLength(currentPath.lastIndexOf(".") + 1);
        log.info("new path: {}", currentPath);
      }
    }
  }


}
