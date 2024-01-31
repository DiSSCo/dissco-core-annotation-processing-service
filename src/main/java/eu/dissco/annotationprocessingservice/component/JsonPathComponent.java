package eu.dissco.annotationprocessingservice.component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.Filter;
import com.jayway.jsonpath.JsonPathException;
import eu.dissco.annotationprocessingservice.domain.annotation.ClassSelector;
import eu.dissco.annotationprocessingservice.domain.annotation.FieldSelector;
import eu.dissco.annotationprocessingservice.domain.annotation.Target;
import eu.dissco.annotationprocessingservice.exception.BatchingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.stereotype.Component;

import static com.jayway.jsonpath.Criteria.where;
import static com.jayway.jsonpath.Filter.filter;
import static com.jayway.jsonpath.JsonPath.using;

@Component
@RequiredArgsConstructor
@Slf4j
public class JsonPathComponent {

  private final ObjectMapper mapper;
  private final Configuration jsonPathConfig;
  private final Pattern lastKeyPattern;


  public List<String> getAnnotationTargetPaths(JsonNode batchMetadata, JsonNode annotatedObject,
      Target baseTarget) throws JsonProcessingException, BatchingException {
    var context = using(jsonPathConfig).parse(mapper.writeValueAsString(annotatedObject));
    var filter = generateFilter(batchMetadata);
    List<String> correctJsonInputPaths = null;
    try {
      correctJsonInputPaths = context.read(
          getParentKey(batchMetadata.fields().next().getKey()), filter);
    } catch (JsonPathException e) {
      log.error("Poorly formatted json path", e);
      throw new BatchingException();
    }
    var correctJsonInputPathsDot = correctJsonInputPaths.stream()
        .map(this::toDotDelineation).toList();
    var targetPath = getTargetPath(baseTarget);

    return iterateOverList(correctJsonInputPathsDot, targetPath);
  }

  private String getTargetPath(Target baseTarget) throws BatchingException {
    var selectorType = baseTarget.getOaSelector().getOdsType();
    switch (selectorType) {
      case CLASS_SELECTOR -> {
        var selector = (ClassSelector) (baseTarget.getOaSelector());
        return toDotDelineation(selector.getOaClass());
      }
      case FIELD_SELECTOR -> {
        var selector = (FieldSelector) baseTarget.getOaSelector();
        return toDotDelineation(selector.getOdsField());
      }
      default -> {
        log.error("Unable to batch annotations with selector type {}", selectorType);
        throw new BatchingException();
      }
    }
  }

  private Filter generateFilter(JsonNode batchMetadata) throws BatchingException {
    var fields = batchMetadata.fields();
    var firstField = fields.next();
    var targetField = getLastKey(firstField.getKey());
    var targetValue = firstField.getValue().asText();
    return filter(where(targetField).is(targetValue));
  }

  private String getLastKey(String jsonPath) throws BatchingException {
    // Get last key of a jsonPath using regex defined in
    var lastKeyMatcher = lastKeyPattern.matcher(jsonPath);
    if (lastKeyMatcher.find()){
      return lastKeyMatcher.group().replace("\\.", "");
    }
    else {
      log.error("Unable to parse last key of jsonPath {}", jsonPath);
      throw new BatchingException();
    }
  }

  String getParentKey(String jsonPath) {
    // 1st regex removes last key
    // 2nd regex removes trailing periods
    // Finally add [?] to allow filtering
    return jsonPath
        .replaceAll(lastKeyPattern.pattern(), "")
        .replaceAll("[.]+$","")
        + "[?]";
  }

  private List<String> iterateOverList(List<String> correctJsonInputPaths, String baseTargetPath) {
    /*
    - `baseTargetPath`: the target field or class of the original annotation
    - `correctJsonInputPaths`: JSON paths corresponding to input fields that contain the same value used by the annotating MAS to create the original annotation.

    - In this method, we examine the indexes within `correctJsonInputPaths` to construct the target field/class of the new annotation.
    - For example, if "occurrence.1.georeference.latitude" is a correct input JSON path, it signifies that the value stored at this path is identical to the value used by the MAS to create the original annotation.
    - If "occurrence.3.locality" is found in the `baseTargetPath` (indicating that it is the field that was annotated), we need to update the index of the occurrence array.
    - The result will rgwb be "occurrence.3.locality".
     */

    List<String> targetPaths = new ArrayList<>();
    for (var correctJsonInputPath : correctJsonInputPaths) {
      String annotatePath = baseTargetPath;
      var segments = Arrays.asList(correctJsonInputPath.split("\\."));
      var segmentItr = segments.listIterator();
      while (segmentItr.hasNext()) {
        var segment = segmentItr.next();
        if (segmentItr.hasPrevious() && NumberUtils.isCreatable(segment)) {
          segmentItr.previous();
          var fieldName = segmentItr.previous();
          annotatePath = setIndexOnTargetPath(fieldName, annotatePath, Integer.parseInt(segment));
          segmentItr.next();
          segmentItr.next(); // Iterate twice to move counter to correct position after calling itr.previous()
        }
      }
      targetPaths.add(annotatePath);
    }
    return targetPaths;
  }

  private String setIndexOnTargetPath(String fieldName, String targetPath, int index) {
    // Regex that captures the field name plus next 3 characters (i.e. field name plus index)
    var replaceThis = "(" + fieldName + ".{3})";
    var withThis = fieldName + "." + index + ".";
    return targetPath.replaceAll(replaceThis, withThis);
  }

  private String toDotDelineation(String jsonPath) {
    // Transforms bracket notation to dot notation
    // We use dot notation to split jsonPaths, but our jsonPath library will only output in bracket notation
    // From: "[field][1][otherField]" or "field[1].otherfield"
    // To: "field.1.otherField"

    jsonPath = jsonPath
        .replace("$", "")
        .replaceAll("\\[(?=\\*|[0-9]])", ".") // Captures [ next to * or 1-9
        .replaceAll("\\[(?!\\*|[0-9]])", "")  // Captures [ next to all other characters
        .replace("]", ".")
        .replace("..", ".")
        .replace("\'", "");
    return jsonPath;
  }

}
