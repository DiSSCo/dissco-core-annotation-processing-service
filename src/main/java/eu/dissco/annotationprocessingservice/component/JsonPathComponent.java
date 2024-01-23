package eu.dissco.annotationprocessingservice.component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.Filter;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import eu.dissco.annotationprocessingservice.domain.annotation.ClassSelector;
import eu.dissco.annotationprocessingservice.domain.annotation.FieldSelector;
import eu.dissco.annotationprocessingservice.domain.annotation.Target;
import eu.dissco.annotationprocessingservice.exception.BatchingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.stereotype.Component;

import static com.jayway.jsonpath.Criteria.where;
import static com.jayway.jsonpath.Filter.filter;
import static com.jayway.jsonpath.JsonPath.using;

@Component
@RequiredArgsConstructor
public class JsonPathComponent {

  private final ObjectMapper mapper;
  private final Configuration jsonPathConfig;

  public List<String> getAnnotationTargetBaths(JsonNode batchMetadata, JsonNode annotatedObject,
      Target baseTarget) throws JsonProcessingException, BatchingException {
    var context = using(jsonPathConfig).parse(mapper.writeValueAsString(annotatedObject));
    var filter = generateFirstFilter(batchMetadata);
    List<String> correctJsonPaths = context.read(
        getParentKey(batchMetadata.fields().next().getKey()), filter);
    var targetPath = getTargetPath(baseTarget);

    return iterateOverList(correctJsonPaths, targetPath);
  }

  private String getTargetPath(Target baseTarget) throws BatchingException {
    var selectorType = baseTarget.getOaSelector().getOdsType();
    switch (selectorType) {
      case CLASS_SELECTOR -> {
        var selector = (ClassSelector) (baseTarget.getOaSelector());
        return toDotNotation(selector.getOaClass());
      }
      case FIELD_SELECTOR -> {
        var selector = (FieldSelector) baseTarget.getOaSelector();
        return toDotNotation(selector.getOdsField());
      }
      default -> {
        throw new BatchingException(
            "Unable to batch annotations with selector type " + selectorType);
      }
    }
  }


  private Filter generateFirstFilter(JsonNode batchMetadata) {
    var fields = batchMetadata.fields();
    var firstField = fields.next();
    var targetField = getLastKey(firstField.getKey());
    var targetValue = firstField.getValue();
    return filter(where(targetField).eq(targetValue));
  }

  private String getLastKey(String jsonPath) {
    return jsonPath.replaceAll("^(.*)(?=\\.)", "");
  }

  String getParentKey(String jsonPath) {
    return jsonPath.replaceAll("([^\\.]+$)", "") + "[?]";
  }

  private List<String> iterateOverList(List<String> correctJsonPaths, String basePath) {
    // basePath = targetField/class of the original annotation
    // In this method, we look at indexes for each correct jsonPath we found
    // If there is a corresponding list object in the base path, use that index
    // e.g. if occurrence.1.georef.lattitude is in the correct jsonpath,
    // and occurrence.3.locality is in the basePath
    // Then we update "3" to match the correct jsonpath index of "1"
    // Because in this annotated object, occurrence.1.georef will have the  value that the MAS used to create the annotation
    // So we need to make sure we're annotating the correct field

    List<String> targetPaths = new ArrayList<>();
    for (var correctJsonPath : correctJsonPaths) {
      String annotatePath = basePath;
      var segments = Arrays.asList(correctJsonPath.split("\\."));
      var segmentItr = segments.listIterator();
      while (segmentItr.hasNext()) {
        var segment = segmentItr.next();
        if (segmentItr.hasPrevious() && NumberUtils.isCreatable(segment)) {
            segmentItr.previous();
            var fieldName = segmentItr.previous();
            annotatePath = setIndexOnTargetPath(fieldName, annotatePath, Integer.parseInt(segment));
            segmentItr.next();
            segmentItr.next(); // Iterate twice to move counter to correct position
        }
      }
      targetPaths.add(annotatePath);
    }
    return targetPaths;
  }

  private String setIndexOnTargetPath(String fieldName, String targetPath, int index) {
    var replaceThis = fieldName + ".*.";
    var withThis = fieldName + "." + index + ".";
    return targetPath.replace(replaceThis, withThis);
  }

  private String toDotNotation(String jsonPath) {
    jsonPath = jsonPath
        .replace("$", "")
        .replaceAll("\\[(?=\\*])", ".")
        .replaceAll("\\[(?!\\*])", "")
        .replace("]", ".")
        .replace("..", ".")
        .replace("\'", "");
    return jsonPath;
  }

}
