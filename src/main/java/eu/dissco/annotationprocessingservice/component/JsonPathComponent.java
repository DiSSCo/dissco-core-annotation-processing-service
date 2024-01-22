package eu.dissco.annotationprocessingservice.component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.Filter;
import com.jayway.jsonpath.JsonPath;
import eu.dissco.annotationprocessingservice.domain.annotation.Target;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import static com.jayway.jsonpath.Criteria.where;
import static com.jayway.jsonpath.Filter.filter;
import static com.jayway.jsonpath.JsonPath.using;

@Component
@RequiredArgsConstructor
public class JsonPathComponent {

  private final ObjectMapper mapper;
  private final Configuration jsonPathConfig;

  public List<String> getAnnotationTargetBaths(JsonNode batchMetadata, JsonNode annotationObject,
      Target baseTarget) throws JsonProcessingException {
    var context = using(jsonPathConfig).parse(mapper.writeValueAsString(annotationObject));
    var filter = generateFirstFilter(batchMetadata);
    var correctVals = context.read(getParentKey(batchMetadata.fields().next().getKey()), filter);

    return null;
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

}
