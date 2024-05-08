package eu.dissco.annotationprocessingservice.component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.Filter;
import com.jayway.jsonpath.JsonPathException;
import eu.dissco.annotationprocessingservice.domain.BatchMetadata;
import eu.dissco.annotationprocessingservice.domain.BatchMetadataExtended;
import eu.dissco.annotationprocessingservice.domain.BatchMetadataSearchParam;
import eu.dissco.annotationprocessingservice.domain.annotation.ClassSelector;
import eu.dissco.annotationprocessingservice.domain.annotation.FieldSelector;
import eu.dissco.annotationprocessingservice.domain.annotation.Selector;
import eu.dissco.annotationprocessingservice.domain.annotation.SelectorType;
import eu.dissco.annotationprocessingservice.domain.annotation.Target;
import eu.dissco.annotationprocessingservice.exception.BatchingException;
import eu.dissco.annotationprocessingservice.exception.BatchingRuntimeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import static com.jayway.jsonpath.Criteria.where;
import static com.jayway.jsonpath.Filter.filter;
import static com.jayway.jsonpath.JsonPath.using;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;

@Component
@RequiredArgsConstructor
@Slf4j
public class JsonPathComponent {

  private final ObjectMapper mapper;
  private final Configuration jsonPathConfig;
  @Qualifier("lastKey")
  private final Pattern lastKeyPattern;
  private final GreatestCommonSubstringComponent substringComponent;
  private final Pattern arrayFieldPattern = Pattern.compile(".\\w+\\.\\d+.|\\.\\w+\\.\\d+.");


  public List<Target> getAnnotationTargetsExtended(BatchMetadataExtended batchMetadata,
      JsonNode annotatedObject,
      Target baseTarget) throws JsonProcessingException, BatchingException {
    if (!isTrueMatch(annotatedObject, batchMetadata)){
      log.warn("False positive detected");
      log.debug("{} does not comply to batch metadata {}", annotatedObject, batchMetadata);
      return Collections.emptyList();
    }
    var context = using(jsonPathConfig).parse(mapper.writeValueAsString(annotatedObject));
    var filter = generateFilterExtended(batchMetadata);
    List<String> correctJsonInputPaths = null;

    try {
      var parent = removeLastKey(batchMetadata.searchParams().get(0).inputField());
      correctJsonInputPaths = context.read(parent, filter);
    } catch (JsonPathException e) {
      log.error("Poorly formatted json path", e);
      throw new BatchingException();
    }
    var correctJsonInputPathsDot = correctJsonInputPaths.stream()
        .map(this::toDotDelineation).toList();
    var targetPath = getTargetPath(baseTarget);

    var targetPaths = iterateOverList(correctJsonInputPathsDot, targetPath);
    return buildOaTargets(targetPaths, baseTarget, annotatedObject.get("id").asText());
  }


  public List<Target> getAnnotationTargets(BatchMetadata batchMetadata, JsonNode annotatedObject,
      Target baseTarget) throws JsonProcessingException, BatchingException {
    var context = using(jsonPathConfig).parse(mapper.writeValueAsString(annotatedObject));
    var filter = generateFilter(batchMetadata);
    List<String> correctJsonInputPaths = null;
    try {
      var parent = removeLastKey(batchMetadata.inputField());
      correctJsonInputPaths = context.read(
          removeLastKey(batchMetadata.inputField()), filter);
    } catch (JsonPathException e) {
      log.error("Poorly formatted json path", e);
      throw new BatchingException();
    }
    var correctJsonInputPathsDot = correctJsonInputPaths.stream()
        .map(this::toDotDelineation).toList();
    var targetPath = getTargetPath(baseTarget);

    var targetPaths = iterateOverList(correctJsonInputPathsDot, targetPath);
    return buildOaTargets(targetPaths, baseTarget, annotatedObject.get("id").asText());
  }

  private List<Target> buildOaTargets(List<String> targetPaths, Target baseTarget,
      String newTargetId) {
    boolean isClassSelector = baseTarget.getOaSelector().getOdsType()
        .equals(SelectorType.CLASS_SELECTOR);
    List<Target> newTargets = new ArrayList<>();
    for (var targetPath : targetPaths) {
      var append = targetPath.contains("digitalSpecimenWrapper") ? "" : "$";
      Selector newSelector = null;
      if (isClassSelector) {
        newSelector = new ClassSelector()
            .withOaClass(append + targetPath);
      } else {
        newSelector = new FieldSelector()
            .withOdsField(append + targetPath);
      }
      newTargets.add(new Target()
          .withOdsType(baseTarget.getOdsType())
          .withOdsId("https://doi.org/" + newTargetId)
          .withSelector(newSelector));
    }
    return newTargets;
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

  private Filter generateFilterExtended(BatchMetadataExtended batchMetadata)
      throws BatchingException {
    var paramItr = batchMetadata.searchParams().iterator();
    var paramCursor = paramItr.next();
    var criteria = where(getLastKey(paramCursor.inputField())).is(paramCursor.inputValue());

    while (paramItr.hasNext()) {
      paramCursor = paramItr.next();
      criteria = criteria.and(getLastKey(paramCursor.inputField())).is(paramCursor.inputValue());
    }
    return filter(criteria);
  }

  private Filter generateFilter(BatchMetadata batchMetadata) throws BatchingException {
    var targetField = getLastKey(batchMetadata.inputField());
    var targetValue = batchMetadata.inputValue();
    return filter(where(targetField).is(targetValue));
  }

  private Filter generateFilter(BatchMetadataSearchParam batchMetadata) throws BatchingException {
    var targetField = getLastKey(batchMetadata.inputField());
    var targetValue = batchMetadata.inputValue();
    return filter(where(targetField).is(targetValue));
  }

  private String getLastKey(String jsonPath) throws BatchingException {
    // Get last key of a jsonPath using regex defined in
    var lastKeyMatcher = lastKeyPattern.matcher(jsonPath);
    if (lastKeyMatcher.find()) {
      return lastKeyMatcher.group().replace("\\.", "");
    } else {
      log.error("Unable to parse last key of jsonPath {}", jsonPath);
      throw new BatchingException();
    }
  }

  String removeLastKey(String jsonPath) {
    jsonPath = jsonPath.replaceAll(lastKeyPattern.pattern(), "");
    return removeTrailingPeriod(jsonPath) + "[?]";
  }

  private List<String> iterateOverList(List<String> correctJsonInputPaths, String baseTargetPath) {
    /*
    - `baseTargetPath`: the target field or class of the original annotation
    - `correctJsonInputPaths`: JSON paths corresponding to input fields that contain the same value used by the annotating MAS to create the original annotation.

    - In this method, we examine the indexes within `correctJsonInputPaths` to construct the target field/class of the new annotation.
    - For example, if "occurrence.1.georeference.latitude" is a correct input JSON path, it signifies that the value stored at this path is identical to the value used by the MAS to create the original annotation.
    - If "occurrence.3.locality" is found in the `baseTargetPath` (indicating that it is the field that was annotated), we need to update the index of the occurrence array.
    - The result will be "occurrence.3.locality".
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

  private static String setIndexOnTargetPath(String fieldName, String targetPath, int index) {
    // Regex that captures the field name plus next 3 characters (i.e. field name plus index)
    var replaceThis = "(" + fieldName + ".{3})";
    var withThis = fieldName + "[" + index + "].";
    return targetPath.replaceAll(replaceThis, withThis);
  }

  private String toDotDelineation(String jsonPath) {
    // We use dot notation to split jsonPaths, but our jsonPath library will only output in bracket notation
    // From: "[field][1][otherField]" or "field[1].otherfield"
    // To: "field.1.otherField"

    jsonPath = jsonPath
        .replace("$", "")
        .replaceAll("\\[(?=[*|\\d])", ".") // Captures [ next to * or 1-9
        .replaceAll("\\[(?![*|\\d])", "")  // Captures [ next to all other characters
        .replace("]", ".")
        .replace("..", ".")
        .replace("'", "");
    return removeTrailingPeriod(jsonPath);
  }

  private static String removeTrailingPeriod(String jsonPath) {
    var jsonPathArray = jsonPath.split("\\.");
    return String.join(".", jsonPathArray);
  }

  public boolean isTrueMatch(JsonNode annotatedObject, BatchMetadataExtended batchMetadata) {
    if (batchMetadata.searchParams().size() == 1) {
      return true;
    }
    DocumentContext context;
    HashMap<List<String>, HashMap<BatchMetadataSearchParam, List<Integer>>> indexedPaths = new HashMap<>();
    try {
      context = using(jsonPathConfig).parse(mapper.writeValueAsString(annotatedObject));
      for (var param : batchMetadata.searchParams()) {
        var validPaths = new HashSet<String>(
            context.read(removeLastKey(param.inputField()), generateFilter(param)))
            .stream().map(this::toDotDelineation)
            .collect(Collectors.toCollection(HashSet::new));
        indexArrayPaths(param, validPaths, indexedPaths);
      }
    } catch (JsonProcessingException e) {
      log.error("Unable to read jsonpath", e);
      throw new BatchingRuntimeException();
    } catch (BatchingException e) {
      throw new BatchingRuntimeException();
    }
    return indexedPathsHaveCommonality(indexedPaths);
  }

  private boolean indexedPathsHaveCommonality(
      HashMap<List<String>, HashMap<BatchMetadataSearchParam, List<Integer>>> commonPathMap) {
    for (var fields : commonPathMap.entrySet()) {
      var firstList = fields.getValue().entrySet().iterator().next().getValue();
      HashSet<List<Integer>> commonList = new HashSet<>(List.of(firstList));
      for (var indexMap : fields.getValue().entrySet()){
        commonList.retainAll(List.of(indexMap.getValue()));
      }
      if (commonList.isEmpty()){
        return false;
      }
    }
    return true;
  }


  /*
  Looks at field names that are arrays (i.e. meet the criteria of arrayFieldPattern) and collects
  all the indexes of that array. E.g. given the jsonpath specimen.occurrences.1.locality.2, we get
  [occurrences] -> searchParam, [1],
  [occurrences, locality] -> searchParam, [1, 2]
   */
  private void indexArrayPaths(BatchMetadataSearchParam searchParam, HashSet<String> jsonPaths,
      HashMap<List<String>, HashMap<BatchMetadataSearchParam, List<Integer>>> commonPathMap) {
    var fieldIdxs = new ArrayList<Pair<List<String>, List<Integer>>>();
    for (var jsonPath : jsonPaths) {
      var matcher = arrayFieldPattern.matcher(jsonPath);
      var matches = new ArrayList<String>();
      while (matcher.find()) {
        matches.add(matcher.group());
      }
      var matchItr = matches.listIterator();
      while (matchItr.hasNext()) {
        var match = matchItr.next();
        var fieldName = match.replaceAll("\\PL+", "");
        var idx = Integer.valueOf(match.replaceAll("\\D", ""));
        fieldIdxs.add(Pair.of(List.of(fieldName), List.of(idx)));
        if (matchItr.hasPrevious()) {
          compoundPreviousElements(fieldIdxs);
        }
      }
    }
    var idxMap = fieldIdxs.stream()
        .collect(Collectors.groupingBy(Pair::getKey,
            Collectors.mapping(Pair::getValue, Collectors.toList())));
    compoundIndexMap(idxMap, searchParam, commonPathMap);
  }

  private void compoundIndexMap(Map<List<String>, List<List<Integer>>> idxMap,
      BatchMetadataSearchParam searchParam,
      HashMap<List<String>, HashMap<BatchMetadataSearchParam, List<Integer>>> commonPathMap) {
    for (var fields : idxMap.entrySet()) {
      if (commonPathMap.containsKey(fields.getKey())) {
        commonPathMap.get(fields.getKey())
            .put(searchParam, fields.getValue().stream().flatMap(List::stream).toList());
      } else {
        commonPathMap.put(fields.getKey(), new HashMap<>(
            Map.of(searchParam, fields.getValue().stream().flatMap(List::stream).toList())));
      }
    }

  }

  private void compoundPreviousElements(List<Pair<List<String>, List<Integer>>> fieldIdxs) {
    var fieldList = fieldIdxs.stream().map(Pair::getLeft).flatMap(List::stream).toList();
    var idxList = fieldIdxs.stream().map(Pair::getRight).flatMap(List::stream).toList();
    fieldIdxs.add(Pair.of(fieldList, idxList));
  }

  /*

  private HashSet<String> findCommonPaths
      (Set<String> param1Paths, Set<String> param2Paths, String commonPath) {
    // occurrence[A].locality[B].city
    // occurrence[X].citations[Y].author
    // Assert A = X while ignoring B, Y
    var param1Subpaths = param1Paths.stream()
        .map(p -> p.substring(0, findCommonPathTerminus(commonPath, p)))
        .collect(Collectors.toSet());
    var param2Subpaths = param2Paths.stream()
        .map(p -> p.substring(0, findCommonPathTerminus(commonPath, p)))
        .collect(Collectors.toSet());
    var setJoin = new HashSet<>(param2Subpaths);
    setJoin.retainAll(param1Subpaths);
    return setJoin;
  }
  private int findCommonPathTerminus(String commonPath, String searchPath) {
    var starCount = commonPath.length() - commonPath.replaceAll("[*]", "").length();
    var digitCount = searchPath.length() - searchPath.replaceAll("\\d", "").length();
    return commonPath.length() + digitCount - starCount;
  }
   */

}
