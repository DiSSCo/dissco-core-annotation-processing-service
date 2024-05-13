package eu.dissco.annotationprocessingservice.component;

import static com.jayway.jsonpath.Criteria.where;
import static com.jayway.jsonpath.Filter.filter;
import static com.jayway.jsonpath.JsonPath.using;

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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class JsonPathComponent {

  private final ObjectMapper mapper;
  private final Configuration jsonPathConfig;

  // Identifies the last field in a dot notation pattern
  private final Pattern lastKeyPattern = Pattern.compile("[^.]+(?=\\.$)|([^.]+$)");
  // Identifies array fields in a mixed notation pattern
  private final Pattern arrayFieldPattern = Pattern.compile("(\\w+\\.\\d+)");
  private final Pattern dotIndexPatternFirst = Pattern.compile("\\.\\d");
  private final Pattern dotIndexPatternLast = Pattern.compile("\\d\\.");


  public List<Target> getAnnotationTargetsExtended(BatchMetadataExtended batchMetadata,
      JsonNode annotatedObject, Target baseTarget) {
    var commonIndexes = new HashMap<List<String>, List<List<Integer>>>();
    DocumentContext context;
    try {
      context = using(jsonPathConfig).parse(mapper.writeValueAsString(annotatedObject));
    } catch (JsonProcessingException e) {
      log.error("Unable to read jsonPath", e);
      throw new BatchingRuntimeException();
    }
    if (!isTrueMatch(batchMetadata, commonIndexes, context)) {
      log.warn("False positive detected");
      log.info("{} does not comply to batch metadata {}", annotatedObject, batchMetadata);
      return Collections.emptyList();
    }
    var targetPaths = getAnnotationTargetPaths(commonIndexes, baseTarget, context);
    return buildOaTargets(targetPaths, baseTarget, annotatedObject.get("id").asText());
  }


  private List<String> getAnnotationTargetPaths(
      Map<List<String>, List<List<Integer>>> commonIndexes, Target baseTarget,
      DocumentContext context)
      throws BatchingRuntimeException {
    var baseTargetPath = getTargetPath(baseTarget);
    var matcher = arrayFieldPattern.matcher(baseTargetPath);
    var arrayFields = new ArrayList<String>();
    while (matcher.find()) {
      var match = matcher.group();
      arrayFields.add(match.replaceAll("\\P{L}+", ""));
    }
    if (arrayFields.isEmpty()) {
      return List.of(baseTargetPath);
    }
    var modifiedPath = "$." + baseTargetPath.replaceAll("\\d+", "*");
    ArrayList<String> jsonPaths = context.read(modifiedPath);
    if (!commonIndexes.isEmpty()) {
      var invalidPaths = new ArrayList<String>();
      var indexedPath = findTargetPathIndexes(arrayFields, commonIndexes);
      for (var jsonPath : jsonPaths) {
        if (!isValidJsonPath(toDotNotation(jsonPath), indexedPath)) {
          invalidPaths.add(jsonPath);
        }
      }
      jsonPaths.removeAll(invalidPaths);
    }
    return jsonPaths.stream()
        .map(this::toDotNotation)
        .map(this::toMixedNotation)
        .toList();
  }


  /*
  We found "commonInputIndexes" when we were checking if this annotated object was a true match
  We want to find the longest sequence of indexes we have for our target path
  Given 2 input parameters:
      occurrence[A]locality[B]city = paris  -> valid indexes [1, 2], [1, 3] (A = 1, B = 2 and 3)
      occurrence[C] = found at night        -> valid indexes [1] (C = 1)
  Given the targetPath:
      occurrence[X]locality[Y]country
   We want to find the values of X and Y in our target path. Only the indexes

   We find our indexes by comparing the array fields in the target path (targetArrays) with the array
   fields in the input paths. The indexed input with the greatest number of array fields in common
   with our target path will be used to make our final indexed target path
   */
  private Pair<List<String>, List<List<Integer>>> findTargetPathIndexes(List<String> targetArrays,
      Map<List<String>, List<List<Integer>>> commonInputIndexes) {
    int highestMatches = 0;
    Entry<List<String>, List<List<Integer>>> bestMatch = null;
    var matchedFields = new ArrayList<String>();
    for (var commonIndex : commonInputIndexes.entrySet()) {
      var inputArrays = commonIndex.getKey();
      matchedFields = new ArrayList<>();
      for (int i = 0; i < targetArrays.size(); i++) {
        if (i > inputArrays.size()-1 || !targetArrays.get(i).equals(inputArrays.get(i))) {
          break;
        }
        matchedFields.add(targetArrays.get(i));
      }
      if (matchedFields.size() > highestMatches) {
        highestMatches = matchedFields.size();
        bestMatch = commonIndex;
      }
    }
    if (bestMatch != null) {
      var maxMatches = highestMatches;
      var idxList = bestMatch.getValue().stream().map(l -> l.subList(0, maxMatches)).toList();
      return Pair.of(matchedFields, idxList);
    }
    return Pair.of(Collections.emptyList(), Collections.emptyList());
  }

  // Takes the found json path and compares it to indexes that met the criteria of the input parameters

  boolean isValidJsonPath(String jsonPath, List<FieldIndex> validInputIndexes) {
    var targetArrayFieldMatcher = arrayFieldPattern.matcher(jsonPath);
    var foundIndexesInTargetJsonPath = new ArrayList<Integer>();
    while (targetArrayFieldMatcher.find()) {
      var match = targetArrayFieldMatcher.group();
      // only get index of json path arrays
      foundIndexesInTargetJsonPath.add(Integer.parseInt(match.replaceAll("\\D+", "")));
    }
    // We take sublists because we may be using fewer indexes than the target path has
    // e.g. if the target path has occurrence[*]locality[*], but we only used occurrence[*] in our input parameters
    // Then we would only be able to look at the indexes for occurrence
    // If this is the case, we log the ambiguity but annotate all localities
    var targetIndexSublist = foundIndexesInTargetJsonPath
        .subList(0, validInputIndexes.size());
    return (validInputIndexes.stream().map(FieldIndex::indexes).toList().contains(targetIndexSublist));
  }


  boolean isValidJsonPath(String jsonPath, Pair<List<String>, List<List<Integer>>> validInputIndexes) {
    var targetArrayFieldMatcher = arrayFieldPattern.matcher(jsonPath);
    var foundIndexesInTargetJsonPath = new ArrayList<Integer>();
    while (targetArrayFieldMatcher.find()) {
      var match = targetArrayFieldMatcher.group();
      // only get index of json path arrays
      foundIndexesInTargetJsonPath.add(Integer.parseInt(match.replaceAll("\\D+", "")));
    }
    // We take sublists because we may be using fewer indexes than the target path has
    // e.g. if the target path has occurrence[*]locality[*], but we only used occurrence[*] in our input parameters
    // Then we would only be able to look at the indexes for occurrence
    // If this is the case, we log the ambiguity but annotate all localities
    var targetIndexSublist = foundIndexesInTargetJsonPath
        .subList(0, validInputIndexes.getRight().get(0).size());
    return (validInputIndexes.getRight().contains(targetIndexSublist));
  }

  public List<Target> getAnnotationTargets(BatchMetadata batchMetadata, JsonNode annotatedObject,
      Target baseTarget) throws JsonProcessingException, BatchingException {
    var context = using(jsonPathConfig).parse(mapper.writeValueAsString(annotatedObject));
    var filter = generateFilter(batchMetadata);
    List<String> correctJsonInputPaths = null;
    try {
      correctJsonInputPaths = context.read(
          removeLastKey(batchMetadata.inputField()), filter);
    } catch (JsonPathException e) {
      log.error("Poorly formatted json path", e);
      throw new BatchingException();
    }
    var correctJsonInputPathsDot = correctJsonInputPaths.stream()
        .map(this::toDotNotation).toList();
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
      newTargets.add(Target.builder()
          .odsType(baseTarget.getOdsType())
          .odsId("https://doi.org/" + newTargetId)
          .oaSelector(newSelector)
          .build());
    }
    return newTargets;
  }

  private String getTargetPath(Target baseTarget) throws BatchingRuntimeException {
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
        log.error("Unable to batch annotations with selector type {}", selectorType);
        throw new BatchingRuntimeException();
      }
    }
  }

  private Filter generateFilter(BatchMetadata batchMetadata) {
    var targetField = getLastKey(batchMetadata.inputField());
    var targetValue = batchMetadata.inputValue();
    return filter(where(targetField).is(targetValue));
  }

  private Filter generateFilter(BatchMetadataSearchParam batchMetadata) {
    var targetField = getLastKey(batchMetadata.inputField());
    var targetValue = batchMetadata.inputValue();
    return filter(where(targetField).is(targetValue));
  }

  private String getLastKey(String jsonPath) {
    var lastKeyMatcher = lastKeyPattern.matcher(jsonPath);
    if (lastKeyMatcher.find()) {
      return lastKeyMatcher.group().replace("\\.", "");
    } else {
      log.error("Unable to parse last key of jsonPath {}", jsonPath);
      throw new BatchingRuntimeException();
    }
  }

  String removeLastKey(String jsonPath) {
    jsonPath = jsonPath.replaceAll(lastKeyPattern.pattern(), "");
    return removeTrailingPeriod(jsonPath) + "[?]";
  }

  private List<String> iterateOverList(List<String> correctJsonInputPaths, String baseTargetPath) {
    /*
    - `baseTargetPath`: the target fields or class of the original annotation
    - `correctJsonInputPaths`: JSON paths corresponding to input fields that contain the same value used by the annotating MAS to create the original annotation.

    - In this method, we examine the indexes within `correctJsonInputPaths` to construct the target fields/class of the new annotation.
    - For example, if "occurrence.1.georeference.latitude" is a correct input JSON path, it signifies that the value stored at this path is identical to the value used by the MAS to create the original annotation.
    - If "occurrence.3.locality" is found in the `baseTargetPath` (indicating that it is the fields that was annotated), we need to update the indexes of the occurrence array.
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
    // Regex that captures the fields name plus next 3 characters (i.e. fields name plus indexes)
    var replaceThis = "(" + fieldName + ".{3})";
    var withThis = fieldName + "[" + index + "].";
    return targetPath.replaceAll(replaceThis, withThis);
  }

  private String toDotNotation(String jsonPath) {
    // We use dot notation to split jsonPaths, but our jsonPath library will only output in bracket notation
    // From: "[fields][1][otherField]" or "fields[1].otherfield"
    // To: "fields.1.otherField"

    jsonPath = jsonPath
        .replace("$", "")
        .replaceAll("\\[(?=[*|\\d])", ".") // Captures [ next to * or 1-9, replace with .
        .replaceAll("\\[(?![*|\\d])", "")  // Captures [ next to all other characters, removes
        .replace("]", ".")
        .replace("..", ".")
        .replace("'", "");
    return removeTrailingPeriod(jsonPath);
  }

  // From: "fields.1.otherField"
  // To: "fields[1].otherField"
  private String toMixedNotation(String jsonPath) {
    var matcher1 = dotIndexPatternFirst.matcher(jsonPath);
    var matcher2 = dotIndexPatternLast.matcher(jsonPath);
    var digitPattern = Pattern.compile("\\d");
    while (matcher1.find()) {
      var match = matcher1.group();
      var indexMatcher = digitPattern.matcher(match);
      indexMatcher.find();
      jsonPath = jsonPath.replace(match, "[" + indexMatcher.group());
    }
    while (matcher2.find()) {
      var match = matcher2.group();
      var indexMatcher = digitPattern.matcher(match);
      indexMatcher.find();
      jsonPath = jsonPath.replace(match, indexMatcher.group() + "].");
    }
    return removeTrailingPeriod(jsonPath);

  }

  private static String removeTrailingPeriod(String jsonPath) {
    var jsonPathArray = jsonPath.split("\\.");
    return String.join(".", jsonPathArray);
  }

  /*
  Checks if a given elastic result is a true match - meaning all the criteria in the batchMetadata are met
  1. For each parameter in the batch metadata, find all jsonPaths that match that parameter
  2. Index array paths: collect the indexes of any arrays in the valid json paths (step 1).
  Group array indexes by name of field(s) containing arrays, and the parameter these indexes meet.
  3. Check that there is at least one common sequence of indexes that fulfills each batch parameter (that has a common array field)
   */
  private boolean isTrueMatch(BatchMetadataExtended batchMetadata,
      HashMap<List<String>, List<List<Integer>>> commonIndexes, DocumentContext context) {
    HashMap<List<String>, HashMap<BatchMetadataSearchParam, ArrayList<List<Integer>>>> indexedPaths = new HashMap<>();
    for (var param : batchMetadata.searchParams()) {
      var validPaths = new HashSet<String>(
          context.read(removeLastKey(param.inputField()), generateFilter(param)))
          .stream().map(this::toDotNotation)
          .collect(Collectors.toCollection(HashSet::new));
      var newIndexedPaths = indexArrayPaths(param, validPaths);
      mergePathMaps(indexedPaths, newIndexedPaths);
    }
    return indexedPathsHaveCommonality(indexedPaths, commonIndexes);
  }

  private void mergePathMaps(
      HashMap<List<String>, HashMap<BatchMetadataSearchParam, ArrayList<List<Integer>>>> indexedPaths,
      HashMap<List<String>, HashMap<BatchMetadataSearchParam, ArrayList<List<Integer>>>> newPaths) {
    for (var newIndexedPaths : newPaths.entrySet()) {
      if (indexedPaths.containsKey(newIndexedPaths.getKey())) {
        indexedPaths.get(newIndexedPaths.getKey())
            .putAll(newIndexedPaths.getValue());
      } else {
        indexedPaths.put(newIndexedPaths.getKey(), newIndexedPaths.getValue());
      }
    }
  }

  /*
  Looks through all fields and indexes to determine if there is a common path
  Lets say we have 3 parameters: param1, param2, param3

  occurrence  -> param1 -> [[1]]
              -> param2 -> [[1, 2]]
              -> param3 -> [[1, 3]]
  occurrence, locality -> param1 -> [[1, 1], [1, 2]] // values in array occurrence[1]loc[1], occurrence[1]locality[2] meet param1
                       -> param2 -> [[1, 1], [2, 1]] // values in array occurrence[1]loc[1], occurrence[2]locality[1] meet param2
  occurrence, citation -> param3 -> [[1, 4], [3, 3]] // values in array occurrence[1]citation[4] and occurrence[3]citation[3] meet param 3

  In the above example, occurrence[1] meets param1, param2, param3. Additionally, locality[1] in occurrence[1] meets param1 and param2
  By looking at the common paths, we can determine if this is a true match.
   */

  private boolean indexedPathsHaveCommonality(
      HashMap<List<String>, HashMap<BatchMetadataSearchParam, ArrayList<List<Integer>>>> commonPathMap,
      HashMap<List<String>, List<List<Integer>>> commonIndexes) {
    for (var fields : commonPathMap.entrySet()) {
      var firstList = fields.getValue().entrySet().iterator().next().getValue();
      var commonSet = new HashSet<>(firstList);
      for (var indexMap : fields.getValue().entrySet()) {
        commonSet.retainAll(indexMap.getValue());
      }
      if (commonSet.isEmpty()) {
        return false;
      }
      commonIndexes.put(fields.getKey(), new ArrayList<>(commonSet));
    }
    return true;
  }

  /*
  Looks at fields names that are arrays (i.e. meet the criteria of arrayFieldPattern) and collects
  all the indexes of that array. E.g. given the jsonpath specimen.occurrences.1.locality.2, we get
  [occurrences] -> searchParam, [1],
  [occurrences, locality] -> searchParam, [1, 2]
   */
  private HashMap<List<String>, HashMap<BatchMetadataSearchParam, ArrayList<List<Integer>>>> indexArrayPaths(
      BatchMetadataSearchParam searchParam, HashSet<String> jsonPaths) {
    var fieldIdxs = new HashMap<String, List<FieldIndex>>();
    for (var jsonPath : jsonPaths) {
      var indexPairs = new ArrayList<FieldIndex>();
      var matcher = arrayFieldPattern.matcher(jsonPath);
      while (matcher.find()) {
        var match = matcher.group();
        var fieldName = match.replaceAll("\\P{L}+", "");
        var idx = Integer.valueOf(match.replaceAll("\\D", ""));
        indexPairs.add(new FieldIndex(List.of(fieldName), List.of(idx)));
      }
      fieldIdxs.put(jsonPath, compoundPreviousFieldIndex(indexPairs));
    }
    return mapCommonPaths(fieldIdxs, searchParam);
  }


  /*
  Takes List<FieldIndex> and compounds the list of field names and indexes with previous elements
  Input:
    {
      fieldIndex(["A"], [1]),
      fieldIndex(["B"], [2]),
      fieldIndex(["C"], [3])
    }
   Output:
     {
      fieldIndex(["A"], [1]),
      fieldIndex(["A", "B"], [1, 2]),
      fieldIndex(["A", "B", "C"], [1, 2, 3])
    }
   */
  private ArrayList<FieldIndex> compoundPreviousFieldIndex(ArrayList<FieldIndex> lst) {
    if (lst.size() <= 1) {
      return lst;
    }
    var previousCompound = compoundPreviousFieldIndex(
        new ArrayList<>(lst.subList(0, lst.size() - 1)));
    FieldIndex currentFieldIndex = lst.get(lst.size() - 1);
    var compoundedField = new ArrayList<>(
        previousCompound.get(previousCompound.size() - 1).fields());
    var compoundedIndex = new ArrayList<>(
        previousCompound.get(previousCompound.size() - 1).indexes());
    compoundedField.addAll(currentFieldIndex.fields());
    compoundedIndex.addAll(currentFieldIndex.indexes());
    previousCompound.add(new FieldIndex(compoundedField, compoundedIndex));
    return previousCompound;
  }

  private HashMap<List<String>, HashMap<BatchMetadataSearchParam, ArrayList<List<Integer>>>> mapCommonPaths(
      HashMap<String, List<FieldIndex>> idxMap,
      BatchMetadataSearchParam searchParam) {
    var commonPathMap = new HashMap<List<String>, HashMap<BatchMetadataSearchParam, ArrayList<List<Integer>>>>();
    for (var fieldGroups : idxMap.entrySet()) {
      for (var fieldList : fieldGroups.getValue()) {
        if (commonPathMap.containsKey(fieldList.fields)) {
          commonPathMap.get(fieldList.fields).get(searchParam).add(fieldList.indexes);
        } else {
          commonPathMap.put(fieldList.fields,
              new HashMap<>(Map.of(searchParam, new ArrayList<>(List.of(fieldList.indexes)))));
        }
      }
    }
    return commonPathMap;
  }

  record FieldIndex(
      List<String> fields,
      List<Integer> indexes
  ) {

  }
}
