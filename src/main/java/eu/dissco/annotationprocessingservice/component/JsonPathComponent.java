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
import com.jayway.jsonpath.InvalidPathException;
import eu.dissco.annotationprocessingservice.domain.SelectorType;
import eu.dissco.annotationprocessingservice.exception.BatchingException;
import eu.dissco.annotationprocessingservice.exception.BatchingRuntimeException;
import eu.dissco.annotationprocessingservice.schema.AnnotationBatchMetadata;
import eu.dissco.annotationprocessingservice.schema.AnnotationTarget;
import eu.dissco.annotationprocessingservice.schema.OaHasSelector;
import eu.dissco.annotationprocessingservice.schema.SearchParam;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class JsonPathComponent {

  private static final String TYPE = "@type";

  @Qualifier("objectMapper")
  private final ObjectMapper mapper;
  @Qualifier("localJsonPath")
  private final Configuration jsonPathConfig;

  private final Pattern lastKeyPattern = Pattern.compile("\\[(?!.*\\[')(.*)");
  private final Pattern arrayFieldPattern = Pattern.compile("\\[([^]]+)]\\[\\d+]");

  public List<AnnotationTarget> getAnnotationTargets(AnnotationBatchMetadata batchMetadata,
      JsonNode annotatedObject, AnnotationTarget baseTarget) throws BatchingException {
    var commonIndexes = new HashMap<List<String>, List<List<Integer>>>();
    DocumentContext context;
    try {
      context = using(jsonPathConfig).parse(mapper.writeValueAsString(annotatedObject));
      if (!isTrueMatch(batchMetadata, commonIndexes, context)) {
        log.info("False positive detected: {} does not comply to batch metadata {}",
            annotatedObject, batchMetadata);
        return Collections.emptyList();
      }
      var targetPaths = getAnnotationTargetPaths(commonIndexes, baseTarget, context);
      return buildOaTargets(targetPaths, baseTarget, annotatedObject.get("@id").asText());
    } catch (JsonProcessingException | InvalidPathException e) {
      log.error("Unable to read jsonPath", e);
      throw new BatchingException();
    }
  }

  private List<String> getAnnotationTargetPaths(
      Map<List<String>, List<List<Integer>>> commonIndexes, AnnotationTarget baseTarget,
      DocumentContext context) {
    var baseTargetPath = getTargetPath(baseTarget);
    var matcher = arrayFieldPattern.matcher(baseTargetPath);
    var arrayFields = new ArrayList<String>();
    while (matcher.find()) {
      var match = matcher.group();
      var bareField = match
          .replaceAll("[\\[\\]\\d+]", "")
          .replace("'", "");
      arrayFields.add(bareField); // remove square
    }
    // If there are no arrays are in the target field, we can use what is in the base hashedAnnotation
    if (arrayFields.isEmpty()) {
      return List.of(baseTargetPath);
    }
    var readablePath = baseTargetPath.replaceAll("\\d+", "*");
    ArrayList<String> jsonPaths = context.read(readablePath);
    removeInvalidPaths(jsonPaths, arrayFields, commonIndexes);
    return jsonPaths;
  }

  private void removeInvalidPaths(ArrayList<String> jsonPaths, List<String> arrayFields,
      Map<List<String>, List<List<Integer>>> commonIndexes) {
    if (commonIndexes.isEmpty()) {
      return;
    }
    var invalidPaths = new ArrayList<String>();
    var indexedPathOptional = findTargetPathIndexes(arrayFields, commonIndexes);
    if (indexedPathOptional.isPresent()) {
      for (var jsonPath : jsonPaths) {
        if (!isValidJsonPath(jsonPath, indexedPathOptional.get())) {
          invalidPaths.add(jsonPath);
        }
      }
      jsonPaths.removeAll(invalidPaths);
    }
  }

  /*
  We found "commonInputIndexes" when we were checking if this annotated object was a true match
  We want to find the longest sequence of indexes we have for our target path
  Given 2 input parameters:
      occurrence[A]locality[B]city = paris  -> valid indexes [1, 2], [1, 3] (A = 1, B = 2 and 3)
      occurrence[C] = found at night        -> valid indexes [1] (C = 1)
  Given the targetPath:
      occurrence[X]locality[Y]country
   We want to find the values of X and Y in our target path. Only the indexes that are in both paths
   meet the criteria for both paths.

   We find our indexes by comparing the array fields in the target path (targetArrays) with the array
   fields in the input paths. The indexed input with the greatest number of array fields in common
   with our target path will be used to make our final indexed target path
   */
  private Optional<Pair<List<String>, List<List<Integer>>>> findTargetPathIndexes(
      List<String> targetArrays,
      Map<List<String>, List<List<Integer>>> commonInputIndexes) {
    int highestMatches = 0;
    Entry<List<String>, List<List<Integer>>> bestMatch = null;
    var matchedFields = new ArrayList<String>();
    for (var commonIndex : commonInputIndexes.entrySet()) {
      var inputArrays = commonIndex.getKey();
      matchedFields = new ArrayList<>();
      for (int i = 0; i < targetArrays.size(); i++) {
        if (i > inputArrays.size() - 1 || !targetArrays.get(i).equals(inputArrays.get(i))) {
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
      return Optional.of(Pair.of(matchedFields, idxList));
    }
    return Optional.empty();
  }

  boolean isValidJsonPath(String jsonPath,
      Pair<List<String>, List<List<Integer>>> validInputIndexes) {
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

  private List<AnnotationTarget> buildOaTargets(List<String> targetPaths,
      AnnotationTarget baseTarget,
      String newTargetId) {
    boolean isClassSelector = baseTarget.getOaHasSelector().getAdditionalProperties().get(TYPE)
        .equals(SelectorType.CLASS_SELECTOR.toString());
    var newTargets = new ArrayList<AnnotationTarget>();
    for (var targetPath : targetPaths) {
      var selector = new OaHasSelector();
      if (isClassSelector) {
        selector.setAdditionalProperty(TYPE, "ods:ClassSelector");
        selector.setAdditionalProperty("ods:class", targetPath);
      } else {
        selector.setAdditionalProperty(TYPE, "ods:TermSelector");
        selector.setAdditionalProperty("ods:term", targetPath);
      }
      newTargets.add(new AnnotationTarget()
          .withOdsFdoType(baseTarget.getOdsFdoType())
          .withType(baseTarget.getType())
          .withDctermsIdentifier(newTargetId)
          .withId(newTargetId)
          .withOaHasSelector(selector));
    }
    return newTargets;
  }

  private String getTargetPath(AnnotationTarget baseTarget) throws BatchingRuntimeException {
    var selector = baseTarget.getOaHasSelector().getAdditionalProperties();
    var selectorType = SelectorType.fromString((String) selector.get(TYPE));
    switch (selectorType) {
      case CLASS_SELECTOR -> {
        return selector.get("ods:class").toString();
      }
      case FIELD_SELECTOR -> {
        return selector.get("ods:term").toString();
      }
      default -> {
        log.error("Unable to batch annotations with selector type {}", selectorType);
        throw new BatchingRuntimeException();
      }
    }
  }

  private Filter generateFilter(SearchParam batchMetadata) {
    var targetField = getLastKey(batchMetadata.getInputField());
    var targetValue = batchMetadata.getInputValue();
    return filter(where(targetField).is(targetValue));
  }

  private String getLastKey(String jsonPath) {
    if (jsonPath.contains("[")) {
      var lastKeyMatcher = lastKeyPattern.matcher(jsonPath);
      lastKeyMatcher.find();
      var lastKey = lastKeyMatcher.group()
          .replace("\\.", "")
          .replaceAll("[\\d+]", "[*]");
      if (lastKey.length() < jsonPath.length()) {
        return lastKey;
      } else {
        log.error("Unable to parse last key of jsonPath {}", jsonPath);
        throw new BatchingRuntimeException();
      }
    } else {
      return jsonPath;
    }
  }

  String removeLastKey(String jsonPath) {
    return jsonPath.replaceAll(lastKeyPattern.pattern(), "[?]");
  }

  /*
  Checks if a given elastic result is a true match - meaning all the criteria in the annotationBatchMetadata are met
  1. For each parameter in the batch metadata, find all jsonPaths that match that parameter
  2. Index array paths: collect the indexes of any arrays in the valid json paths (step 1).
  Group array indexes by name of field(s) containing arrays, and the parameter these indexes meet.
  3. Check that there is at least one common sequence of indexes that fulfills each batch parameter (that has a common array field)
   */
  private boolean isTrueMatch(AnnotationBatchMetadata batchMetadata,
      HashMap<List<String>, List<List<Integer>>> commonIndexes, DocumentContext context) {
    HashMap<List<String>, HashMap<SearchParam, ArrayList<List<Integer>>>> indexedPaths = new HashMap<>();
    for (var param : batchMetadata.getSearchParams()) {
      var validPaths = new HashSet<String>(
          context.read(removeLastKey(param.getInputField()), generateFilter(param)));
      var newIndexedPaths = indexArrayPaths(validPaths);
      mergePathMaps(indexedPaths, newIndexedPaths, param);
    }
    return indexedPathsHaveCommonality(indexedPaths, commonIndexes);
  }

  /*
  Looks at fields names that are arrays (i.e. meet the criteria of arrayFieldPattern) and collects
  all the indexes of that array. E.g. given the jsonpath specimen.occurrences.1.locality.2, we get
  [occurrences] -> [1],
  [occurrences, locality] -> [1, 2]

  We need this "compounded list" in order to later compare array indexes with later paths.
  e.g. Let's say we also have the jsonPath specimen.occurrences.2.georeference.2, which breaks down
  into the Map:
  [occurrences] -> [2],
  [occurrences, georeference] -> [2, 2]

  We can more easily see the "occurrences" field has no common index between these two paths by looking
  at the first entry of the map instead of the second entry.
   */
  private Map<List<String>, List<List<Integer>>> indexArrayPaths(HashSet<String> jsonPaths) {
    var indexList = new ArrayList<FieldIndex>();
    for (var jsonPath : jsonPaths) {
      var indexListForThisPath = new ArrayList<FieldIndex>();
      var matcher = arrayFieldPattern.matcher(jsonPath);
      while (matcher.find()) {
        var match = matcher.group();
        var fieldName = match.replaceAll("\\[|]|\\d+|'|\\*", "");
        var idx = Integer.valueOf(match.replaceAll("\\D", ""));
        indexListForThisPath.add(new FieldIndex(List.of(fieldName), List.of(idx)));
      }
      indexList.addAll(compoundPreviousFieldIndex(indexListForThisPath));
    }
    return mergeFieldIndexesForSingleParameter(indexList);
  }

  private void mergePathMaps(
      HashMap<List<String>, HashMap<SearchParam,
          ArrayList<List<Integer>>>> indexedPaths,
      Map<List<String>, List<List<Integer>>> newPaths, SearchParam param) {
    for (var newIndexedPaths : newPaths.entrySet()) {
      if (indexedPaths.containsKey(newIndexedPaths.getKey())) {
        indexedPaths.get(newIndexedPaths.getKey())
            .put(param, new ArrayList<>(newIndexedPaths.getValue()));
      } else {
        indexedPaths.put(newIndexedPaths.getKey(),
            new HashMap<>(Map.of(param, new ArrayList<>(newIndexedPaths.getValue()))));
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
      HashMap<List<String>, HashMap<SearchParam, ArrayList<List<Integer>>>> commonPathMap,
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

  // Formatting functions

  private Map<List<String>, List<List<Integer>>> mergeFieldIndexesForSingleParameter(
      List<FieldIndex> fieldIndexList) {
    var commonIndexes = new HashMap<List<String>, List<List<Integer>>>();
    for (var fieldIndex : fieldIndexList) {
      if (commonIndexes.containsKey(fieldIndex.fields())) {
        commonIndexes.get(fieldIndex.fields()).add(fieldIndex.indexes());
      } else {
        commonIndexes.put(fieldIndex.fields(), new ArrayList<>(List.of(fieldIndex.indexes())));
      }
    }
    return commonIndexes;
  }

  /*
  Takes List<FieldIndex> and compounds the list of field names and indexes with previous elements
  Input:
      fieldIndex(["A"], [1]),
      fieldIndex(["B"], [2]),
      fieldIndex(["C"], [3])

   Output:
      fieldIndex(["A"], [1]),
      fieldIndex(["A", "B"], [1, 2]),
      fieldIndex(["A", "B", "C"], [1, 2, 3])
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

  /*
  Internal class useful for coupling field and index lists when creating "compound" lists, when we
  check if an object is a true match.
  */
  record FieldIndex(
      List<String> fields,
      List<Integer> indexes
  ) {

  }
}
