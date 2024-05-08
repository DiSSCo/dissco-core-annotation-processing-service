package eu.dissco.annotationprocessingservice.component;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GreatestCommonSubstringTest {

  private GreatestCommonSubstringComponent substringComponent;

  @BeforeEach
  void init() {
    substringComponent = new GreatestCommonSubstringComponent();
  }

  @Test
  void testFind() {
    var basePath = "digitalSpecimenWrapper.occurrences[*].location.dwc:country";
    var inputPath = "digitalSpecimenWrapper.occurrences[*].location.dwc:continent";
    var expected = "digitalSpecimenWrapper.occurrences[*].location";

    var result = substringComponent.findCommonPath(basePath, inputPath);

    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testFindEmpty() {
    var basePath = "digitalSpecimenWrapper.occurrences[*].location.dwc:country";
    var substring = "mediaObject";
    var result = substringComponent.findCommonPath(basePath, substring);
    assertThat(result).isEmpty();
  }

  @Test
  void testComparePaths() {
    var path1 = Set.of("digitalSpecimenWrapper.occurrences[1].location.dwc:country",
        "digitalSpecimenWrapper.occurrences[1112].location.dwc:country");
    var path2 = Set.of("digitalSpecimenWrapper.occurrences[1112].citations.author");
    var commonPath = substringComponent.findCommonPath(
        "digitalSpecimenWrapper.occurrences[*].location.dwc:country",
        "digitalSpecimenWrapper.occurrences[*].citations.author");

    var expected = Set.of("digitalSpecimenWrapper.occurrences[1112]");

    var result = comparePaths(path1, path2, commonPath);

    assertThat(result).isEqualTo(expected);
  }

  private int findCommonPathTerminus(String commonPath, String searchPath) {
    var starCount = commonPath.length() - commonPath.replaceAll("[*]", "").length();
    var digitCount = searchPath.length() - searchPath.replaceAll("\\d", "").length();
    return commonPath.length() + digitCount - starCount;
  }


  private Set<String> comparePaths(Set<String> param1Paths, Set<String> param2Paths,
      String commonPath) {
    // occurrence[A].locality[B].city
    // occurrence[X].citations[Y].author
    // Assert A = X while ignoring B, Y
    var param1Subpaths = param1Paths.stream().map(p -> p.substring(0, findCommonPathTerminus(commonPath, p))).collect(
        Collectors.toSet());
    var param2Subpaths = param2Paths.stream().map(p -> p.substring(0, findCommonPathTerminus(commonPath, p))).collect(
        Collectors.toSet());
    var setDifference = new HashSet<>(param2Subpaths);
    setDifference.retainAll(param1Subpaths);
    return setDifference;
  }

}
