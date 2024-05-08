package eu.dissco.annotationprocessingservice.component;

import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class GreatestCommonSubstringComponent {
  private final Pattern lastKeyPattern = Pattern.compile("([^.]+$)");



  public String findCommonPath(String basePath, String inputPath){
    var gcs = findGreatestCommonSubstring(basePath, inputPath);
    return gcs.isEmpty() ? "" : removeLastKey(gcs);
  }

  public String removeLastKey(String jsonPath) {
    jsonPath = jsonPath.replaceAll(lastKeyPattern.pattern(), "");
    return removeTrailingPeriod(jsonPath);
  }

  private static String removeTrailingPeriod(String jsonPath) {
    var jsonPathArray = jsonPath.split("\\.");
    return String.join(".", jsonPathArray);
  }


  private static String findGreatestCommonSubstring(String basePath, String inputPath) {
    String inputPathSubstr = "";
    for (int i = 1; i < inputPath.length(); i++) {
      inputPathSubstr = inputPath.substring(0, i);
      if (!basePath.contains(inputPathSubstr)){
        inputPathSubstr = inputPathSubstr.substring(0, i-1);
        break;
      }
    }
    return inputPathSubstr;
  }

}
