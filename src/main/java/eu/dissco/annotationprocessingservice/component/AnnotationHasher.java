package eu.dissco.annotationprocessingservice.component;

import eu.dissco.annotationprocessingservice.domain.SelectorType;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@RequiredArgsConstructor
@Component
public class AnnotationHasher {

  private final MessageDigest messageDigest;

  private static UUID buildUuidFromHash(String hash) {
    return UUID.fromString(hash.replaceFirst(
        "(\\p{XDigit}{8})(\\p{XDigit}{4})(\\p{XDigit}{4})(\\p{XDigit}{4})(\\p{XDigit}+)",
        "$1-$2-$3-$4-$5"
    ));
  }

  private static String getAnnotationHashString(AnnotationProcessingRequest annotationRequest, boolean addValue) {
    String targetString = null;
    var selector = annotationRequest.getOaHasTarget().getOaHasSelector().getAdditionalProperties();
    var selectorType = SelectorType.fromString((String) selector.get("@type"));
    switch (selectorType) {
      case FIELD_SELECTOR -> targetString = (String) selector.get("ods:term");
      case FRAGMENT_SELECTOR -> targetString = selector.get("ac:hasROI").toString();
      case CLASS_SELECTOR -> targetString = (String) selector.get("ods:class");
    }
    var stringToHash = new StringBuilder()
        .append(annotationRequest.getOaHasTarget().getId())
        .append("-")
        .append(targetString)
        .append("-")
        .append(annotationRequest.getDctermsCreator().getId())
        .append("-")
        .append(annotationRequest.getOaMotivation().value());
    if (addValue && annotationRequest.getOaHasBody() != null) {
      stringToHash = stringToHash
          .append("-")
          .append(annotationRequest.getOaHasBody().getOaValue().getFirst());
    }
    return stringToHash.toString();
  }

  public UUID getAnnotationHash(AnnotationProcessingRequest annotationRequest, boolean addValue) {
    var annotationString = getAnnotationHashString(annotationRequest, addValue);
    var annotationHash = hashAnnotation(annotationString);
    return buildUuidFromHash(annotationHash);
  }

  private String hashAnnotation(String annotationString) {
    var hexString = new StringBuilder();
    messageDigest.update(annotationString.getBytes(StandardCharsets.UTF_8));
    var digest = messageDigest.digest();
    for (var b : digest) {
      hexString.append(String.format("%02x", b));
    }
    return hexString.toString();
  }

}
