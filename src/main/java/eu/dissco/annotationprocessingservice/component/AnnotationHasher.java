package eu.dissco.annotationprocessingservice.component;

import eu.dissco.annotationprocessingservice.domain.SelectorType;
import eu.dissco.annotationprocessingservice.schema.Annotation;
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

  private static String getAnnotationHashString(Annotation annotation) {
    String targetString = null;
    var selector = annotation.getOaHasTarget().getOaHasSelector().getAdditionalProperties();
    var selectorType = SelectorType.fromString((String) selector.get("@type"));
    switch (selectorType) {
      case FIELD_SELECTOR -> targetString = (String) selector.get("ods:field");
      case FRAGMENT_SELECTOR -> targetString = selector.get("oa:hasRoi").toString();
      case CLASS_SELECTOR -> targetString = (String) selector.get("ods:class");
    }

    return annotation.getOaHasTarget().getId() + "-" + targetString + "-" +
        annotation.getDctermsCreator().getId() + "-" + annotation.getOaMotivation().value();
  }

  public UUID getAnnotationHash(Annotation annotation) {
    var annotationString = getAnnotationHashString(annotation);
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
