package eu.dissco.annotationprocessingservice.service.serviceuitls;

import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.domain.annotation.ClassValueSelector;
import eu.dissco.annotationprocessingservice.domain.annotation.FieldValueSelector;
import eu.dissco.annotationprocessingservice.domain.annotation.FragmentSelector;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.UUID;

@RequiredArgsConstructor
@Component
public class AnnotationHasher {

  private final MessageDigest messageDigest;


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

  private static UUID buildUuidFromHash(String hash) {
    return UUID.fromString(hash.replaceFirst(
        "(\\p{XDigit}{8})(\\p{XDigit}{4})(\\p{XDigit}{4})(\\p{XDigit}{4})(\\p{XDigit}+)",
        "$1-$2-$3-$4-$5"
    ));
  }

  private static String getAnnotationHashString(Annotation annotation) {
    String targetString = null;
    var selector = annotation.getOaTarget().getOaSelector();
    switch (selector.getOdsType()){
      case FIELD_VALUE_SELECTOR -> targetString = ((FieldValueSelector) selector).getOdsField();
      case FRAGMENT_SELECTOR -> targetString = ((FragmentSelector) selector).getAcHasRoi().toString();
      case CLASS_VALUE_SELECTOR -> targetString = ((ClassValueSelector) selector).getOdsClass();
    }

    return annotation.getOaTarget().getOdsId() + "-" + targetString + "-" +
        annotation.getOaCreator().getOdsId() + "-" + annotation.getOaMotivation().toString();
  }

}
