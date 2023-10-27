package eu.dissco.annotationprocessingservice.service.serviceuitls;

import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.domain.annotation.ClassValueSelector;
import eu.dissco.annotationprocessingservice.domain.annotation.FieldValueSelector;
import eu.dissco.annotationprocessingservice.domain.annotation.FragmentSelector;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

public class AnnotationHasher {

  private AnnotationHasher() {
  }

  public static UUID getAnnotationHash(Annotation annotation) {
    var annotationString = getAnnotationHashString(annotation);
    var annotationHash = hashAnnotation(annotationString);
    return buildUuidFromHash(annotationHash);
  }

  private static String hashAnnotation(String annotationString) {
    MessageDigest messageDigest = null;
    try {
      messageDigest = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("MD5 Algorithm not found");
    }

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
