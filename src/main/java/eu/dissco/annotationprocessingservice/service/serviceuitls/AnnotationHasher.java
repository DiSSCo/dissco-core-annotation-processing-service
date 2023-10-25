package eu.dissco.annotationprocessingservice.service.serviceuitls;

import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
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
    return annotation.getOaTarget().toString() + "-" + annotation.getOaCreator().getOdsId() + "-"
        + annotation.getOaMotivation().toString();
  }

}
