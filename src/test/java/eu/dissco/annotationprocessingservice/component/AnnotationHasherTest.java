package eu.dissco.annotationprocessingservice.component;

import static eu.dissco.annotationprocessingservice.TestUtils.ANNOTATION_HASH;
import static eu.dissco.annotationprocessingservice.TestUtils.HANDLE_PROXY;
import static eu.dissco.annotationprocessingservice.TestUtils.TARGET_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenOaTarget;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import eu.dissco.annotationprocessingservice.domain.AnnotationTargetType;
import eu.dissco.annotationprocessingservice.schema.OaHasSelector;
import eu.dissco.annotationprocessingservice.schema.OaHasTarget;
import java.security.MessageDigest;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AnnotationHasherTest {

  private AnnotationHasher annotationHasher;

  @BeforeEach
  void setup() {
    try {
      this.annotationHasher = new AnnotationHasher(
          MessageDigest.getInstance("MD5")
      );
    } catch (Exception ignored) {

    }
  }

  @Test
  void hashTestFieldValueSelector() {
    // When
    var result = annotationHasher.getAnnotationHash(givenAnnotationProcessed());

    // Then
    assertThat(result).isEqualTo(ANNOTATION_HASH);
  }

  @Test
  void hashTestFragmentSelector() {
    // Given
    var selector = new OaHasSelector()
        .withAdditionalProperty("@type", "oa:FragmentSelector")
        .withAdditionalProperty("oa:hasRoi", Map.of(
            "ac:xFrac", 0.99,
            "ac:yFrac", 0.99,
            "ac:widthFrac", 0.1,
            "ac:heightFrac", 0.1
        ));

    var expected = UUID.fromString("a831698e-8bfd-4dbe-51c4-3236d0f2b047");

    // When
    var result = annotationHasher.getAnnotationHash(
        givenAnnotationProcessed().withOaHasTarget(
            new OaHasTarget()
                .withOaHasSelector(selector)
                .withId(HANDLE_PROXY + TARGET_ID)
                .withType(AnnotationTargetType.DIGITAL_SPECIMEN.toString())));

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void hashTestClassValueSelector() {
    // Given
    var selector = new OaHasSelector().withAdditionalProperty("ods:class", "ClassName");
    var expected = UUID.fromString("9906d693-479d-e4db-0790-323ae64a7565");

    // When
    var result = annotationHasher.getAnnotationHash(
        givenAnnotationProcessed().withOaHasTarget(
            givenOaTarget(TARGET_ID, AnnotationTargetType.DIGITAL_SPECIMEN, selector)));

    // Then
    assertThat(result).isEqualTo(expected);
  }

}
