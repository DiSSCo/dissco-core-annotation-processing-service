package eu.dissco.annotationprocessingservice.component;

import static eu.dissco.annotationprocessingservice.TestUtils.HANDLE_PROXY;
import static eu.dissco.annotationprocessingservice.TestUtils.TARGET_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenOaTarget;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import eu.dissco.annotationprocessingservice.domain.AnnotationTargetType;
import eu.dissco.annotationprocessingservice.schema.OaHasSelector;
import eu.dissco.annotationprocessingservice.schema.OaHasTarget;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AnnotationHasherTest {

  private AnnotationHasher annotationHasher;

  @BeforeEach
  void setup() throws NoSuchAlgorithmException {
    this.annotationHasher = new AnnotationHasher(
        MessageDigest.getInstance("MD5"));
  }

  @Test
  void hashTestFieldValueSelector() {
    // When
    var result = annotationHasher.getAnnotationHash(givenAnnotationProcessed());

    // Then
    assertThat(result).isEqualTo(UUID.fromString("2fc042f6-34ff-4bcd-9d7f-71bf449a720f"));
  }

  @Test
  void hashTestFragmentSelector() {
    // Given
    var map = new LinkedHashMap<>();
    map.put("ac:xFrac", 0.99);
    map.put("ac:yFrac", 0.99);
    map.put("ac:widthFrac", 0.1);
    map.put("ac:heightFrac", 0.1);
    var selector = new OaHasSelector()
        .withAdditionalProperty("@type", "oa:FragmentSelector")
        .withAdditionalProperty("ac:hasRoi", map);

    var expected = UUID.fromString("bff30f04-e1ca-ed30-4841-d0138dfab477");

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
    var selector = new OaHasSelector().withAdditionalProperty("ods:class", "ClassName")
        .withAdditionalProperty("@type", "ods:ClassSelector");
    var expected = UUID.fromString("9906d693-479d-e4db-0790-323ae64a7565");

    // When
    var result = annotationHasher.getAnnotationHash(
        givenAnnotationProcessed().withOaHasTarget(
            givenOaTarget(TARGET_ID, AnnotationTargetType.DIGITAL_SPECIMEN, selector)));

    // Then
    assertThat(result).isEqualTo(expected);
  }

}
