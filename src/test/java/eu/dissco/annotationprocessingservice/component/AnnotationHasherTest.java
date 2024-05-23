package eu.dissco.annotationprocessingservice.component;

import static eu.dissco.annotationprocessingservice.TestUtils.ANNOTATION_HASH;
import static eu.dissco.annotationprocessingservice.TestUtils.HANDLE_PROXY;
import static eu.dissco.annotationprocessingservice.TestUtils.TARGET_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenOaTarget;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import eu.dissco.annotationprocessingservice.domain.annotation.AnnotationTargetType;
import eu.dissco.annotationprocessingservice.domain.annotation.ClassSelector;
import eu.dissco.annotationprocessingservice.domain.annotation.FragmentSelector;
import eu.dissco.annotationprocessingservice.domain.annotation.HasRoi;
import eu.dissco.annotationprocessingservice.domain.annotation.Target;
import java.security.MessageDigest;
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
    var selector = new FragmentSelector()
        .withAcHasRoi(HasRoi.builder()
            .heightFrac(0.1)
            .widthFrac(0.1)
            .valY(0.99)
            .valX(0.99)
            .build()
        );

    var expected = UUID.fromString("a831698e-8bfd-4dbe-51c4-3236d0f2b047");

    // When
    var result = annotationHasher.getAnnotationHash(
        givenAnnotationProcessed().setOaTarget(
            Target.builder()
                .oaSelector(selector)
                .odsId(HANDLE_PROXY + TARGET_ID)
                .odsType(AnnotationTargetType.DIGITAL_SPECIMEN)
                .build()));

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void hashTestClassValueSelector() {
    // Given
    var selector = new ClassSelector()
        .withOaClass("ClassName");
    var expected = UUID.fromString("9906d693-479d-e4db-0790-323ae64a7565");

    // When
    var result = annotationHasher.getAnnotationHash(
        givenAnnotationProcessed().setOaTarget(givenOaTarget(TARGET_ID, AnnotationTargetType.DIGITAL_SPECIMEN, selector)));

    // Then
    assertThat(result).isEqualTo(expected);
  }

}
