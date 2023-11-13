package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.ANNOTATION_HASH;
import static eu.dissco.annotationprocessingservice.TestUtils.JOB_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.TARGET_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenCreator;
import static eu.dissco.annotationprocessingservice.TestUtils.givenOaTarget;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import eu.dissco.annotationprocessingservice.domain.annotation.ClassValueSelector;
import eu.dissco.annotationprocessingservice.domain.annotation.FragmentSelector;
import eu.dissco.annotationprocessingservice.domain.annotation.HasRoi;
import eu.dissco.annotationprocessingservice.service.serviceuitls.AnnotationHasher;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class AnnotationHasherTest {

  @Test
  void hashTestFieldValueSelector() {
    // When
    var result = AnnotationHasher.getAnnotationHash(givenAnnotationProcessed());

    // Then
    assertThat(result).isEqualTo(ANNOTATION_HASH);
  }

  @Test
  void hashTestFragmentSelector() {
    // Given
    var selector = new FragmentSelector()
        .withAcHasRoi(new HasRoi()
            .withAcHeightFrac(0.1)
            .withAcWidthFrac(0.1)
            .withAcYFrac(0.99)
            .withAcXFrac(0.99)
        );

    var expected = UUID.fromString("12cdd96c-758e-dea3-64ec-5234d3115f0e");

    // When
    var result = AnnotationHasher.getAnnotationHash(givenAnnotationProcessed().withOaTarget(givenOaTarget(TARGET_ID).withSelector(selector)));

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void hashTestClassValueSelector() {
    // Given
    var selector = new ClassValueSelector()
        .withOdsClass("ClassName");
    var expected = UUID.fromString("c0188fcb-9afb-0fba-e926-4cb7aa5097e8");

    // When
    var result = AnnotationHasher.getAnnotationHash(givenAnnotationProcessed().withOaTarget(givenOaTarget(TARGET_ID).withSelector(selector)));

    // Then
    assertThat(result).isEqualTo(expected);
  }

}
