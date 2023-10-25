package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.ANNOTATION_HASH;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import eu.dissco.annotationprocessingservice.service.serviceuitls.AnnotationHasher;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class AnnotationHasherTest {

  @Test
  void hashTest() {
    // When
    var result = AnnotationHasher.getAnnotationHash(givenAnnotationProcessed());

    // Then
    assertThat(result).isEqualTo(ANNOTATION_HASH);
  }
}
