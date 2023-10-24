package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import eu.dissco.annotationprocessingservice.service.serviceuitls.AnnotationHasher;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class AnnotationHasherTest {

  @Test
  void hashTest() {
    // Given
    var expected = UUID.fromString("091dcacc-d5f9-4c33-038c-476202c1f825");

    // When
    var result = AnnotationHasher.getAnnotationHash(givenAnnotationProcessed());

    // Then
    assertThat(result).isEqualTo(expected);
  }
}
