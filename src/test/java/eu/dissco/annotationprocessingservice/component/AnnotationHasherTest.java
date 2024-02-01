package eu.dissco.annotationprocessingservice.component;

import static eu.dissco.annotationprocessingservice.TestUtils.ANNOTATION_HASH;
import static eu.dissco.annotationprocessingservice.TestUtils.TARGET_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenOaTarget;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import eu.dissco.annotationprocessingservice.domain.annotation.ClassSelector;
import eu.dissco.annotationprocessingservice.domain.annotation.FragmentSelector;
import eu.dissco.annotationprocessingservice.domain.annotation.HasRoi;

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
      } catch (Exception ignored){

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
                .withAcHasRoi(new HasRoi()
                        .withAcHeightFrac(0.1)
                        .withAcWidthFrac(0.1)
                        .withAcYFrac(0.99)
                        .withAcXFrac(0.99)
                );

        var expected = UUID.fromString("a831698e-8bfd-4dbe-51c4-3236d0f2b047");

        // When
        var result = annotationHasher.getAnnotationHash(givenAnnotationProcessed().withOaTarget(givenOaTarget(TARGET_ID).withSelector(selector)));

        // Then
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void hashTestClassValueSelector() {
        // Given
        var selector = new ClassSelector()
                .withOaClass("ClassName");  //c0188fcb-9afb-0fba-e926-4cb7aa5097e8
        var expected = UUID.fromString("753ad133-4212-e03b-00e7-b757957901fd");

        // When
        var result = annotationHasher.getAnnotationHash(givenAnnotationProcessed().withOaTarget(givenOaTarget(TARGET_ID).withSelector(selector)));

        // Then
        assertThat(result).isEqualTo(expected);
    }

}
