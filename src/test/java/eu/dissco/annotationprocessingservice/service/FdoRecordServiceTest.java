package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenCreator;
import static eu.dissco.annotationprocessingservice.TestUtils.givenOaTarget;
import static eu.dissco.annotationprocessingservice.TestUtils.givenPostRequest;
import static eu.dissco.annotationprocessingservice.TestUtils.givenRollbackCreationRequest;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.domain.annotation.Motivation;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FdoRecordServiceTest {

  private FdoRecordService fdoRecordService;

  @BeforeEach
  void setUp() {
    fdoRecordService = new FdoRecordService(MAPPER);
  }

  @Test
  void testBuildPostRequest() throws Exception {
    assertThat(fdoRecordService.buildPostHandleRequest(givenAnnotationProcessed()))
        .isEqualTo(givenPostRequest());
  }

  @Test
  void testPatchRequest() throws Exception {
    var expected = givenPostRequest();
    ((ObjectNode) expected.get(0).get("data")).put("id", ID);

    assertThat(fdoRecordService.buildPatchRollbackHandleRequest(givenAnnotationProcessed(), ID))
        .isEqualTo(expected);
  }

  @Test
  void testRollbackCreation() throws Exception {
    assertThat(fdoRecordService.buildRollbackCreationRequest(givenAnnotationProcessed()))
        .isEqualTo(givenRollbackCreationRequest());
  }

  @Test
  void testArchiveAnnotation() throws Exception {
    // Given
    var expected = MAPPER.readTree("""
        {
          "data":{
            "id":"20.5000.1025/KZL-VC0-ZK2",
            "attributes":{
              "tombstoneText":"This annotation was archived"
            }
          }
        }
        """);

    // When
    var result = fdoRecordService.buildArchiveHandleRequest(ID);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testHandleNeedsUpdateFalse() {
    assertThat(fdoRecordService.handleNeedsUpdate(givenAnnotationProcessed(),
        givenAnnotationProcessed())).isFalse();
  }

  private static Stream<Arguments> handleNeedsUpdate() {
    return Stream.of(
        Arguments.of(givenAnnotationProcessed().withOaMotivation(Motivation.EDITING)),
        Arguments.of(givenAnnotationProcessed().withOaTarget(givenOaTarget("different target"))));
  }

  @ParameterizedTest
  @MethodSource("handleNeedsUpdate")
  void testHandleNeedsUpdate(Annotation newAnnotation) {
    assertThat(
        fdoRecordService.handleNeedsUpdate(givenAnnotationProcessed(), newAnnotation)).isTrue();
  }

}
