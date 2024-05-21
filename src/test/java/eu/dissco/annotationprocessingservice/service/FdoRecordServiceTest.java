package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.TARGET_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenHashedAnnotation;
import static eu.dissco.annotationprocessingservice.TestUtils.givenHashedAnnotationAlt;
import static eu.dissco.annotationprocessingservice.TestUtils.givenOaTarget;
import static eu.dissco.annotationprocessingservice.TestUtils.givenPatchRequest;
import static eu.dissco.annotationprocessingservice.TestUtils.givenPatchRequestBatch;
import static eu.dissco.annotationprocessingservice.TestUtils.givenPostRequest;
import static eu.dissco.annotationprocessingservice.TestUtils.givenPostRequestBatch;
import static eu.dissco.annotationprocessingservice.TestUtils.givenRollbackCreationRequest;
import static org.assertj.core.api.Assertions.assertThat;

import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.domain.annotation.Motivation;
import eu.dissco.annotationprocessingservice.properties.FdoProperties;
import java.util.List;
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
    fdoRecordService = new FdoRecordService(MAPPER, new FdoProperties());
  }

  @Test
  void testBuildPostRequest() throws Exception {
    assertThat(fdoRecordService.buildPostHandleRequest(givenAnnotationProcessed()))
        .isEqualTo(givenPostRequest());
  }

  @Test
  void testBuildPostRequestBatch() throws Exception {
    assertThat(fdoRecordService.buildPostHandleRequest(
        List.of(givenHashedAnnotation(), givenHashedAnnotationAlt())))
        .isEqualTo(givenPostRequestBatch());
  }

  @Test
  void testPatchRequest() throws Exception {
    // When
    var result = fdoRecordService.buildPatchRollbackHandleRequest(givenAnnotationProcessed());

    // Then
    assertThat(result).isEqualTo(givenPatchRequest());
  }

  @Test
  void testPatchRequestList() throws Exception {
    // Given
    var expected = givenPatchRequestBatch();

    // When
    var result = fdoRecordService.buildPatchRollbackHandleRequest(List.of(givenHashedAnnotation(),
        givenHashedAnnotationAlt()));

    // Then
    assertThat(result).isEqualTo(expected);
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
  void testRollbackAnnotationList() throws Exception {
    // Given
    var expected = MAPPER.readTree("""
        {
          "data": [
                      {"id":"20.5000.1025/KZL-VC0-ZK2"},
                      {"id":"20.5000.1025/QRS-123-ABC"}
                    ]
                  }
        """);

    // When
    var result = fdoRecordService.buildRollbackCreationRequest(List.of(ID, TARGET_ID));

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
        Arguments.of(givenAnnotationProcessed().setOaMotivation(Motivation.EDITING)),
        Arguments.of(givenAnnotationProcessed().setOaTarget(givenOaTarget("different target"))));
  }

  @ParameterizedTest
  @MethodSource("handleNeedsUpdate")
  void testHandleNeedsUpdate(Annotation newAnnotation) {
    assertThat(
        fdoRecordService.handleNeedsUpdate(givenAnnotationProcessed(), newAnnotation)).isTrue();
  }

}
