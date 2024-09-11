package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.ID_ALT;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRequest;
import static eu.dissco.annotationprocessingservice.TestUtils.givenHashedAnnotation;
import static eu.dissco.annotationprocessingservice.TestUtils.givenHashedAnnotationAlt;
import static eu.dissco.annotationprocessingservice.TestUtils.givenHashedAnnotationRequest;
import static eu.dissco.annotationprocessingservice.TestUtils.givenHashedAnnotationRequestAlt;
import static eu.dissco.annotationprocessingservice.TestUtils.givenOaTarget;
import static eu.dissco.annotationprocessingservice.TestUtils.givenPatchRequest;
import static eu.dissco.annotationprocessingservice.TestUtils.givenPatchRequestBatch;
import static eu.dissco.annotationprocessingservice.TestUtils.givenPostRequest;
import static eu.dissco.annotationprocessingservice.TestUtils.givenPostRequestBatch;
import static eu.dissco.annotationprocessingservice.TestUtils.givenRollbackCreationRequest;
import static org.assertj.core.api.Assertions.assertThat;

import eu.dissco.annotationprocessingservice.properties.FdoProperties;
import eu.dissco.annotationprocessingservice.schema.Annotation;
import eu.dissco.annotationprocessingservice.schema.Annotation.OaMotivation;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FdoRecordServiceTest {

  private FdoRecordService fdoRecordService;

  private static Stream<Arguments> handleNeedsUpdate() {
    return Stream.of(
        Arguments.of(givenAnnotationProcessed().withOaMotivation(OaMotivation.OA_EDITING)),
        Arguments.of(
            givenAnnotationProcessed().withOaHasTarget(givenOaTarget("different target"))));
  }

  @BeforeEach
  void setUp() {
    fdoRecordService = new FdoRecordService(MAPPER, new FdoProperties());
  }

  @Test
  void testBuildPostRequest() throws Exception {
    assertThat(fdoRecordService.buildPostHandleRequest(givenAnnotationRequest()))
        .isEqualTo(givenPostRequest());
  }

  @Test
  void testBuildPostRequestBatch() throws Exception {
    assertThat(fdoRecordService.buildPostHandleRequest(
        List.of(givenHashedAnnotationRequest(), givenHashedAnnotationRequestAlt())))
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
            "id":"https://hdl.handle.net/20.5000.1025/KZL-VC0-ZK2",
            "attributes":{
              "tombstoneText":"This annotation was archived"
            }
          }
        }
        """);

    // When
    var result = fdoRecordService.buildTombstoneHandleRequest(ID);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testRollbackAnnotationList() throws Exception {
    // Given
    var expected = MAPPER.readTree("""
        {
          "data": [
                      {"id":"https://hdl.handle.net/20.5000.1025/KZL-VC0-ZK2"},
                      {"id":"https://hdl.handle.net/20.5000.1025/ZZZ-YYY-XXX"}
                    ]
                  }
        """);

    // When
    var result = fdoRecordService.buildRollbackCreationRequest(List.of(ID, ID_ALT));

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testHandleNeedsUpdateFalse() {
    assertThat(fdoRecordService.handleNeedsUpdate(givenAnnotationProcessed(),
        givenAnnotationProcessed())).isFalse();
  }

  @ParameterizedTest
  @MethodSource("handleNeedsUpdate")
  void testHandleNeedsUpdate(Annotation newAnnotation) {
    assertThat(
        fdoRecordService.handleNeedsUpdate(givenAnnotationProcessed(), newAnnotation)).isTrue();
  }

}
