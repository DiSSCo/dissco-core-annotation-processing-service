package eu.dissco.annotationprocessingservice.service;

import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.dissco.annotationprocessingservice.domain.Annotation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static eu.dissco.annotationprocessingservice.TestUtils.CREATOR;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.MOTIVATION;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotation;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRecord;
import static eu.dissco.annotationprocessingservice.TestUtils.givenPostRequest;
import static eu.dissco.annotationprocessingservice.TestUtils.givenRollbackCreationRequest;
import static org.assertj.core.api.Assertions.assertThat;

class FdoRecordServiceTest {

  private FdoRecordService fdoRecordService;

  @BeforeEach
  void setUp() {
    fdoRecordService = new FdoRecordService(MAPPER);
  }

  @Test
  void testPostRequest() throws Exception {
    assertThat(fdoRecordService.buildPostHandleRequest(givenAnnotation(MOTIVATION, CREATOR)))
        .isEqualTo(givenPostRequest());
  }

  @Test
  void testPatchRequest() throws Exception {
    var expected = givenPostRequest();
    ((ObjectNode) expected.get(0).get("data")).put("id", ID);

    assertThat(fdoRecordService.buildPatchDeleteHandleRequest(givenAnnotationRecord()))
        .isEqualTo(expected);
  }

  @Test
  void testRollbackCreation() throws Exception {
    assertThat(fdoRecordService.buildRollbackCreationRequest(givenAnnotationRecord()))
        .isEqualTo(givenRollbackCreationRequest());
  }

  @Test
  void testHandleNeedsUpdateFalse() throws Exception {
    assertThat(fdoRecordService.handleNeedsUpdate(givenAnnotation(), givenAnnotation())).isFalse();
  }

  @Test
  void testHandleNeedsUpdateMotivation() throws Exception {
    assertThat(
        fdoRecordService.handleNeedsUpdate(givenAnnotation("A", "A"),
            givenAnnotation())).isTrue();
  }

  @Test
  void testHandleNeedsUpdateId() throws Exception {
    var annotation1 = givenAnnotation();
    var newTarget = MAPPER.readTree("""
         {
              "id": "https://hdl.handle.net/20.5000.1025/AAA-AAA-AAA",
              "type": "digital_specimen",
              "indvProp": "modified"
            }
        """);
    var annotation2 = new Annotation(
        annotation1.type(),
        annotation1.motivation(),
        newTarget,
        annotation1.body(),
        annotation1.preferenceScore(),
        annotation1.creator(),
        annotation1.created(),
        annotation1.generator(),
        annotation1.generated()
    );
    assertThat(fdoRecordService.handleNeedsUpdate(annotation1, annotation2)).isTrue();
  }

}
