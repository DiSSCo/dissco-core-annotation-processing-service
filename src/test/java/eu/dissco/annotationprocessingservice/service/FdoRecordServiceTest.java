package eu.dissco.annotationprocessingservice.service;

import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.dissco.annotationprocessingservice.domain.Annotation;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static eu.dissco.annotationprocessingservice.TestUtils.CREATED;
import static eu.dissco.annotationprocessingservice.TestUtils.CREATOR;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.MOTIVATION;
import static eu.dissco.annotationprocessingservice.TestUtils.TYPE;
import static eu.dissco.annotationprocessingservice.TestUtils.generateGenerator;
import static eu.dissco.annotationprocessingservice.TestUtils.generateTarget;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotation;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRecord;
import static eu.dissco.annotationprocessingservice.TestUtils.givenPostRequest;
import static eu.dissco.annotationprocessingservice.TestUtils.givenRollbackCreationRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FdoRecordServiceTest {

  private FdoRecordService fdoRecordService;

  @BeforeEach
  void setUp() {
    fdoRecordService = new FdoRecordService(MAPPER);
  }

  @Test
  void testBuildPostRequest() throws Exception {
    assertThat(fdoRecordService.buildPostHandleRequest(givenAnnotation(MOTIVATION, CREATOR)))
        .isEqualTo(givenPostRequest());
  }

  @Test
  void testBuildPostRequestFailure() throws Exception {
    // Given
    var annotation = new Annotation(TYPE, MOTIVATION, MAPPER.createObjectNode(),
        MAPPER.createObjectNode(), 100, CREATOR, CREATED, generateGenerator(), CREATED);

    // Then
    assertThrows(IllegalStateException.class, () -> fdoRecordService.buildPostHandleRequest(annotation));
  }

  void testBuildPostRequestNoGenerator() throws Exception {
    var annotation = new Annotation(TYPE, MOTIVATION, generateTarget(),
        MAPPER.createObjectNode(), 100, CREATOR, CREATED, MAPPER.createObjectNode(), CREATED);
    var expected = MAPPER.readTree("""
           {
            "data": {
              "type": "annotation",
              "attributes": {
               "fdoProfile": "https://hdl.handle.net/21.T11148/64396cf36b976ad08267",
                "issuedForAgent": "https://ror.org/0566bfb96",
                "digitalObjectType": "https://hdl.handle.net/21.T11148/64396cf36b976ad08267",
                "subjectDigitalObjectId": "https://hdl.handle.net/20.5000.1025/DW0-BNT-FM0",
                "annotationTopic":"20.5000.1025/460-A7R-QMJ",
                "replaceOrAppend": "append",
                "accessRestricted":false,
              }
            }
          }
        """);

    var result = fdoRecordService.buildPostHandleRequest(annotation);

    // Then
    assertThat(result).isEqualTo(expected);

  }

  @Test
  void testPatchRequest() throws Exception {
    var expected = givenPostRequest();
    ((ObjectNode) expected.get(0).get("data")).put("id", ID);

    assertThat(fdoRecordService.buildPatchRollbackHandleRequest(givenAnnotation(), ID))
        .isEqualTo(expected);
  }

  @Test
  void testRollbackCreation() throws Exception {
    assertThat(fdoRecordService.buildRollbackCreationRequest(givenAnnotationRecord()))
        .isEqualTo(givenRollbackCreationRequest());
  }

  @Test
  void testArchiveAnnotation() throws Exception {
    // Given
    var expected = List.of(MAPPER.readTree("""
        {
          "data":{
            "id":"20.5000.1025/KZL-VC0-ZK2",
            "attributes":{
              "tombstoneText":"This annotation was archived"
            }
          }
        }
        """));
    // When
    var result = fdoRecordService.buildArchiveHandleRequest(ID);

    // Then
    assertThat(result).isEqualTo(expected);
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
