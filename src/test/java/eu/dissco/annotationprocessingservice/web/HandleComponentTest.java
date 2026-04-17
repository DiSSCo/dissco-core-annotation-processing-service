package eu.dissco.annotationprocessingservice.web;

import static eu.dissco.annotationprocessingservice.TestUtils.ANNOTATION_HASH;
import static eu.dissco.annotationprocessingservice.TestUtils.BARE_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.DOI_PROXY;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.TARGET_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenPostRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;

import eu.dissco.annotationprocessingservice.client.HandleClient;
import eu.dissco.annotationprocessingservice.exception.PidException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

@ExtendWith(MockitoExtension.class)
class HandleComponentTest {

  private HandleComponent handleComponent;
  @Mock
  private HandleClient handleClient;

  @BeforeEach
  void setup() {
    handleComponent = new HandleComponent(handleClient);
  }

  @Test
  void testPostHandle() throws Exception {
    // Given
    var requestBody = givenPostRequest();
    var responseBody = givenHandleResponse();
    given(handleClient.postHandles(any())).willReturn(responseBody);

    // When
    var response = handleComponent.postHandle(requestBody);

    // Then
    assertThat(response).isEqualTo(BARE_ID);
  }

  @Test
  void testPostHandlesHashed() throws Exception {
    // Given
    var responseBody = MAPPER.readTree("""
        {
          "data": [{
            "type": "mediaObject",
            "id":"20.5000.1025/KZL-VC0-ZK2",
            "attributes": {
               "digitalObjectType": "https://hdl.handle.net/21.T11148/64396cf36b976ad08267",
               "subjectDigitalObjectId": "https://hdl.handle.net/20.5000.1025/DW0-BNT-FM0",
               "annotationTopic":"20.5000.1025/460-A7R-QMJ",
               "replaceOrAppend": "append",
               "accessRestricted": false,
               "linkedObjectUrl":"https://hdl.handle.net/anno-process-service-pid",
               "annotationHash": "3a36d684-deb8-8779-2753-caef497e9ed8"
             }
           }]
        }
        """);
    given(handleClient.postHandles(any())).willReturn(responseBody);
    var expected = Map.of(ANNOTATION_HASH, BARE_ID);

    // When
    var response = handleComponent.postHandlesHashed(givenPostRequest());

    // Then
    assertThat(response).isEqualTo(expected);
  }

  @Test
  void testPostHandlesTargetPid() throws Exception {
    // Given
    var responseBody = givenHandleResponse();
    given(handleClient.postHandles(any())).willReturn(responseBody);
    var expected = Map.of(DOI_PROXY + TARGET_ID, BARE_ID);

    // When
    var response = handleComponent.postHandlesTargetPid(givenPostRequest());

    // Then
    assertThat(response).isEqualTo(expected);
  }

  @Test
  void testRollbackHandleCreation() throws Exception {
    // Given
    var request = List.of(ID);

    // When
    handleComponent.rollbackHandleCreation(request);

    // Then
    then(handleClient).should().rollbackHandleCreation(request);
  }

  @Test
  void testRollbackHandleUpdate() throws Exception {
    // Given
    var request = givenPostRequest();

    // When
    assertDoesNotThrow(() -> handleComponent.rollbackHandleUpdate(request));

    // Then
    then(handleClient).should().rollbackHandleUpdate(request);
  }

  @Test
  void testUpdateHandles() throws Exception {
    // Given
    var request = givenPostRequest();

    // When
    handleComponent.updateHandles(request);

    // Then
    then(handleClient).should().updateHandles(request);
  }

  @Test
  void testArchiveHandle() throws Exception {
    // Given
    var request = MAPPER.createObjectNode();

    // When
    handleComponent.archiveHandle(request, ID);

    // Then
    then(handleClient).should().archiveHandle(ID, request);
  }

  @Test
  void testDataNodeNotArray() throws Exception {
    // Given
    var requestBody = givenPostRequest();
    var responseBody = MAPPER.createObjectNode().put("data", "val");
    given(handleClient.postHandles(requestBody)).willReturn(responseBody);

    // When / Then
    assertThrows(PidException.class, () -> handleComponent.postHandle(requestBody));
  }

  @ParameterizedTest
  @MethodSource("badResponse")
  void testBadResponse(JsonNode response) throws Exception {
    // Given
    given(handleClient.postHandles(any())).willReturn(response);

    // When / Then
    assertThrows(PidException.class, () -> handleComponent.postHandle(givenPostRequest()));
  }

  private static Stream<Arguments> badResponse() {
    return Stream.of(
        Arguments.of(MAPPER.createObjectNode()),
        Arguments.of(removeGivenAttribute("subjectLocalId")),
        Arguments.of(removeGivenAttribute("mediaUrl")),
        Arguments.of(removeGivenAttribute("id"))
    );
  }

  private static JsonNode removeGivenAttribute(String targetAttribute) {
    var response = (ObjectNode) givenHandleResponse();
    return ((ObjectNode) response.get("data").get(0).get("attributes")).remove(targetAttribute);
  }

  private static JsonNode givenHandleResponse() {
    return MAPPER.readTree("""
        {
          "data": [{
            "type": "mediaObject",
            "id":"20.5000.1025/KZL-VC0-ZK2",
            "attributes": {
               "targetPid": "https://doi.org/20.5000.1025/QRS-123-ABC",
               "digitalObjectType": "https://hdl.handle.net/21.T11148/64396cf36b976ad08267",
               "subjectDigitalObjectId": "https://hdl.handle.net/20.5000.1025/DW0-BNT-FM0",
               "annotationTopic":"20.5000.1025/460-A7R-QMJ",
               "replaceOrAppend": "append",
               "accessRestricted":false,
               "linkedObjectUrl":"https://hdl.handle.net/anno-process-service-pid"
             }
           }]
        }
        """);
  }


}
