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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import java.io.IOException;
import java.util.Map;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClient;

@ExtendWith(MockitoExtension.class)
class HandleComponentTest {

  @Mock
  private TokenAuthenticator tokenAuthenticator;
  private HandleComponent handleComponent;

  private static MockWebServer mockHandleServer;

  @BeforeAll
  static void init() throws IOException {
    mockHandleServer = new MockWebServer();
    mockHandleServer.start();
  }

  @BeforeEach
  void setup() {
    WebClient webClient = WebClient.create(
        String.format("http://%s:%s", mockHandleServer.getHostName(), mockHandleServer.getPort()));
    handleComponent = new HandleComponent(webClient, tokenAuthenticator);

  }

  @AfterAll
  static void destroy() throws IOException {
    mockHandleServer.shutdown();
  }

  @Test
  void testPostHandle() throws Exception {
    // Given
    var requestBody = givenPostRequest();
    var responseBody = givenHandleResponse();
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.CREATED.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));

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
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.CREATED.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));
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
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.CREATED.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));
    var expected = Map.of(DOI_PROXY + TARGET_ID, BARE_ID);

    // When
    var response = handleComponent.postHandlesTargetPid(givenPostRequest());

    // Then
    assertThat(response).isEqualTo(expected);
  }

  @Test
  void testUnauthorized() throws Exception {
    // Given
    var requestBody = givenPostRequest();

    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.UNAUTHORIZED.value())
        .addHeader("Content-Type", "application/json"));

    // Then
    assertThrows(PidCreationException.class, () -> handleComponent.postHandle(requestBody));
  }

  @Test
  void testBadRequest() throws Exception {
    // Given
    var requestBody = givenPostRequest();

    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.BAD_REQUEST.value())
        .addHeader("Content-Type", "application/json"));

    // Then
    assertThrows(PidCreationException.class, () -> handleComponent.postHandle(requestBody));
  }

  @Test
  void testRetriesSuccess() throws Exception {
    // Given
    var requestBody = givenPostRequest();
    var responseBody = givenHandleResponse();
    var expected = BARE_ID;
    int requestCount = mockHandleServer.getRequestCount();

    mockHandleServer.enqueue(new MockResponse().setResponseCode(501));
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.CREATED.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));

    // When
    var response = handleComponent.postHandle(requestBody);

    // Then
    assertThat(response).isEqualTo(expected);
    assertThat(mockHandleServer.getRequestCount() - requestCount).isGreaterThan(1);
  }

  @Test
  void testRollbackHandleCreation() {
    // Given
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
        .addHeader("Content-Type", "application/json"));

    // When / Then
    assertDoesNotThrow(() -> handleComponent.rollbackHandleCreation(List.of(ID)));
  }

  @Test
  void testRollbackHandleUpdate() throws Exception {
    // Given
    var requestBody = givenPostRequest();
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
        .addHeader("Content-Type", "application/json"));

    // Then
    assertDoesNotThrow(() -> handleComponent.rollbackHandleUpdate(requestBody));
  }

  @Test
  void testUpdateHandle() throws Exception {
    // Given
    var requestBody = givenPostRequest();
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
        .addHeader("Content-Type", "application/json"));

    // Then
    assertDoesNotThrow(() -> handleComponent.updateHandle(requestBody));
  }

  @Test
  void testArchiveHandle() {
    // Given
    var requestBody = MAPPER.createObjectNode();
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
        .addHeader("Content-Type", "application/json"));

    // Then
    assertDoesNotThrow(() -> handleComponent.archiveHandle(requestBody, ID));
  }

  @Test
  void testInterruptedException() throws Exception {
    // Given
    var requestBody = givenPostRequest();
    var responseBody = givenHandleResponse();

    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));

    Thread.currentThread().interrupt();

    // When
    var response = assertThrows(PidCreationException.class,
        () -> handleComponent.postHandle(requestBody));

    // Then
    assertThat(response).hasMessage(
        "Interrupted execution: A connection error has occurred in creating a handle.");
  }

  @Test
  void testRetriesFail() throws Exception {
    // Given
    var requestBody = givenPostRequest();
    int requestCount = mockHandleServer.getRequestCount();

    mockHandleServer.enqueue(new MockResponse().setResponseCode(501));
    mockHandleServer.enqueue(new MockResponse().setResponseCode(501));
    mockHandleServer.enqueue(new MockResponse().setResponseCode(501));
    mockHandleServer.enqueue(new MockResponse().setResponseCode(501));

    // Then
    assertThrows(PidCreationException.class, () -> handleComponent.postHandle(requestBody));
    assertThat(mockHandleServer.getRequestCount() - requestCount).isEqualTo(4);
  }

  @Test
  void testDataNodeNotArray() throws Exception {
    // Given
    var requestBody = givenPostRequest();
    var responseBody = MAPPER.createObjectNode();
    responseBody.put("data", "val");
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.CREATED.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));
    // Then
    assertThrows(PidCreationException.class, () -> handleComponent.postHandle(requestBody));
  }

  @Test
  void testDataMissingId() throws Exception {
    // Given
    var requestBody = givenPostRequest();
    var responseBody = givenHandleResponse();
    ((ObjectNode) responseBody.get("data").get(0)).remove("id");

    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.CREATED.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));
    // Then
    assertThrows(PidCreationException.class, () -> handleComponent.postHandle(requestBody));
  }

  @ParameterizedTest
  @ValueSource(strings = {"subjectLocalId", "mediaUrl"})
  void testMissingDigitalMediaKey(String attribute) throws Exception {
    // Given
    var requestBody = givenPostRequest();
    var responseBody = removeGivenAttribute(attribute);
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.CREATED.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));

    // Then
    assertThrows(PidCreationException.class, () -> handleComponent.postHandle(requestBody));
  }

  @Test
  void testEmptyResponse() throws Exception {
    // Given
    var requestBody = givenPostRequest();
    var responseBody = MAPPER.createObjectNode();

    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.CREATED.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));
    // Then
    assertThrows(PidCreationException.class, () -> handleComponent.postHandle(requestBody));
  }

  private JsonNode removeGivenAttribute(String targetAttribute) throws Exception {
    var response = (ObjectNode) givenHandleResponse();
    return ((ObjectNode) response.get("data").get(0).get("attributes")).remove(targetAttribute);
  }

  private JsonNode givenHandleResponse() throws Exception {
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
