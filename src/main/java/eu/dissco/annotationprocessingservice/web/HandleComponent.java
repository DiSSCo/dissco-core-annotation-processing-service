package eu.dissco.annotationprocessingservice.web;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ReactiveHttpOutputMessage;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserter;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Component
@RequiredArgsConstructor
@Slf4j
public class HandleComponent {
  
  private final WebClient handleClient;

  private static final String UNEXPECTED_LOG = "Unexpected response from handle API: {}";
  private static final String UNEXPECTED_ERR = "Unexpected response from handle API.";

  public String postHandle(List<JsonNode> request)
      throws PidCreationException {
    var responseJson = sendRequest(request);
    return getHandleName(responseJson);
  }

  public Map<UUID, String> postHandlesHashed(List<JsonNode> request) throws PidCreationException {
    var responseJson = sendRequest(request);
    return getHandleMapHash(responseJson);
  }

  public Map<String, String> postHandlesTargetPid(List<JsonNode> request)
      throws PidCreationException {
    var responseJson = sendRequest(request);
    return getHandleMapTargetPid(responseJson);
  }

  private JsonNode sendRequest(List<JsonNode> request) throws PidCreationException {
    var requestBody = BodyInserters.fromValue(request);
    var response = sendRequest(HttpMethod.POST, requestBody, "batch");
    return validateResponse(response);
  }

  public void updateHandle(List<JsonNode> request)
      throws PidCreationException {
    var requestBody = BodyInserters.fromValue(request);
    var response = sendRequest(HttpMethod.PATCH, requestBody, "");
    validateResponse(response);
  }

  public void rollbackHandleCreation(List<String> request)
      throws PidCreationException {
    var requestBody = BodyInserters.fromValue(request);
    var response = sendRequest(HttpMethod.DELETE, requestBody, "rollback/create");
    validateResponse(response);
  }

  public void rollbackHandleUpdate(List<JsonNode> request)
      throws PidCreationException {
    var requestBody = BodyInserters.fromValue(request);
    var response = sendRequest(HttpMethod.DELETE, requestBody, "rollback/update");
    validateResponse(response);
  }

  public void archiveHandle(JsonNode request, String handle) throws PidCreationException {
    var requestBody = BodyInserters.fromValue(request);
    var response = sendRequest(HttpMethod.PUT, requestBody, handle);
    validateResponse(response);
  }

  private <T> Mono<JsonNode> sendRequest(HttpMethod httpMethod,
      BodyInserter<T, ReactiveHttpOutputMessage> requestBody, String endpoint) {
    return handleClient
        .method(httpMethod)
        .uri(uriBuilder -> uriBuilder.path(endpoint).build())
        .body(requestBody)
        .acceptCharset(StandardCharsets.UTF_8)
        .retrieve()
        .onStatus(HttpStatus.UNAUTHORIZED::equals,
            r -> Mono.error(
                new PidCreationException("Unable to authenticate with Handle Service.")))
        .onStatus(HttpStatusCode::is4xxClientError, r -> Mono.error(new PidCreationException(
            "Unable to create PID. Response from Handle API: " + r.statusCode())))
        .bodyToMono(JsonNode.class).retryWhen(
            Retry.fixedDelay(3, Duration.ofSeconds(2)).filter(WebClientUtils::is5xxServerError)
                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> new PidCreationException(
                    "External Service failed to process after max retries")));
  }

  private JsonNode validateResponse(Mono<JsonNode> response) throws PidCreationException {
    try {
      return response.toFuture().get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("Interrupted exception has occurred.");
      throw new PidCreationException(
          "Interrupted execution: A connection error has occurred in creating a handle.");
    } catch (ExecutionException e) {
      log.error("PID creation failed.", e.getCause());
      throw new PidCreationException(e.getCause().getMessage());
    }
  }

  private String getHandleName(JsonNode jsonResponse) throws PidCreationException {
    try {
      var dataNodeArray = jsonResponse.get("data");
      if (!dataNodeArray.isArray()) {
        throw new PidCreationException(
            "UNEXPECTED_MSG + \" Response: {}\", jsonResponse.toPrettyString()");
      }
      if (dataNodeArray.size() != 1) {
        log.error(UNEXPECTED_LOG, jsonResponse);
        throw new PidCreationException(UNEXPECTED_ERR);
      }
      return dataNodeArray.get(0).get("id").asText();
    } catch (NullPointerException e) {
      log.error(UNEXPECTED_LOG, jsonResponse);
      throw new PidCreationException(UNEXPECTED_ERR);
    }
  }

  private Map<UUID, String> getHandleMapHash(JsonNode jsonResponse) throws PidCreationException {
    try {
      var handleNames = new HashMap<UUID, String>();
      var dataNodeArray = jsonResponse.get("data");
      if (!dataNodeArray.isArray()) {
        throw new PidCreationException(UNEXPECTED_ERR);
      }
      for (var dataNode : dataNodeArray) {
        handleNames.put(
            UUID.fromString(dataNode.get("attributes").get("annotationHash").asText()),
            dataNode.get("id").asText());
      }
      return handleNames;
    } catch (NullPointerException e) {
      log.error(UNEXPECTED_LOG, jsonResponse);
      throw new PidCreationException(UNEXPECTED_ERR);
    }
  }

  private Map<String, String> getHandleMapTargetPid(JsonNode jsonResponse)
      throws PidCreationException {
    try {
      var handleNames = new HashMap<String, String>();
      var dataNodeArray = jsonResponse.get("data");
      if (!dataNodeArray.isArray()) {
        throw new PidCreationException(
            "Unexpected response from handle API. Response: " + jsonResponse);
      }
      for (var dataNode : dataNodeArray) {
        handleNames.put(
            dataNode.get("attributes").get("targetPid").asText(),
            dataNode.get("id").asText());
      }
      return handleNames;
    } catch (NullPointerException e) {
      log.error(UNEXPECTED_LOG, jsonResponse);
      throw new PidCreationException(UNEXPECTED_ERR);
    }
  }

}