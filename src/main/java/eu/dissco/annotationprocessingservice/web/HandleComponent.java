package eu.dissco.annotationprocessingservice.web;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
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

  @Qualifier("handleClient")
  private final WebClient handleClient;
  private final TokenAuthenticator tokenAuthenticator;

  public List<String> postHandle(List<JsonNode> request)
      throws PidCreationException {
    var responseJson = sendRequest(request);
    return getHandleNames(responseJson);
  }

  public Map<UUID, String> postHandles(List<JsonNode> request) throws PidCreationException {
    var responseJson = sendRequest(request);
    return getHandleMap(responseJson);
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
      BodyInserter<T, ReactiveHttpOutputMessage> requestBody, String endpoint)
      throws PidCreationException {
    var token = "Bearer " + tokenAuthenticator.getToken();
    return handleClient
        .method(httpMethod)
        .uri(uriBuilder -> uriBuilder.path(endpoint).build())
        .body(requestBody)
        .header("Authorization", token)
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

  private List<String> getHandleNames(JsonNode jsonResponse) throws PidCreationException {
    try {
      var handleNames = new ArrayList<String>();
      var dataNodeArray = jsonResponse.get("data");
      if (!dataNodeArray.isArray()) {
        throw new PidCreationException(
            "UNEXPECTED_MSG + \" Response: {}\", jsonResponse.toPrettyString()");
      }
      for (var dataNode : dataNodeArray) {
        handleNames.add(dataNode.get("id").asText());
      }
      return handleNames;
    } catch (NullPointerException e) {
      log.error("Unexpected response from handle API. Response: {}", jsonResponse.toPrettyString());
      throw new PidCreationException("Unexpected response from handle API.");
    }
  }

  private Map<UUID, String> getHandleMap(JsonNode jsonResponse) throws PidCreationException {
    try {
      var handleNames = new HashMap<UUID, String>();
      var dataNodeArray = jsonResponse.get("data");
      if (!dataNodeArray.isArray()) {
        throw new PidCreationException(
            "Unexpected response from handle API. Response: {}\", jsonResponse.toPrettyString()");
      }
      for (var dataNode : dataNodeArray) {
        handleNames.put(
            UUID.fromString(dataNode.get("attributes").get("annotationHash").asText()),
            dataNode.get("id").asText());
      }
      return handleNames;
    } catch (NullPointerException e) {
      log.error("Unexpected response from handle API. Response: {}", jsonResponse.toPrettyString());
      throw new PidCreationException("Unexpected response from handle API.");
    }
  }
}