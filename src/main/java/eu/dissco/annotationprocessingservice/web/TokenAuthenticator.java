package eu.dissco.annotationprocessingservice.web;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import eu.dissco.annotationprocessingservice.properties.TokenProperties;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Component
@RequiredArgsConstructor
@Slf4j
public class TokenAuthenticator {

  private final TokenProperties properties;

  @Qualifier("tokenClient")
  private final WebClient tokenClient;

  @Cacheable("token-cache")
  public String getToken() throws PidCreationException {
    log.info("Requesting new token from keycloak");
    var response = tokenClient
        .post()
        .body(BodyInserters.fromFormData(properties.getFromFormData()))
        .acceptCharset(StandardCharsets.UTF_8)
        .retrieve()
        .onStatus(HttpStatus.UNAUTHORIZED::equals,
            r -> Mono.error(new PidCreationException("Service is unauthorized.")))
        .bodyToMono(JsonNode.class)
        .retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(2))
            .filter(WebClientUtils::is5xxServerError)
            .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) ->
                new PidCreationException(
                    "Token Authentication failed to process after max retries")
            ));
    try {
      var tokenNode = response.toFuture().get();
      return getToken(tokenNode);
    } catch (ExecutionException e) {
      log.error(
          "Token authentication: Unable to authenticate processing service with Keycloak. Verify client secret is up to-date");
      throw new PidCreationException(
          "Unable to authenticate processing service with Keycloak. More information: "
              + e.getMessage());
    } catch (InterruptedException e){
      Thread.currentThread().interrupt();
      log.error("An interrupted exception has occurred", e);
      throw new PidCreationException("Unable to authenticate service with keycloak. More information: "+ e.getMessage());
    }
  }

  private String getToken(JsonNode tokenNode) throws PidCreationException {
    if (tokenNode != null && tokenNode.get("access_token") != null) {
      return tokenNode.get("access_token").asText();
    }
    log.debug("Unexpected response from keycloak server. Unable to parse access_token");
    throw new PidCreationException(
        "Unable to authenticate processing service with Keycloak. An error has occurred parsing keycloak response");
  }

}
