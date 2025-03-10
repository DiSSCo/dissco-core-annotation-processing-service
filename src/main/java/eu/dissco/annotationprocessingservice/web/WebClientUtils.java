package eu.dissco.annotationprocessingservice.web;

import org.springframework.web.reactive.function.client.WebClientResponseException;

public class WebClientUtils {

  private WebClientUtils() {
  }

  public static boolean is5xxServerError(Throwable throwable) {
    return throwable instanceof WebClientResponseException webClientResponseException
        && webClientResponseException.getStatusCode().is5xxServerError();
  }


}
