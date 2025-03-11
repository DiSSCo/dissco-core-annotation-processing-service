package eu.dissco.annotationprocessingservice.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.http.HttpStatus;

public record ExceptionResponse(
    @JsonProperty("status")
    HttpStatus statusCode,
    String title,
    String detail) {

}
