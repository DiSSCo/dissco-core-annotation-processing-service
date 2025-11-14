package eu.dissco.annotationprocessingservice.controller;

import static org.assertj.core.api.Assertions.assertThat;

import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import eu.dissco.annotationprocessingservice.exception.ConflictException;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.MethodNotAllowedException;
import eu.dissco.annotationprocessingservice.exception.NotFoundException;
import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

class RestResponseEntityExceptionHandlerTest {

  private RestResponseEntityExceptionHandler exceptionHandler;

  @BeforeEach
  void setup() {
    exceptionHandler = new RestResponseEntityExceptionHandler();
  }

  @Test
  void testFailedProcessingException(){
    // When
    var result = exceptionHandler.handleException(new FailedProcessingException());

    // Then
    assertThat(result.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
  }

  @Test
  void testIOException(){
    // When
    var result = exceptionHandler.handleException(new IOException());

    // Then
    assertThat(result.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
  }

  @Test
  void testDataBaseException(){
    // When
    var result = exceptionHandler.handleException(new DataBaseException(""));

    // Then
    assertThat(result.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
  }

  @Test
  void testAnnotationValidationException(){
    // When
    var result = exceptionHandler.handleException(new AnnotationValidationException());

    // Then
    assertThat(result.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
  }

  @Test
  void testConflictException(){
    // When
    var result = exceptionHandler.handleException(new ConflictException());

    // Then
    assertThat(result.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
  }

  @Test
  void testNotFoundException(){
    // When
    var result = exceptionHandler.handleException(new NotFoundException("",""));

    // Then
    assertThat(result.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
  }

  @Test
  void testMethodNotAllowedException(){
    // When
    var result = exceptionHandler.handleException(new MethodNotAllowedException(""));

    // Then
    assertThat(result.getStatusCode()).isEqualTo(HttpStatus.METHOD_NOT_ALLOWED);
  }
}
