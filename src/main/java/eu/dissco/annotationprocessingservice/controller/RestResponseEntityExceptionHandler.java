package eu.dissco.annotationprocessingservice.controller;

import eu.dissco.annotationprocessingservice.domain.ExceptionResponse;
import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import eu.dissco.annotationprocessingservice.exception.ConflictException;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.NotFoundException;
import java.io.IOException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.reactive.result.method.annotation.ResponseEntityExceptionHandler;

@ControllerAdvice
public class RestResponseEntityExceptionHandler extends ResponseEntityExceptionHandler {

  @ResponseStatus(HttpStatus.UNPROCESSABLE_ENTITY)
  @ExceptionHandler(FailedProcessingException.class)
  public ResponseEntity<ExceptionResponse> handleException(
      FailedProcessingException e) {
    var response = new ExceptionResponse(
        HttpStatus.UNPROCESSABLE_ENTITY, "FailedPocessingException", e.getMessage()
    );
    return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(response);
  }

  @ResponseStatus(HttpStatus.UNPROCESSABLE_ENTITY)
  @ExceptionHandler(IOException.class)
  public ResponseEntity<ExceptionResponse> handleException(IOException e) {
    var response = new ExceptionResponse(
        HttpStatus.UNPROCESSABLE_ENTITY, "IOException", e.getMessage()
    );
    return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(response);
  }

  @ResponseStatus(HttpStatus.UNPROCESSABLE_ENTITY)
  @ExceptionHandler(DataBaseException.class)
  public ResponseEntity<ExceptionResponse> handleException(DataBaseException e) {
    var response = new ExceptionResponse(
        HttpStatus.UNPROCESSABLE_ENTITY, DataBaseException.class.getSimpleName(), e.getMessage()
    );
    return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(response);
  }

  @ResponseStatus(HttpStatus.BAD_REQUEST)
  @ExceptionHandler(AnnotationValidationException.class)
  public ResponseEntity<ExceptionResponse> handleException(AnnotationValidationException e) {
    var response = new ExceptionResponse(
        HttpStatus.BAD_REQUEST, "AnnotationValidationException", e.getMessage()
    );
    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
  }

  @ResponseStatus(HttpStatus.CONFLICT)
  @ExceptionHandler(ConflictException.class)
  public ResponseEntity<ExceptionResponse> handleException(ConflictException e) {
    var response = new ExceptionResponse(
        HttpStatus.CONFLICT, "ConflictException", "Provided ID does not match ID in annotation"
    );
    return ResponseEntity.status(HttpStatus.CONFLICT).body(response);
  }

  @ResponseStatus(HttpStatus.NOT_FOUND)
  @ExceptionHandler(NotFoundException.class)
  public ResponseEntity<ExceptionResponse> handleException(NotFoundException e) {
    var response = new ExceptionResponse(
        HttpStatus.NOT_FOUND, "NotFoundException", e.getMessage()
    );
    return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
  }


}
