package eu.dissco.annotationprocessingservice.exception;

import org.jooq.exception.DataAccessException;

public class DataBaseException extends DataAccessException {

  public DataBaseException(String message) {
    super(message);
  }
}
