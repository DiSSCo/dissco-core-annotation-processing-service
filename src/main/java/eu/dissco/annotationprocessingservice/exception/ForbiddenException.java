package eu.dissco.annotationprocessingservice.exception;

public class ForbiddenException extends Exception {

  public ForbiddenException(String annotationId, String creatorId){
    super("No annotations with id " + annotationId + " found for user " + creatorId);
  }

}
