package eu.dissco.annotationprocessingservice.exception;

public class NotFoundException extends Exception {

  public NotFoundException(String annotationId, String creatorId){
    super("No annotations with id " + annotationId + " found for user " + creatorId);
  }

}
