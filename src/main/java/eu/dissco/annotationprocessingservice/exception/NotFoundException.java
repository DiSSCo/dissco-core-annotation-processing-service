package eu.dissco.annotationprocessingservice.exception;

public class NotFoundException extends Exception {
  public NotFoundException(String id){
    super("No annotations found for id " + id);
  }

}
