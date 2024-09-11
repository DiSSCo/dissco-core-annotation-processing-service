package eu.dissco.annotationprocessingservice.controller;

import static eu.dissco.annotationprocessingservice.utils.HandleUtils.removeProxy;

import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.domain.AnnotationTombstoneWrapper;

import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingEvent;
import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import eu.dissco.annotationprocessingservice.exception.ConflictException;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.NotFoundException;
import eu.dissco.annotationprocessingservice.schema.Annotation;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;
import eu.dissco.annotationprocessingservice.service.ProcessingWebService;
import java.io.IOException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@Profile(Profiles.WEB)
@RestController
@RequestMapping("/")
@RequiredArgsConstructor
public class AnnotationController {

  private final ProcessingWebService processingService;

  @PostMapping(value = "", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Annotation> createAnnotation(@RequestBody AnnotationProcessingRequest annotation)
      throws DataBaseException, FailedProcessingException, AnnotationValidationException {
    log.info("Received hashedAnnotation creation request");
    var result = processingService.persistNewAnnotation(annotation, false);
    return ResponseEntity.ok(result);
  }

  @PostMapping(value = "batch", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Annotation> createAnnotationBatch(@RequestBody AnnotationProcessingEvent event)
      throws DataBaseException, FailedProcessingException, AnnotationValidationException {
    log.info("Received batch hashedAnnotation creation request");
    var result = processingService.persistNewAnnotation(event.getAnnotations().get(0), true);
    processingService.batchWebAnnotations(event, result);
    return ResponseEntity.ok(result);
  }

  @PutMapping(value = "/{prefix}/{suffix}", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Annotation> updateAnnotation(
      @PathVariable("prefix") String prefix,
      @PathVariable("suffix") String suffix, @RequestBody AnnotationProcessingRequest annotation)
      throws DataBaseException, FailedProcessingException, ConflictException, NotFoundException, AnnotationValidationException {
    checkId(prefix, suffix, annotation);
    log.info("Received hashedAnnotation update request for annotations {}", annotation.getOdsID());
    var result = processingService.updateAnnotation(annotation);
    return ResponseEntity.ok(result);
  }

  @DeleteMapping(value = "/{prefix}/{suffix}")
  public ResponseEntity<Void> tombstoneAnnotation(@PathVariable("prefix") String prefix,
      @PathVariable("suffix") String suffix, @RequestBody AnnotationTombstoneWrapper annotationTombstoneWrapper) throws IOException, FailedProcessingException {
    var id = prefix + '/' + suffix;
    log.info("Received an archive request for annotations: {}", id);
    var annotation = annotationTombstoneWrapper.annotation();
    var tombstoningAgent = annotationTombstoneWrapper.tombstoningAgent();
    if (!annotation.getId().contains(id)){
      log.error("Id in path {} does not match id in annotation {}", id, annotation.getId());
      throw new FailedProcessingException();
    }
    processingService.archiveAnnotation(annotation, tombstoningAgent);
    return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
  }

  private void checkId(String prefix, String suffix, AnnotationProcessingRequest annotation)
      throws ConflictException {
    var id = prefix + "/" + suffix;
    var annotationID = removeProxy(annotation);
    if (!id.equals(annotationID)) {
      log.error("provided id does not match annotations id");
      throw new ConflictException();
    }
  }

}
