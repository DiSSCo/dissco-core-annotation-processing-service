package eu.dissco.annotationprocessingservice.controller;

import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import eu.dissco.annotationprocessingservice.exception.ConflictException;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.NotFoundException;
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
  public ResponseEntity<Annotation> createAnnotation(@RequestBody Annotation annotation)
      throws DataBaseException, FailedProcessingException, AnnotationValidationException {
    log.info("Received annotation creation request");
    var result = processingService.persistNewAnnotation(annotation, false);
    return ResponseEntity.ok(result);
  }

  @PostMapping(value = "batch", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Annotation> createAnnotationBatch(@RequestBody AnnotationEvent event)
      throws DataBaseException, FailedProcessingException, AnnotationValidationException {
    log.info("Received batch annotation creation request");
    var result = processingService.persistNewAnnotation(event.annotations().get(0), true);
    processingService.batchWebAnnotations(event, result);
    return ResponseEntity.ok(result);
  }

  @PutMapping(value = "/{prefix}/{suffix}", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Annotation> updateAnnotation(
      @PathVariable("prefix") String prefix,
      @PathVariable("suffix") String suffix, @RequestBody Annotation annotation)
      throws DataBaseException, FailedProcessingException, ConflictException, NotFoundException, AnnotationValidationException {
    checkId(prefix, suffix, annotation);
    log.info("Received annotation update request for annotations {}", annotation.getOdsId());
    var result = processingService.updateAnnotation(annotation);
    return ResponseEntity.ok(result);
  }

  @DeleteMapping(value = "/{prefix}/{postfix}")
  public ResponseEntity<Void> archiveAnnotation(@PathVariable("prefix") String prefix,
      @PathVariable("postfix") String postfix) throws IOException, FailedProcessingException {
    var id = prefix + '/' + postfix;
    log.info("Received an archive request for annotations: {}", id);
    processingService.archiveAnnotation(id);
    return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
  }

  private void checkId(String prefix, String suffix, Annotation annotation)
      throws ConflictException {
    var id = prefix + "/" + suffix;
    if (!id.equals(annotation.getOdsId())) {
      log.error("provided id does not match annotations id");
      throw new ConflictException();
    }
  }

}
