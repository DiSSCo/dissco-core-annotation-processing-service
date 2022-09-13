package eu.dissco.annotationprocessingservice.controller;

import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.domain.AnnotationRecord;
import eu.dissco.annotationprocessingservice.service.ProcessingService;
import javax.xml.transform.TransformerException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@Profile(Profiles.WEB)
@RestController
@RequestMapping("/")
@RequiredArgsConstructor
public class AnnotationController {

  private final ProcessingService processingService;

  @PostMapping(produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<AnnotationRecord> createAnnotation(@RequestBody AnnotationEvent event)
      throws TransformerException {
    log.error("Received annotation request");
    var result = processingService.handleMessages(event);
    return ResponseEntity.ok(result);
  }

  @DeleteMapping(value = "/{prefix}/{postfix}")
  public ResponseEntity<Void> archiveAnnotation(@PathVariable("prefix") String prefix,
      @PathVariable("postfix") String postfix){
    var id = prefix + '/' + postfix;
    log.info("Received a archive request for annotation: {}", id);
    processingService.archiveAnnotation(id);
    return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
  }
}
