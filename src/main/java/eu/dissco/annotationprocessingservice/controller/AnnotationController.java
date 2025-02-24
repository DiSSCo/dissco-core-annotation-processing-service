package eu.dissco.annotationprocessingservice.controller;

import static eu.dissco.annotationprocessingservice.utils.HandleUtils.removeProxy;

import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.domain.AnnotationTombstoneWrapper;
import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import eu.dissco.annotationprocessingservice.exception.ConflictException;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.NotFoundException;
import eu.dissco.annotationprocessingservice.schema.Annotation;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingEvent;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;
import eu.dissco.annotationprocessingservice.service.ProcessingWebService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
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
@RequestMapping("/annotation")
@RequiredArgsConstructor
public class AnnotationController {

  private final ProcessingWebService processingService;
  protected static final String TARGET_ID_PREFIX_OAS = "Prefix of target ID";
  protected static final String TARGET_ID_SUFFIX_OAS = "Suffix of target ID";

  @Operation(summary = "Create a new annotation")
  @ApiResponses(value = {
      @ApiResponse(responseCode = "201", description = "Annotation successfully created", content = {
          @Content(mediaType = "application/json", schema = @Schema(implementation = Annotation.class))
      })
  })
  @PostMapping(value = "", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Annotation> createAnnotation(
      @RequestBody AnnotationProcessingRequest annotation)
      throws DataBaseException, FailedProcessingException, AnnotationValidationException {
    log.info("Received hashedAnnotation creation request");
    var result = processingService.persistNewAnnotation(annotation, false);
    return ResponseEntity.ok(result);
  }

  @Operation(summary = "Create a batch of annotations", description = """
      Creates multiple annotations based on a single "parent" annotation and provided batch metadata.
      All target objects that match the batch metadata's criteria will be annotated.
      Returns original annotation as result.
      """)
  @ApiResponses(value = {
      @ApiResponse(responseCode = "201", description = "Parent annotation successfully created. Child annotations successfully scheduled.", content = {
          @Content(mediaType = "application/json", schema = @Schema(implementation = Annotation.class))
      })
  })
  @PostMapping(value = "batch", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Annotation> createAnnotationBatch(
      @RequestBody AnnotationProcessingEvent event)
      throws DataBaseException, FailedProcessingException, AnnotationValidationException {
    log.info("Received batch hashedAnnotation creation request");
    var result = processingService.persistNewAnnotation(event.getAnnotations().get(0), true);
    processingService.batchWebAnnotations(event, result);
    return ResponseEntity.ok(result);
  }

  @Operation(summary = "Update an existing annotation")
  @ApiResponses(value = {
      @ApiResponse(responseCode = "200", description = "Annotation successfully updated", content = {
          @Content(mediaType = "application/json", schema = @Schema(implementation = Annotation.class))
      })
  })
  @PutMapping(value = "/{prefix}/{suffix}", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Annotation> updateAnnotation(
      @Parameter(description = TARGET_ID_PREFIX_OAS) @PathVariable("prefix") String prefix,
      @Parameter(description = TARGET_ID_SUFFIX_OAS) @PathVariable("suffix") String suffix,
      @RequestBody AnnotationProcessingRequest annotation)
      throws DataBaseException, FailedProcessingException, ConflictException, NotFoundException, AnnotationValidationException {
    checkId(prefix, suffix, annotation);
    log.info("Received hashedAnnotation update request for annotations {}",
        annotation.getDctermsIdentifier());
    var result = processingService.updateAnnotation(annotation);
    return ResponseEntity.ok(result);
  }

  @Operation(summary = "Tombstone a given annotation")
  @ApiResponses(value = {
      @ApiResponse(responseCode = "204", description = "Annotation successfully tombstoned")
  })
  @DeleteMapping(value = "/{prefix}/{suffix}")
  public ResponseEntity<Void> tombstoneAnnotation(
      @Parameter(description = TARGET_ID_PREFIX_OAS) @PathVariable("prefix") String prefix,
      @Parameter(description = TARGET_ID_SUFFIX_OAS) @PathVariable("suffix") String suffix,
      @RequestBody AnnotationTombstoneWrapper annotationTombstoneWrapper)
      throws IOException, FailedProcessingException {
    var id = prefix + '/' + suffix;
    log.info("Received an archive request for annotations: {}", id);
    var annotation = annotationTombstoneWrapper.annotation();
    var tombstoningAgent = annotationTombstoneWrapper.tombstoningAgent();
    if (!annotation.getId().contains(id)) {
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
