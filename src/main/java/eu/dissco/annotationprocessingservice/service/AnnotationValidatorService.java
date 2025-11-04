package eu.dissco.annotationprocessingservice.service;

import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.properties.FdoProperties;
import eu.dissco.annotationprocessingservice.repository.DigitalSpecimenRepository;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingEvent;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;
import io.github.dissco.annotationlogic.exception.InvalidAnnotationException;
import io.github.dissco.annotationlogic.exception.InvalidTargetException;
import io.github.dissco.annotationlogic.validator.AnnotationValidator;
import io.github.dissco.core.annotationlogic.schema.Agent;
import io.github.dissco.core.annotationlogic.schema.Annotation;
import io.github.dissco.core.annotationlogic.schema.Annotation.OaMotivation;
import io.github.dissco.core.annotationlogic.schema.Annotation.OdsStatus;
import io.github.dissco.core.annotationlogic.schema.DigitalSpecimen;
import io.github.dissco.core.annotationlogic.schema.Identifier;
import io.github.dissco.core.annotationlogic.schema.Identifier.DctermsType;
import io.github.dissco.core.annotationlogic.schema.Identifier.OdsGupriLevel;
import io.github.dissco.core.annotationlogic.schema.Identifier.OdsIdentifierStatus;
import io.github.dissco.core.annotationlogic.schema.OaHasSelector;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class AnnotationValidatorService {

  private final AnnotationValidator annotationValidator;
  private final DigitalSpecimenRepository digitalSpecimenRepository;
  private final FdoProperties fdoProperties;
  private static final String PLACEHOLDER_HANDLE = "https://hdl.handle.net/20.5000.1025/AAA-BBB-CCC";
  private static final int PLACEHOLDER_VERSION = 1;

  public void validateAnnotationRequest(
      List<AnnotationProcessingRequest> annotationProcessingRequests, boolean isNewAnnotation)
      throws AnnotationValidationException {
    for (var annotationProcessingRequest : annotationProcessingRequests) {
      validateId(annotationProcessingRequest, isNewAnnotation);
    }
    validateAnnotationContents(annotationProcessingRequests);
  }

  public void validateEvent(AnnotationProcessingEvent event) throws AnnotationValidationException {
    validateAnnotationContents(event.getAnnotations());
  }

  private void validateAnnotationContents(
      List<AnnotationProcessingRequest> annotationProcessingRequests)
      throws AnnotationValidationException {
    var annotations = annotationProcessingRequests.stream().map(this::toAnnotation).toList();
    var specimenTargets = getSpecimenTargets(annotationProcessingRequests);
    try {
      for (var annotation : annotations) {
        var target = specimenTargets.get(annotation.getOaHasTarget().getDctermsIdentifier());
        if (target != null) {
          annotationValidator.applyAnnotation(target, annotation);
        }
      }
    } catch (InvalidTargetException | InvalidAnnotationException e) {
      log.error("Annotation validation failed", e);
      throw new AnnotationValidationException(e.getMessage());
    }
  }

  private Map<String, DigitalSpecimen> getSpecimenTargets(
      List<AnnotationProcessingRequest> annotationProcessingRequests) {
    var specimenIds = annotationProcessingRequests.stream().filter(
            annotationProcessingRequest -> annotationProcessingRequest.getOaHasTarget().getType()
                .equals("ods:DigitalSpecimen"))
        .map(annotationProcessingRequest -> annotationProcessingRequest.getOaHasTarget().getId())
        .collect(Collectors.toSet());
    var specimens = digitalSpecimenRepository.getDigitalSpecimenTargets(specimenIds);
    return specimens.stream()
        .collect(Collectors.toMap(
            DigitalSpecimen::getDctermsIdentifier,
            Function.identity()
        ));
  }

  private Annotation toAnnotation(AnnotationProcessingRequest annotationProcessingRequest) {
    var timestamp = Instant.now();
    return new Annotation()
        .withId(PLACEHOLDER_HANDLE)
        .withType("ods:Annotation")
        .withDctermsIdentifier(PLACEHOLDER_HANDLE)
        .withOdsFdoType(fdoProperties.getType())
        .withOdsVersion(PLACEHOLDER_VERSION)
        .withOdsStatus(OdsStatus.ACTIVE)
        .withOaMotivation(
            OaMotivation.fromValue(annotationProcessingRequest.getOaMotivation().value()))
        .withOaMotivatedBy(annotationProcessingRequest.getOaMotivatedBy())
        .withOaHasTarget(new io.github.dissco.core.annotationlogic.schema.AnnotationTarget()
            .withDctermsIdentifier(
                annotationProcessingRequest.getOaHasTarget().getDctermsIdentifier())
            .withId(annotationProcessingRequest.getOaHasTarget().getId())
            .withType(annotationProcessingRequest.getOaHasTarget().getType())
            .withOdsFdoType(annotationProcessingRequest.getOaHasTarget().getOdsFdoType())
            .withOaHasSelector(
                toSelector(annotationProcessingRequest.getOaHasTarget().getOaHasSelector()))
        )
        .withOaHasBody(new io.github.dissco.core.annotationlogic.schema.AnnotationBody()
            .withOaValue(annotationProcessingRequest.getOaHasBody().getOaValue())
            .withType(annotationProcessingRequest.getOaHasBody().getType())
            .withOdsScore(annotationProcessingRequest.getOaHasBody().getOdsScore())
            .withDctermsReferences(
                annotationProcessingRequest.getOaHasBody().getDctermsReferences()))
        .withDctermsCreator(getAgent(annotationProcessingRequest))
        .withAsGenerator(getAgent(annotationProcessingRequest))
        .withDctermsIssued(Date.from(timestamp))
        .withDctermsModified(Date.from(timestamp))
        .withDctermsCreated(annotationProcessingRequest.getDctermsCreated())
        .withOdsPlaceInBatch(annotationProcessingRequest.getOdsPlaceInBatch())
        .withOdsBatchID(annotationProcessingRequest.getOdsBatchID());
  }

  private static Agent getAgent(AnnotationProcessingRequest annotationProcessingRequest) {
    return new Agent()
        .withId(annotationProcessingRequest.getDctermsCreator().getId())
        .withType(Agent.Type.fromValue(
            annotationProcessingRequest.getDctermsCreator().getType().value()))
        .withOdsHasIdentifiers(toIdentifiers(annotationProcessingRequest.getDctermsCreator()
            .getOdsHasIdentifiers()));
  }

  private static OaHasSelector toSelector(
      eu.dissco.annotationprocessingservice.schema.OaHasSelector requestSelector) {
    var selector = new OaHasSelector();
    for (var property : requestSelector.getAdditionalProperties().entrySet()) {
      selector = selector.withAdditionalProperty(property.getKey(), property.getValue());
    }
    return selector;
  }

  private static List<Identifier> toIdentifiers(
      List<eu.dissco.annotationprocessingservice.schema.Identifier> requestIdentifiers) {
    var identifierList = new ArrayList<Identifier>();
    for (var identifier : requestIdentifiers) {
      identifierList.add(
          new Identifier()
              .withId(identifier.getId())
              .withType(identifier.getType())
              .withDctermsTitle(identifier.getDctermsTitle())
              .withDctermsType(DctermsType.fromValue(identifier.getDctermsType().value()))
              .withDctermsIdentifier(identifier.getDctermsIdentifier())
              .withDctermsFormat(identifier.getDctermsFormat())
              .withDctermsSubject(identifier.getDctermsSubject())
              .withOdsIsPartOfLabel(identifier.getOdsIsPartOfLabel())
              .withOdsGupriLevel(OdsGupriLevel.fromValue(identifier.getOdsGupriLevel().value()))
              .withOdsIdentifierStatus(
                  OdsIdentifierStatus.fromValue(identifier.getOdsIdentifierStatus().value()))
      );
    }
    return identifierList;
  }

  private static void validateId(AnnotationProcessingRequest annotation, Boolean isNewAnnotation)
      throws AnnotationValidationException {
    if (Boolean.TRUE.equals(isNewAnnotation) && annotation.getId() != null) {
      log.warn("Attempting overwrite annotation with \"@id\" {}", annotation.getId());
      throw new AnnotationValidationException("New annotations can not have @id");
    }
    if (Boolean.FALSE.equals(isNewAnnotation) && annotation.getId() == null) {
      log.error("\"@id\" not provided for annotation update");
      throw new AnnotationValidationException("Missing @id for annotation update");
    }
  }

}
