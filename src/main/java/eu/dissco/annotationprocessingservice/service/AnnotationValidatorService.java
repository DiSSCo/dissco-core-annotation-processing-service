package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.configuration.ApplicationConfiguration.DOI_PROXY;
import static eu.dissco.annotationprocessingservice.domain.AnnotationTargetType.DIGITAL_SPECIMEN;
import static eu.dissco.annotationprocessingservice.utils.ServiceUtils.createGenerator;
import static eu.dissco.annotationprocessingservice.utils.ServiceUtils.isTransformativeMotivation;

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
import io.github.dissco.core.annotationlogic.schema.AnnotationBody;
import io.github.dissco.core.annotationlogic.schema.AnnotationTarget;
import io.github.dissco.core.annotationlogic.schema.DigitalSpecimen;
import io.github.dissco.core.annotationlogic.schema.Identifier;
import io.github.dissco.core.annotationlogic.schema.Identifier.DctermsType;
import io.github.dissco.core.annotationlogic.schema.Identifier.OdsGupriLevel;
import io.github.dissco.core.annotationlogic.schema.Identifier.OdsIdentifierStatus;
import io.github.dissco.core.annotationlogic.schema.OaHasSelector;
import io.github.dissco.core.annotationlogic.schema.OdsHasRole;
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
  private final ApplicationProperties applicationProperties;
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
    var specimenAnnotations = annotationProcessingRequests.stream()
        .filter(annotationProcessingRequest -> DIGITAL_SPECIMEN.getFdoType()
            .equals(annotationProcessingRequest.getOaHasTarget().getOdsFdoType()))
        .filter(annotationProcessingRequest ->
            isTransformativeMotivation(annotationProcessingRequest.getOaMotivation()))
        .map(this::toAnnotation).toList();
    var specimenTargets = getSpecimenTargets(annotationProcessingRequests);
    try {
      for (var annotation : specimenAnnotations) {
        var target = specimenTargets.get(annotation.getOaHasTarget().getDctermsIdentifier());
        if (target != null) {
          annotationValidator.applyAnnotation(target, annotation);
        } else {
          log.warn(
              "Annotation attempting to target a specimen that does not exist. Can not find target {}",
              annotation.getOaHasTarget().getDctermsIdentifier());
          throw new AnnotationValidationException(
              "Target " + annotation.getOaHasTarget().getDctermsIdentifier()
                  + " of incoming annotation does not exist");
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
            annotationProcessingRequest ->
                "ods:DigitalSpecimen".equals(annotationProcessingRequest.getOaHasTarget().getType())
                    || DIGITAL_SPECIMEN.getFdoType().equals(
                    annotationProcessingRequest.getOaHasTarget().getType()))
        .map(annotationProcessingRequest -> annotationProcessingRequest.getOaHasTarget().getId())
        .map(id -> id.replace(DOI_PROXY, ""))
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
        .withOaMotivatedBy(annotationProcessingRequest.getOaMotivatedBy())
        .withOaHasTarget(getTarget(annotationProcessingRequest))
        .withOaHasBody(getBody(annotationProcessingRequest))
        .withDctermsCreator(getAgent(annotationProcessingRequest.getDctermsCreator()))
        .withAsGenerator(getAgent(createGenerator(applicationProperties)))
        .withDctermsIssued(Date.from(timestamp))
        .withDctermsModified(Date.from(timestamp))
        .withDctermsCreated(annotationProcessingRequest.getDctermsCreated())
        .withOdsPlaceInBatch(annotationProcessingRequest.getOdsPlaceInBatch())
        .withOdsBatchID(annotationProcessingRequest.getOdsBatchID())
        .withOaMotivation(
            OaMotivation.fromValue(annotationProcessingRequest.getOaMotivation().value()));
  }

  private static AnnotationTarget getTarget(
      AnnotationProcessingRequest annotationProcessingRequest) {
    var target = new AnnotationTarget();
    target
        .withDctermsIdentifier(
            annotationProcessingRequest.getOaHasTarget().getDctermsIdentifier())
        .withId(annotationProcessingRequest.getOaHasTarget().getId())
        .withType(annotationProcessingRequest.getOaHasTarget().getType())
        .withOdsFdoType(annotationProcessingRequest.getOaHasTarget().getOdsFdoType())
        .withOaHasSelector(
            toSelector(annotationProcessingRequest.getOaHasTarget().getOaHasSelector()));
    return target;
  }

  private static AnnotationBody getBody(AnnotationProcessingRequest annotationProcessingRequest) {
    if (annotationProcessingRequest.getOaHasBody() != null) {
      return new io.github.dissco.core.annotationlogic.schema.AnnotationBody()
          .withOaValue(annotationProcessingRequest.getOaHasBody().getOaValue())
          .withType(annotationProcessingRequest.getOaHasBody().getType())
          .withOdsScore(annotationProcessingRequest.getOaHasBody().getOdsScore())
          .withDctermsReferences(
              annotationProcessingRequest.getOaHasBody().getDctermsReferences());
    } else {
      return new AnnotationBody();
    }
  }

  private static Agent getAgent(
      eu.dissco.annotationprocessingservice.schema.Agent requestAgent) {
    var newAgent = new Agent();
    newAgent
        .withId(requestAgent.getId())
        .withSchemaName(requestAgent.getSchemaName())
        .withOdsHasIdentifiers(toIdentifiers(requestAgent.getOdsHasIdentifiers()));
    if (requestAgent.getType() != null) {
      newAgent.withType(Agent.Type.fromValue(
          requestAgent.getType().value()));
    }
    var roles = new ArrayList<OdsHasRole>();
    for (var role : requestAgent.getOdsHasRoles()) {
      roles.add(
          new OdsHasRole()
              .withSchemaRoleName(role.getSchemaRoleName())
              .withSchemaStartDate(role.getSchemaStartDate())
              .withSchemaEndDate(role.getSchemaEndDate())
              .withType(role.getType())
      );
      newAgent.withOdsHasRoles(roles);
    }

    return newAgent;
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
      var newIdentifier = new Identifier()
          .withId(identifier.getId())
          .withType(identifier.getType())
          .withDctermsTitle(identifier.getDctermsTitle())
          .withDctermsIdentifier(identifier.getDctermsIdentifier())
          .withDctermsFormat(identifier.getDctermsFormat())
          .withDctermsSubject(identifier.getDctermsSubject())
          .withOdsIsPartOfLabel(identifier.getOdsIsPartOfLabel());

      if (identifier.getDctermsType() != null) {
        newIdentifier.withDctermsType(DctermsType.fromValue(identifier.getDctermsType().value()));
      }
      if (identifier.getOdsIdentifierStatus() != null) {
        newIdentifier.withOdsIdentifierStatus(
            OdsIdentifierStatus.fromValue(identifier.getOdsIdentifierStatus().value()));
      }
      if (identifier.getOdsGupriLevel() != null) {
        newIdentifier.withOdsGupriLevel(
            OdsGupriLevel.fromValue(identifier.getOdsGupriLevel().value()));
      }
      identifierList.add(newIdentifier);
    }
    return identifierList;
  }

  private static void validateId(AnnotationProcessingRequest annotation, boolean isNewAnnotation)
      throws AnnotationValidationException {
    if (isNewAnnotation && annotation.getId() != null) {
      log.warn("Attempting overwrite annotation with \"@id\" {}", annotation.getId());
      throw new AnnotationValidationException("New annotations can not have @id");
    } else if (!isNewAnnotation && annotation.getId() == null) {
      log.error("\"@id\" not provided for annotation update");
      throw new AnnotationValidationException("Missing @id for annotation update");
    }
  }

}
