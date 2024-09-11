package eu.dissco.annotationprocessingservice.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.diff.JsonDiff;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.schema.Annotation;
import eu.dissco.annotationprocessingservice.schema.CreateUpdateTombstoneEvent;
import eu.dissco.annotationprocessingservice.schema.OdsChangeValue;
import eu.dissco.annotationprocessingservice.schema.ProvActivity;
import eu.dissco.annotationprocessingservice.schema.ProvActivity.Type;
import eu.dissco.annotationprocessingservice.schema.ProvEntity;
import eu.dissco.annotationprocessingservice.schema.ProvValue;
import eu.dissco.annotationprocessingservice.schema.ProvWasAssociatedWith;
import eu.dissco.annotationprocessingservice.schema.ProvWasAssociatedWith.ProvHadRole;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ProvenanceService {

  private final ObjectMapper mapper;
  private final ApplicationProperties properties;

  public CreateUpdateTombstoneEvent generateCreateEvent(Annotation annotation) {
    return generateCreateUpdateTombStoneEvent(annotation, ProvActivity.Type.ODS_CREATE,
        null);
  }

  private CreateUpdateTombstoneEvent generateCreateUpdateTombStoneEvent(
      Annotation annotation, ProvActivity.Type activityType,
      JsonNode jsonPatch) {
    var entityID = annotation.getId() + "/" + annotation.getOdsVersion();
    var activityID = UUID.randomUUID().toString();
    return new CreateUpdateTombstoneEvent()
        .withId(entityID)
        .withType("ods:CreateUpdateTombstoneEvent")
        .withOdsID(entityID)
        .withOdsType(properties.getCreateUpdateTombstoneEventType())
        .withProvActivity(new ProvActivity()
            .withId(activityID)
            .withType(activityType)
            .withOdsChangeValue(mapJsonPatch(jsonPatch))
            .withProvEndedAtTime(Date.from(Instant.now()))
            .withProvWasAssociatedWith(List.of(
                new ProvWasAssociatedWith()
                    .withId(annotation.getDctermsCreator().getId())
                    .withProvHadRole(ProvHadRole.ODS_REQUESTOR),
                new ProvWasAssociatedWith()
                    .withId(annotation.getAsGenerator().getId())
                    .withProvHadRole(ProvHadRole.ODS_GENERATOR)))
            .withProvUsed(entityID)
            .withRdfsComment(getRdfsComment(activityType)))
        .withProvEntity(new ProvEntity()
            .withId(entityID)
            .withType(annotation.getType())
            .withProvValue(mapEntityToProvValue(annotation))
            .withProvWasGeneratedBy(activityID))
        .withOdsHasProvAgent(List.of(annotation.getDctermsCreator(), annotation.getAsGenerator()));
  }

  private static String getRdfsComment(ProvActivity.Type activityType) {
    if (Type.ODS_CREATE.equals(activityType)){
      return "Annotation newly created";
    }
    if (Type.ODS_UPDATE.equals(activityType)){
      return "Annotation updated";
    }
    return "Annotation tombstoned";
  }

  private List<OdsChangeValue> mapJsonPatch(JsonNode jsonPatch) {
    if (jsonPatch == null) {
      return null;
    }
    return mapper.convertValue(jsonPatch, new TypeReference<>() {
    });
  }

  public CreateUpdateTombstoneEvent generateUpdateEvent(Annotation annotation,
      Annotation currentAnnotation) {
    var jsonPatch = createJsonPatch(annotation, currentAnnotation);
    return generateCreateUpdateTombStoneEvent(annotation, ProvActivity.Type.ODS_UPDATE,
        jsonPatch);
  }

  public CreateUpdateTombstoneEvent generateTombstoneEvent(Annotation tombstoneAnnotation,
      Annotation currentAnnotation) {
    var jsonPatch = createJsonPatch(tombstoneAnnotation, currentAnnotation);
    return generateCreateUpdateTombStoneEvent(tombstoneAnnotation, Type.ODS_TOMBSTONE, jsonPatch);
  }

  private ProvValue mapEntityToProvValue(Annotation annotation) {
    var provValue = new ProvValue();
    var node = mapper.convertValue(annotation, new TypeReference<Map<String, Object>>() {
    });
    for (var entry : node.entrySet()) {
      provValue.setAdditionalProperty(entry.getKey(), entry.getValue());
    }
    return provValue;
  }

  private JsonNode createJsonPatch(Annotation annotation, Annotation currentAnnotation) {
    return JsonDiff.asJson(mapper.valueToTree(currentAnnotation), mapper.valueToTree(annotation));
  }
}
