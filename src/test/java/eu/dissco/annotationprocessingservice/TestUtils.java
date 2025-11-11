package eu.dissco.annotationprocessingservice;

import static eu.dissco.annotationprocessingservice.domain.AgentRoleType.PROCESSING_SERVICE;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import eu.dissco.annotationprocessingservice.configuration.DateDeserializer;
import eu.dissco.annotationprocessingservice.configuration.DateSerializer;
import eu.dissco.annotationprocessingservice.configuration.InstantDeserializer;
import eu.dissco.annotationprocessingservice.configuration.InstantSerializer;
import eu.dissco.annotationprocessingservice.domain.AnnotationTargetType;
import eu.dissco.annotationprocessingservice.domain.AutoAcceptedAnnotation;
import eu.dissco.annotationprocessingservice.domain.FailedMasEvent;
import eu.dissco.annotationprocessingservice.domain.HashedAnnotation;
import eu.dissco.annotationprocessingservice.domain.HashedAnnotationRequest;
import eu.dissco.annotationprocessingservice.domain.ProcessedAnnotationBatch;
import eu.dissco.annotationprocessingservice.schema.Agent;
import eu.dissco.annotationprocessingservice.schema.Agent.Type;
import eu.dissco.annotationprocessingservice.schema.Annotation;
import eu.dissco.annotationprocessingservice.schema.Annotation.OaMotivation;
import eu.dissco.annotationprocessingservice.schema.Annotation.OdsMergingDecisionStatus;
import eu.dissco.annotationprocessingservice.schema.Annotation.OdsStatus;
import eu.dissco.annotationprocessingservice.schema.AnnotationBatchMetadata;
import eu.dissco.annotationprocessingservice.schema.AnnotationBody;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingEvent;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;
import eu.dissco.annotationprocessingservice.schema.AnnotationTarget;
import eu.dissco.annotationprocessingservice.schema.Identifier.DctermsType;
import eu.dissco.annotationprocessingservice.schema.OaHasSelector;
import eu.dissco.annotationprocessingservice.schema.OdsHasAggregateRating;
import eu.dissco.annotationprocessingservice.schema.SearchParam;
import eu.dissco.annotationprocessingservice.schema.TombstoneMetadata;
import eu.dissco.annotationprocessingservice.utils.AgentUtils;
import io.github.dissco.core.annotationlogic.schema.DigitalSpecimen;
import io.github.dissco.core.annotationlogic.schema.DigitalSpecimen.OdsLivingOrPreserved;
import io.github.dissco.core.annotationlogic.schema.DigitalSpecimen.OdsPhysicalSpecimenIDType;
import io.github.dissco.core.annotationlogic.schema.DigitalSpecimen.OdsTopicDiscipline;
import io.github.dissco.core.annotationlogic.schema.Identification;
import io.github.dissco.core.annotationlogic.schema.TaxonIdentification;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TestUtils {

  public static final ObjectMapper MAPPER;

  public static final String ID = "https://hdl.handle.net/20.5000.1025/KZL-VC0-ZK2";
  public static final String BARE_ID = "20.5000.1025/KZL-VC0-ZK2";
  public static final String ID_ALT = "https://hdl.handle.net/20.5000.1025/ZZZ-YYY-XXX";
  public static final String TARGET_ID = "20.5000.1025/QRS-123-ABC";
  public static final Instant CREATED = Instant.parse("2023-02-17T09:50:27.391Z");
  public static final Instant UPDATED = Instant.parse("2024-02-17T09:50:27.391Z");
  public static final String CREATOR = "3fafe98f-1bf9-4927-b9c7-4ba070761a72";
  public static final String JOB_ID = "20.5000.1025/7YC-RGZ-LL1";
  public static final String PROCESSOR_NAME = "annotation-processing-service";
  public static final String PROCESSOR_HANDLE = "https://hdl.handle.net/anno-process-service-pid";
  public static final String FDO_TYPE = "https://doi.org/21.T11148/cf458ca9ee1d44a5608f";
  public static final UUID ANNOTATION_HASH = UUID.fromString(
      "3a36d684-deb8-8779-2753-caef497e9ed8");

  public static final UUID ANNOTATION_HASH_2 = UUID.fromString(
      "f43e4ec6-ca1c-4a88-9aac-08f6da4b0b1c");
  public static final UUID ANNOTATION_HASH_3 = UUID.fromString(
      "53502490-24cc-4a93-a1ce-e80f5e77f506");
  public static final UUID BATCH_ID = UUID.fromString("30468044-9198-4335-893a-665574e5f61e");
  public static final UUID BATCH_ID_ALT = UUID.fromString("642a04b8-7a4c-4e53-89e3-0ab1dbab768e");
  public static final String ANNOTATION_JSONB = """
      [
        "https://hdl.handle.net/20.5000.1025/KZL-VC0-ZK2"
       ]
      """;
  public static final String HANDLE_PROXY = "https://hdl.handle.net/";
  public static final String DOI_PROXY = "https://doi.org/";
  public static final String COMMENT = "a comment";

  static {
    var mapper = new ObjectMapper().findAndRegisterModules();
    SimpleModule dateModule = new SimpleModule();
    dateModule.addSerializer(Instant.class, new InstantSerializer());
    dateModule.addDeserializer(Instant.class, new InstantDeserializer());
    dateModule.addSerializer(Date.class, new DateSerializer());
    dateModule.addDeserializer(Date.class, new DateDeserializer());
    mapper.registerModule(dateModule);
    mapper.setSerializationInclusion(Include.NON_NULL);
    MAPPER = mapper.copy();
  }

  public static HashedAnnotation givenHashedAnnotation() {
    return new HashedAnnotation(givenAnnotationProcessed(), ANNOTATION_HASH);
  }

  public static HashedAnnotationRequest givenHashedAnnotationRequest() {
    return new HashedAnnotationRequest(givenAnnotationRequest(), ANNOTATION_HASH);
  }

  public static HashedAnnotation givenHashedAnnotation(UUID batchId) {
    return new HashedAnnotation(givenHashedAnnotation().annotation().withOdsBatchID(batchId),
        ANNOTATION_HASH);
  }

  public static HashedAnnotation givenHashedAnnotation(String id) {
    return new HashedAnnotation(
        givenHashedAnnotation().annotation().withOdsBatchID(BATCH_ID).withId(id),
        ANNOTATION_HASH);
  }

  public static HashedAnnotation givenHashedAnnotationAlt() {
    return new HashedAnnotation(givenAnnotationProcessedAlt(), ANNOTATION_HASH);
  }

  public static Annotation givenAnnotationProcessed(String id) {
    return givenAnnotationProcessed(id, CREATOR, TARGET_ID);
  }

  public static Annotation givenAnnotationProcessed() {
    return givenAnnotationProcessed(ID);
  }

  public static Annotation givenAnnotationProcessedWeb() {
    return givenAnnotationProcessedWeb(ID, CREATOR, TARGET_ID);
  }

  public static Annotation givenAnnotationProcessedWebBatch() {
    return givenAnnotationProcessedWeb(ID, CREATOR, TARGET_ID)
        .withOdsBatchID(BATCH_ID)
        .withOdsPlaceInBatch(1);
  }

  public static Annotation givenAnnotationProcessedWeb(String annotationId, String userId,
      String targetId) {
    return new Annotation()
        .withId(annotationId)
        .withDctermsIdentifier(annotationId)
        .withType("ods:Annotation")
        .withOdsFdoType(FDO_TYPE)
        .withOdsVersion(1)
        .withOdsStatus(OdsStatus.ACTIVE)
        .withOaHasBody(givenOaBody())
        .withOaMotivation(OaMotivation.OA_COMMENTING)
        .withOaHasTarget(givenOaTarget(targetId))
        .withDctermsCreator(givenCreator(userId))
        .withDctermsCreated(Date.from(CREATED))
        .withDctermsIssued(Date.from(CREATED))
        .withDctermsModified(Date.from(CREATED))
        .withAsGenerator(givenGenerator());
  }

  public static Annotation givenAnnotationProcessedAlt() {
    return givenAnnotationProcessed(ID, CREATOR, TARGET_ID)
        .withOaMotivation(OaMotivation.OA_EDITING);
  }

  public static Annotation givenAnnotationProcessed(String annotationId, String userId,
      String targetId) {
    return givenAnnotationProcessedWeb(annotationId, userId, targetId)
        .withOdsJobID(HANDLE_PROXY + JOB_ID);
  }

  public static AnnotationProcessingRequest givenAnnotationRequest(String targetId) {
    return new AnnotationProcessingRequest()
        .withOaHasBody(givenOaBody(COMMENT))
        .withOaMotivation(AnnotationProcessingRequest.OaMotivation.OA_COMMENTING)
        .withOaHasTarget(givenRequestOaTarget(targetId))
        .withDctermsCreated(Date.from(CREATED))
        .withDctermsCreator(givenCreator(CREATOR));
  }

  public static AnnotationProcessingRequest givenAnnotationRequest() {
    return givenAnnotationRequest(TARGET_ID);
  }

  public static Annotation givenBaseAnnotationForBatch(int placeInBatch, String id, UUID bachId) {
    return givenAnnotationProcessed()
        .withOdsBatchID(bachId)
        .withId(id)
        .withOdsPlaceInBatch(placeInBatch);
  }

  public static AnnotationBody givenOaBody() {
    return givenOaBody(COMMENT);
  }

  public static AnnotationBody givenOaBody(String value) {
    return new AnnotationBody()
        .withType("oa:TextualBody")
        .withOaValue(new ArrayList<>(List.of(value)))
        .withDctermsReferences(
            "https://medialib.naturalis.nl/file/id/ZMA.UROCH.P.1555/format/large")
        .withOdsScore(0.99);
  }

  public static AnnotationBody givenOaBodySetType(String type) {
    return new AnnotationBody()
        .withType(type)
        .withOaValue(new ArrayList<>(List.of(COMMENT)))
        .withDctermsReferences(
            "https://medialib.naturalis.nl/file/id/ZMA.UROCH.P.1555/format/large")
        .withOdsScore(0.99);
  }

  public static AnnotationTarget givenOaTarget(String targetId) {
    return givenOaTarget(targetId, AnnotationTargetType.DIGITAL_SPECIMEN);
  }

  public static AnnotationTarget givenRequestOaTarget(String targetId) {
    return givenRequestOaTarget(targetId, AnnotationTargetType.DIGITAL_SPECIMEN);
  }

  public static AnnotationTarget givenRequestOaTarget(String targetId,
      AnnotationTargetType targetType) {
    return givenRequestOaTarget(targetId, targetType, givenRequestSelector());
  }

  public static AnnotationTarget givenRequestOaTarget(String targetId,
      AnnotationTargetType targetType,
      OaHasSelector selector) {
    return new AnnotationTarget()
        .withId(DOI_PROXY + targetId)
        .withType("ods:DigitalSpecimen")
        .withOdsFdoType(targetType.toString())
        .withDctermsIdentifier(DOI_PROXY + targetId)
        .withOaHasSelector(selector);
  }

  public static OaHasSelector givenRequestSelector() {
    return new OaHasSelector()
        .withAdditionalProperty("ods:term",
            "$['ods:hasEvents'][0]['ods:hasLocation']['dwc:locality']")
        .withAdditionalProperty("@type", "ods:TermSelector");
  }

  public static AnnotationTarget givenOaTarget(String targetId, AnnotationTargetType targetType) {
    return new AnnotationTarget()
        .withId(DOI_PROXY + targetId)
        .withType("ods:DigitalSpecimen")
        .withOdsFdoType(targetType.toString())
        .withDctermsIdentifier(DOI_PROXY + targetId)
        .withOaHasSelector(givenSelector());
  }

  public static AnnotationTarget givenOaTarget(String targetId, AnnotationTargetType targetType,
      OaHasSelector selector) {
    return new AnnotationTarget()
        .withId(DOI_PROXY + targetId)
        .withDctermsIdentifier(DOI_PROXY + targetId)
        .withOdsFdoType(targetType.toString())
        .withOaHasSelector(selector)
        .withType("ods:DigitalSpecimen");
  }

  public static OaHasSelector givenSelector() {
    return new OaHasSelector()
        .withAdditionalProperty("ods:term",
            "$['ods:hasEvents'][0]['ods:hasLocation']['dwc:locality']")
        .withAdditionalProperty("@type", "ods:TermSelector");
  }

  public static AnnotationTarget givenOaTarget(OaHasSelector selector) {
    return givenOaTarget(BARE_ID, AnnotationTargetType.DIGITAL_SPECIMEN, selector);
  }

  public static OaHasSelector givenSelector(String field) {
    return new OaHasSelector()
        .withAdditionalProperty("ods:term", field)
        .withAdditionalProperty("@type", "ods:TermSelector");
  }


  public static Agent givenCreator(String userId) {
    return new Agent()
        .withSchemaName("Test User")
        .withId(userId)
        .withType(Type.SCHEMA_PERSON);
  }

  public static Agent givenGenerator() {
    return AgentUtils.createAgent(PROCESSOR_NAME, PROCESSOR_HANDLE, PROCESSING_SERVICE,
        DctermsType.DOI.value(), Type.SCHEMA_SOFTWARE_APPLICATION);
  }

  public static OdsHasAggregateRating givenAggregationRating() {
    return givenAggregationRating(0.1);
  }

  public static OdsHasAggregateRating givenAggregationRating(double ratingValue) {
    return new OdsHasAggregateRating()
        .withSchemaRatingValue(ratingValue)
        .withType("schema:AggregateRating")
        .withSchemaRatingCount(2);
  }


  public static AnnotationProcessingEvent givenAnnotationEvent() {
    return givenAnnotationEvent(givenAnnotationRequest());
  }

  public static AnnotationProcessingEvent givenAnnotationEvent(
      AnnotationProcessingRequest annotation) {
    return new AnnotationProcessingEvent()
        .withJobId(JOB_ID)
        .withAnnotations(List.of(annotation));
  }

  public static List<JsonNode> givenPostRequest() throws Exception {
    return List.of(MAPPER.readTree("""
        {
            "data": {
              "type": "https://doi.org/21.T11148/cf458ca9ee1d44a5608f",
              "attributes": {
               "targetPid":"https://doi.org/20.5000.1025/QRS-123-ABC",
               "targetType":"https://doi.org/21.T11148/894b1e6cad57e921764e"
              }
            }
          }
        """));
  }

  public static List<JsonNode> givenPatchRequest() throws Exception {
    return List.of(MAPPER.readTree("""
        {
            "data": {
              "type": "https://doi.org/21.T11148/cf458ca9ee1d44a5608f",
              "attributes": {
                "targetPid":"https://doi.org/20.5000.1025/QRS-123-ABC",
                "targetType":"https://doi.org/21.T11148/894b1e6cad57e921764e"
            },
            "id":"https://hdl.handle.net/20.5000.1025/KZL-VC0-ZK2"
          }
        }
        """));
  }

  public static JsonNode givenRollbackCreationRequest() throws Exception {
    return MAPPER.readTree("""
        {
          "data": [
            {"id":"https://hdl.handle.net/20.5000.1025/KZL-VC0-ZK2"}
          ]
        }
        """);
  }

  public static SearchParam givenSearchParamCountry() {
    return new SearchParam(
        "$['ods:hasEvents'][*]['ods:hasLocation']['dwc:country']",
        "Netherlands");
  }

  public static AnnotationBatchMetadata givenAnnotationBatchMetadataLatitudeSearch() {
    return new AnnotationBatchMetadata(1,
        List.of(new SearchParam(
            "ods:hasEvents[*].ods:hasLocation.ods:hasGeoreference.dwc:decimalLatitude",
            "52.123")));
  }

  public static AnnotationBatchMetadata givenAnnotationBatchMetadataTwoParam() {
    return new AnnotationBatchMetadata(1, List.of(
        givenSearchParamCountry(),
        new SearchParam(
            "$['ods:hasEvents'][*]['dwc:eventRemarks']",
            "Correct"
        )));
  }

  public static AnnotationBatchMetadata givenAnnotationBatchMetadataOneParam() {
    return new AnnotationBatchMetadata(1, List.of(
        givenSearchParamCountry()));
  }

  public static ProcessedAnnotationBatch givenAnnotationEventBatchEnabled() {
    return new ProcessedAnnotationBatch(List.of(givenBaseAnnotationForBatch(1, ID, BATCH_ID)
        .withOdsPlaceInBatch(1)), JOB_ID,
        List.of(givenAnnotationBatchMetadataLatitudeSearch()), null);
  }

  public static Annotation givenAcceptedAnnotation() {
    var annotation = givenAnnotationProcessedWeb();
    annotation.setOdsMergingStateChangeDate(Date.from(CREATED));
    annotation.setOdsHasMergingStateChangedBy(givenProcessingAgent());
    return annotation;
  }

  public static Annotation givenAcceptedAnnotation(String id) {
    var annotation = givenAnnotationProcessedWeb(id, CREATOR, TARGET_ID);
    annotation.setOdsMergingStateChangeDate(Date.from(CREATED));
    annotation.setOdsMergingDecisionStatus(OdsMergingDecisionStatus.APPROVED);
    annotation.setOdsHasMergingStateChangedBy(givenProcessingAgent());
    return annotation;
  }

  public static AutoAcceptedAnnotation givenAutoAcceptedRequest() {
    return new AutoAcceptedAnnotation(givenProcessingAgent(), givenAnnotationRequest());
  }

  public static Agent givenProcessingAgent() {
    return AgentUtils.createAgent("processing-service", ID, PROCESSING_SERVICE,
        DctermsType.DOI.value(), Type.SCHEMA_SOFTWARE_APPLICATION);
  }

  public static JsonNode givenElasticDocument() {
    return givenElasticDocument("Netherlands", DOI_PROXY + BARE_ID);
  }

  public static JsonNode givenElasticDocument(String id) {
    return givenElasticDocument("Netherlands", id);
  }

  public static JsonNode givenElasticDocument(String country, String id) {
    try {
      return MAPPER.readTree(
          """
              {
               "@id":  \"""" + id + """
              ",
               "@type": "ods:DigitalSpecimen",
               "dcterms:identifier": "https://doi.org/20.5000.1025/KZL-VC0-ZK2",
               "ods:fdoType": "https://doi.org/21.T11148/894b1e6cad57e921764e",
               "ods:midsLevel": 0,
               "ods:version": 4,
               "dcterms:created": "2022-11-01T09:59:24.000Z",
               "ods:physicalSpecimenID": "123",
               "ods:physicalSpecimenIDType": "Resolvable",
               "ods:isMarkedAsType": true,
               "ods:isKnownToContainMedia": true,
               "ods:specimenName": "Abyssothyris Thomson, 1927",
               "ods:sourceSystemID": "https://hdl.handle.net/20.5000.1025/3XA-8PT-SAY",
               "dcterms:license": "http://creativecommons.org/licenses/by/4.0/legalcode",
               "dcterms:modified": "03/12/2012",
               "dwc:preparations": "",
               "ods:organisationID": "https://ror.org/0349vqz63",
               "ods:organisationName": "Royal Botanic Garden Edinburgh Herbarium",
               "dwc:datasetName": "Royal Botanic Garden Edinburgh Herbarium",
               "ods:hasEvents": [
                 {
                   "dwc:eventRemarks": "Correct",
                   "annotateTarget": "this",
                   "ods:hasLocation": {
                     "dwc:country": \"""" + country + """
              ",
                  "dwc:continent": "Europe",
                  "ods:hasGeoreference": {
                    "dwc:decimalLatitude": "52.123",
                    "dwc:decimalLongitude": 10.1233,
                    "dwc": [
                      "1"
                    ]
                  },
                  "dwc:locality": "known"
                }
              },
              {
                "dwc:eventRemarks": "Incorrect",
                "annotateTarget": "this",
                "ods:hasLocation": {
                  "dwc:country": "Unknown",
                  "dwc:continent": "Error",
                  "ods:hasGeoreference": {
                    "dwc:decimalLatitude": "51.123",
                    "dwc:decimalLongitude": 10.5233
                  },
                  "dwc:locality": "unknown"
                }
              },
              {
                "dwc:eventRemarks": "Half Correct",
                "annotateTarget": "this",
                "ods:hasLocation": {
                  "dwc:country": \"""" + country + """
              ",
                            "dwc:continent": "Unknown",
                            "ods:hasGeoreference": {
                              "dwc:decimalLatitude": "52.123",
                              "dwc:decimalLongitude": 10.1233,
                              "test": "hello"
                            },
                            "dwc:locality": "unknown"
                          }
                        }
                      ]
                    }
              """
      );
    } catch (JsonProcessingException e) {
      throw new RuntimeException();
    }
  }

  public static Map<String, UUID> givenBatchIdMap() {
    return Map.of(ID, BATCH_ID);
  }

  public static Annotation givenTombstoneAnnotation() {
    var original = givenAnnotationProcessed();
    return givenAnnotationProcessed()
        .withOdsVersion(original.getOdsVersion() + 1)
        .withOdsStatus(OdsStatus.TOMBSTONE)
        .withOdsHasTombstoneMetadata(givenTombstoneMetadata())
        .withDctermsModified(Date.from(UPDATED))
        .withOdsMergingDecisionStatus(null);
  }

  public static TombstoneMetadata givenTombstoneMetadata() {
    return new TombstoneMetadata()
        .withType("ods:Tombstone")
        .withOdsTombstoneDate(Date.from(UPDATED))
        .withOdsTombstoneText("This annotation was archived")
        .withOdsHasAgents(List.of(givenProcessingAgent()));
  }

  public static FailedMasEvent givenFailedMasEvent() {
    return new FailedMasEvent(
        JOB_ID,
        "MAS Failed"
    );
  }

  public static DigitalSpecimen givenDigitalSpecimen() {
    return new DigitalSpecimen()
        .withDctermsIdentifier(DOI_PROXY + TARGET_ID)
        .withId(DOI_PROXY + TARGET_ID)
        .withType("ods:DigitalSpecimen")
        .withOdsFdoType("https://doi.org/21.T11148/894b1e6cad57e921764e")
        .withOdsOrganisationID("https://ror.org/0443cwa12")
        .withOdsOrganisationName("National Museum of Natural History")
        .withOdsPhysicalSpecimenIDType(OdsPhysicalSpecimenIDType.GLOBAL)
        .withOdsPhysicalSpecimenID("https://geocollections.info/specimen/23602")
        .withOdsNormalisedPhysicalSpecimenID("https://geocollections.info/specimen/23602")
        .withOdsSpecimenName("specimen")
        .withOdsTopicDiscipline(OdsTopicDiscipline.ZOOLOGY)
        .withOdsSourceSystemID(ID_ALT)
        .withOdsSourceSystemName("Source System")
        .withOdsLivingOrPreserved(OdsLivingOrPreserved.PRESERVED)
        .withDctermsLicense("http://creativecommons.org/licenses/by-nc/4.0/")
        .withOdsIsMarkedAsType(true)
        .withOdsIsKnownToContainMedia(false)
        .withDctermsModified("2022-11-01T09:59:24.000Z")
        .withOdsHasIdentifications(List.of(
            new Identification()
                .withType("ods:Identification")
                .withOdsIsVerifiedIdentification(true)
                .withOdsHasTaxonIdentifications(List.of(
                    new TaxonIdentification()
                        .withType("ods:TaxonIdentification")
                        .withDwcKingdom("Animalia")
                        .withDwcPhylum("Chordata")
                        .withDwcClass("Actinopterygii")
                        .withDwcOrder("Tetraodontiformes")
                        .withDwcFamily("Molidae")
                        .withDwcGenus("Mola")
                        .withDwcScientificName("Mola mola (Linnaeus, 1758)")
                ))
        ));
  }


}
