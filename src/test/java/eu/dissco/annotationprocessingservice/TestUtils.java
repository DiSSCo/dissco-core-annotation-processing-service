package eu.dissco.annotationprocessingservice;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import eu.dissco.annotationprocessingservice.configuration.DateDeserializer;
import eu.dissco.annotationprocessingservice.configuration.DateSerializer;
import eu.dissco.annotationprocessingservice.configuration.InstantDeserializer;
import eu.dissco.annotationprocessingservice.configuration.InstantSerializer;
import eu.dissco.annotationprocessingservice.domain.AnnotationProcessingEvent;
import eu.dissco.annotationprocessingservice.domain.AnnotationTargetType;
import eu.dissco.annotationprocessingservice.domain.BatchMetadata;
import eu.dissco.annotationprocessingservice.domain.BatchMetadataExtended;
import eu.dissco.annotationprocessingservice.domain.BatchMetadataSearchParam;
import eu.dissco.annotationprocessingservice.domain.HashedAnnotation;
import eu.dissco.annotationprocessingservice.domain.HashedAnnotationRequest;
import eu.dissco.annotationprocessingservice.schema.Agent;
import eu.dissco.annotationprocessingservice.schema.Agent.Type;
import eu.dissco.annotationprocessingservice.schema.Annotation;
import eu.dissco.annotationprocessingservice.schema.Annotation.OaMotivation;
import eu.dissco.annotationprocessingservice.schema.Annotation.OdsStatus;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingRequest;
import eu.dissco.annotationprocessingservice.schema.OaHasBody;
import eu.dissco.annotationprocessingservice.schema.OaHasBody__1;
import eu.dissco.annotationprocessingservice.schema.OaHasSelector;
import eu.dissco.annotationprocessingservice.schema.OaHasSelector__1;
import eu.dissco.annotationprocessingservice.schema.OaHasTarget;
import eu.dissco.annotationprocessingservice.schema.OaHasTarget__1;
import eu.dissco.annotationprocessingservice.schema.SchemaAggregateRating;
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
  public static final String CREATOR = "3fafe98f-1bf9-4927-b9c7-4ba070761a72";
  public static final String JOB_ID = "20.5000.1025/7YC-RGZ-LL1";
  public static final String PROCESSOR_HANDLE = "https://hdl.handle.net/anno-process-service-pid";
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

  public static HashedAnnotationRequest givenHashedAnnotationRequestAlt() {
    return new HashedAnnotationRequest(givenAnnotationRequest().withOaMotivation(
        AnnotationProcessingRequest.OaMotivation.OA_EDITING), ANNOTATION_HASH);
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
        .withOdsID(annotationId)
        .withType("ods:Annotation")
        .withRdfType("ods:Annotation")
        .withOdsVersion(1)
        .withOdsStatus(OdsStatus.ODS_ACTIVE)
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
        .withOaHasBody(givenRequestOaBody())
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

  public static OaHasBody givenRequestOaBody() {
    return new OaHasBody()
        .withType("oa:TextualBody")
        .withOaValue(new ArrayList<>(List.of("a comment")))
        .withDctermsReferences(
            "https://medialib.naturalis.nl/file/id/ZMA.UROCH.P.1555/format/large")
        .withOdsScore(0.99);
  }

  public static OaHasBody__1 givenOaBody() {
    return givenOaBody("a comment");
  }

  public static OaHasBody__1 givenOaBody(String value) {
    return new OaHasBody__1()
        .withType("oa:TextualBody")
        .withOaValue(new ArrayList<>(List.of(value)))
        .withDctermsReferences(
            "https://medialib.naturalis.nl/file/id/ZMA.UROCH.P.1555/format/large")
        .withOdsScore(0.99);
  }

  public static OaHasBody__1 givenOaBodySetType(String type) {
    return new OaHasBody__1()
        .withType(type)
        .withOaValue(new ArrayList<>(List.of("a comment")))
        .withDctermsReferences(
            "https://medialib.naturalis.nl/file/id/ZMA.UROCH.P.1555/format/large")
        .withOdsScore(0.99);
  }

  public static OaHasTarget__1 givenOaTarget(String targetId) {
    return givenOaTarget(targetId, AnnotationTargetType.DIGITAL_SPECIMEN);
  }

  public static OaHasTarget givenRequestOaTarget(String targetId) {
    return givenRequestOaTarget(targetId, AnnotationTargetType.DIGITAL_SPECIMEN);
  }

  public static OaHasTarget givenRequestOaTarget(String targetId, AnnotationTargetType targetType) {
    return givenRequestOaTarget(targetId, targetType, givenRequestSelector());
  }

  public static OaHasTarget givenRequestOaTarget(String targetId, AnnotationTargetType targetType,
      OaHasSelector selector) {
    return new OaHasTarget()
        .withId(DOI_PROXY + targetId)
        .withType("ods:DigitalSpecimen")
        .withOdsType(targetType.toString())
        .withOdsID(DOI_PROXY + targetId)
        .withOaHasSelector(selector);
  }

  public static OaHasSelector givenRequestSelector() {
    return new OaHasSelector()
        .withAdditionalProperty("ods:field", "ods:hasEvent[1].ods:Location.dwc:locality")
        .withAdditionalProperty("@type", "ods:FieldSelector");
  }

  public static OaHasTarget__1 givenOaTarget(String targetId, AnnotationTargetType targetType) {
    return new OaHasTarget__1()
        .withId(DOI_PROXY + targetId)
        .withType("ods:DigitalSpecimen")
        .withOdsType(targetType.toString())
        .withOdsID(DOI_PROXY + targetId)
        .withOaHasSelector(givenSelector());
  }

  public static OaHasTarget__1 givenOaTarget(String targetId, AnnotationTargetType targetType,
      OaHasSelector__1 selector) {
    return new OaHasTarget__1()
        .withId(DOI_PROXY + targetId)
        .withOdsID(DOI_PROXY + targetId)
        .withOdsType(targetType.toString())
        .withOaHasSelector(selector)
        .withType("ods:DigitalSpecimen");
  }

  public static OaHasSelector__1 givenSelector() {
    return new OaHasSelector__1()
        .withAdditionalProperty("ods:field", "ods:hasEvent[1].ods:Location.dwc:locality")
        .withAdditionalProperty("@type", "ods:FieldSelector");
  }

  public static OaHasTarget__1 givenOaTarget(OaHasSelector__1 selector) {
    return givenOaTarget(BARE_ID, AnnotationTargetType.DIGITAL_SPECIMEN, selector);
  }

  public static OaHasSelector__1 givenSelector(String field) {
    return new OaHasSelector__1()
        .withAdditionalProperty("ods:field", field)
        .withAdditionalProperty("@type", "ods:FieldSelector");
  }


  public static Agent givenCreator(String userId) {
    return new Agent()
        .withSchemaName("Test User")
        .withId(userId)
        .withType(Type.SCHEMA_PERSON);
  }

  public static Agent givenGenerator() {
    return new Agent()
        .withSchemaName("Annotation Processing Service")
        .withId(PROCESSOR_HANDLE)
        .withType(Type.AS_APPLICATION);
  }

  public static SchemaAggregateRating givenAggregationRating() {
    return givenAggregationRating(0.1);
  }

  public static SchemaAggregateRating givenAggregationRating(double ratingValue) {
    return new SchemaAggregateRating()
        .withSchemaRatingValue(ratingValue)
        .withType("schema:AggregateRating")
        .withSchemaRatingCount(2);
  }


  public static AnnotationProcessingEvent givenAnnotationEvent() {
    return givenAnnotationEvent(givenAnnotationRequest());
  }

  public static AnnotationProcessingEvent givenAnnotationEvent(
      AnnotationProcessingRequest annotation) {
    return new AnnotationProcessingEvent(List.of(annotation), JOB_ID, null, null);
  }

  public static List<JsonNode> givenPostRequest() throws Exception {
    return List.of(MAPPER.readTree("""
        {
            "data": {
              "type": "https://hdl.handle.net/21.T11148/cf458ca9ee1d44a5608f",
              "attributes": {
               "issuedForAgent": "https://ror.org/0566bfb96",
               "targetPid":"https://doi.org/20.5000.1025/QRS-123-ABC",
               "targetType":"https://doi.org/21.T11148/894b1e6cad57e921764e",
               "motivation":"oa:commenting"
              }
            }
          }
        """));
  }

  public static List<JsonNode> givenPostRequestBatch() throws Exception {
    var jsonNode = MAPPER.readTree("""
        {
            "data": {
              "type": "https://hdl.handle.net/21.T11148/cf458ca9ee1d44a5608f",
              "attributes": {
               "issuedForAgent": "https://ror.org/0566bfb96",
               "targetPid":"https://doi.org/20.5000.1025/QRS-123-ABC",
               "targetType":"https://doi.org/21.T11148/894b1e6cad57e921764e",
               "motivation":"oa:commenting",
               "annotationHash":"3a36d684-deb8-8779-2753-caef497e9ed8"
              }
            }
          }
        """);
    var jsonNode2 = MAPPER.readTree("""
        {
            "data": {
              "type": "https://hdl.handle.net/21.T11148/cf458ca9ee1d44a5608f",
              "attributes": {
               "issuedForAgent": "https://ror.org/0566bfb96",
               "targetPid":"https://doi.org/20.5000.1025/QRS-123-ABC",
               "targetType":"https://doi.org/21.T11148/894b1e6cad57e921764e",
               "motivation":"oa:editing",
               "annotationHash":"3a36d684-deb8-8779-2753-caef497e9ed8"
              }
            }
          }
        """);
    return List.of(jsonNode, jsonNode2);
  }

  public static List<JsonNode> givenPatchRequest() throws Exception {
    return List.of(MAPPER.readTree("""
        {
            "data": {
              "type": "https://hdl.handle.net/21.T11148/cf458ca9ee1d44a5608f",
              "attributes": {
                "issuedForAgent": "https://ror.org/0566bfb96",
                "targetPid":"https://doi.org/20.5000.1025/QRS-123-ABC",
                "targetType":"https://doi.org/21.T11148/894b1e6cad57e921764e",
                "motivation":"oa:commenting"
            },
            "id":"https://hdl.handle.net/20.5000.1025/KZL-VC0-ZK2"
          }
        }
        """));
  }

  public static List<JsonNode> givenPatchRequestBatch() throws Exception {
    var node1 = MAPPER.readTree("""
        {
            "data": {
              "type": "https://hdl.handle.net/21.T11148/cf458ca9ee1d44a5608f",
              "attributes": {
                "issuedForAgent": "https://ror.org/0566bfb96",
                "targetPid":"https://doi.org/20.5000.1025/QRS-123-ABC",
                "targetType":"https://doi.org/21.T11148/894b1e6cad57e921764e",
                "motivation":"oa:commenting",
                "annotationHash":"3a36d684-deb8-8779-2753-caef497e9ed8"
              },
            "id":"https://hdl.handle.net/20.5000.1025/KZL-VC0-ZK2"
          }
          }
        """);
    var node2 = MAPPER.readTree("""
          {
            "data": {
              "type": "https://hdl.handle.net/21.T11148/cf458ca9ee1d44a5608f",
              "attributes": {
               "issuedForAgent": "https://ror.org/0566bfb96",
               "targetPid":"https://doi.org/20.5000.1025/QRS-123-ABC",
               "targetType":"https://doi.org/21.T11148/894b1e6cad57e921764e",
               "motivation":"oa:editing",
                "annotationHash":"3a36d684-deb8-8779-2753-caef497e9ed8"
              },
              "id":"https://hdl.handle.net/20.5000.1025/KZL-VC0-ZK2"
          }
          }
        """);
    return List.of(node1, node2);
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

  public static BatchMetadataSearchParam givenBatchMetadataSearchParamCountry() {
    return new BatchMetadataSearchParam(
        "ods:hasEvent[*].ods:Location.dwc:country",
        "Netherlands");
  }

  public static BatchMetadataExtended givenBatchMetadataExtendedLatitudeSearch() {
    return new BatchMetadataExtended(1,
        List.of(new BatchMetadataSearchParam(
            "ods:hasEvent[*].ods:Location.ods:GeoReference.dwc:decimalLatitude",
            "52.123")));
  }

  public static BatchMetadataExtended givenBatchMetadataExtendedTwoParam() {
    return new BatchMetadataExtended(1, List.of(
        givenBatchMetadataSearchParamCountry(),
        new BatchMetadataSearchParam(
            "ods:hasEvent[*].dwc:occurrenceRemarks",
            "Correct"
        )));
  }

  public static BatchMetadataExtended givenBatchMetadataExtendedOneParam() {
    return new BatchMetadataExtended(1, List.of(
        givenBatchMetadataSearchParamCountry()));
  }

  public static BatchMetadata givenAnnotationEventBatchEnabled() {
    return new BatchMetadata(List.of(givenBaseAnnotationForBatch(1, ID, BATCH_ID)
        .withOdsPlaceInBatch(1)), JOB_ID,
        List.of(givenBatchMetadataExtendedLatitudeSearch()), null);
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
               "ods:ID": "https://doi.org/20.5000.1025/KZL-VC0-ZK2",
               "ods:type": "https://doi.org/21.T11148/894b1e6cad57e921764e",
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
               "ods:hasEvent": [
                 {
                   "dwc:occurrenceRemarks": "Correct",
                   "annotateTarget": "this",
                   "ods:Location": {
                     "dwc:country": \"""" + country + """
              ",
                  "dwc:continent": "Europe",
                  "ods:GeoReference": {
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
                "dwc:occurrenceRemarks": "Incorrect",
                "annotateTarget": "this",
                "ods:Location": {
                  "dwc:country": "Unknown",
                  "dwc:continent": "Error",
                  "ods:GeoReference": {
                    "dwc:decimalLatitude": "51.123",
                    "dwc:decimalLongitude": 10.5233
                  },
                  "dwc:locality": "unknown"
                }
              },
              {
                "dwc:occurrenceRemarks": "Half Correct",
                "annotateTarget": "this",
                "ods:Location": {
                  "dwc:country": \"""" + country + """
              ",
                            "dwc:continent": "Unknown",
                            "ods:GeoReference": {
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

}
