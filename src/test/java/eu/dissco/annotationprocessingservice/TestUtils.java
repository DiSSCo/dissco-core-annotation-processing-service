package eu.dissco.annotationprocessingservice;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import eu.dissco.annotationprocessingservice.configuration.InstantDeserializer;
import eu.dissco.annotationprocessingservice.configuration.InstantSerializer;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.domain.BatchMetadata;
import eu.dissco.annotationprocessingservice.domain.HashedAnnotation;
import eu.dissco.annotationprocessingservice.domain.annotation.AggregateRating;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.domain.annotation.AnnotationTargetType;
import eu.dissco.annotationprocessingservice.domain.annotation.Body;
import eu.dissco.annotationprocessingservice.domain.annotation.Creator;
import eu.dissco.annotationprocessingservice.domain.annotation.FieldSelector;
import eu.dissco.annotationprocessingservice.domain.annotation.Generator;
import eu.dissco.annotationprocessingservice.domain.annotation.Motivation;
import eu.dissco.annotationprocessingservice.domain.annotation.Target;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TestUtils {

  public static final ObjectMapper MAPPER;

  public static final String ID = "20.5000.1025/KZL-VC0-ZK2";
  public static final String ID_ALT = "20.5000.1025/ZZZ-YYY-XXX";
  public static final String TARGET_ID = "20.5000.1025/QRS-123-ABC";
  public static final Instant CREATED = Instant.parse("2023-02-17T09:50:27.391Z");
  public static final String CREATOR = "3fafe98f-1bf9-4927-b9c7-4ba070761a72";
  public static final String JOB_ID = "20.5000.1025/7YC-RGZ-LL1";
  public static final UUID ANNOTATION_HASH = UUID.fromString(
      "b3dada2d-396b-cab2-665e-125659fc7ae6");

  public static final UUID ANNOTATION_HASH_2 = UUID.fromString(
      "f43e4ec6-ca1c-4a88-9aac-08f6da4b0b1c");
  public static final UUID ANNOTATION_HASH_3 = UUID.fromString(
      "53502490-24cc-4a93-a1ce-e80f5e77f506");
  public static final String ANNOTATION_JSONB = """
      [
        "20.5000.1025/KZL-VC0-ZK2"
       ]
      """;
  public static final String HANDLE_PROXY = "https://hdl.handle.net/";
  public static final String DOI_PROXY = "https://doi.org/";

  static {
    var mapper = new ObjectMapper().findAndRegisterModules();
    SimpleModule dateModule = new SimpleModule();
    dateModule.addSerializer(Instant.class, new InstantSerializer());
    dateModule.addDeserializer(Instant.class, new InstantDeserializer());
    mapper.registerModule(dateModule);
    mapper.setSerializationInclusion(Include.NON_NULL);
    MAPPER = mapper.copy();
  }

  public static HashedAnnotation givenHashedAnnotation() {
    return new HashedAnnotation(givenAnnotationProcessed(), ANNOTATION_HASH);
  }

  public static HashedAnnotation givenHashedAnnotationAlt() {
    return new HashedAnnotation(givenAnnotationProcessedAlt(), ANNOTATION_HASH);
  }

  public static Annotation givenAnnotationProcessed() {
    return givenAnnotationProcessed(ID, CREATOR, TARGET_ID);
  }

  public static Annotation givenAnnotationProcessedWeb(){
    return givenAnnotationProcessedWeb(ID, CREATOR, TARGET_ID);
  }

  public static Annotation givenAnnotationProcessedWeb(String annotationId, String userId, String targetId){
    return new Annotation()
        .withOdsId(annotationId)
        .withOdsVersion(1)
        .withOaBody(givenOaBody())
        .withOaMotivation(Motivation.COMMENTING)
        .withOaTarget(givenOaTarget(targetId))
        .withOaCreator(givenCreator(userId))
        .withDcTermsCreated(CREATED)
        .withOaGenerated(CREATED)
        .withAsGenerator(givenGenerator())
        .withOdsAggregateRating(givenAggregationRating());
  }

  public static Annotation givenAnnotationProcessedAlt() {
    return givenAnnotationProcessed(ID, CREATOR, TARGET_ID).withOaMotivation(Motivation.EDITING);
  }

  public static Annotation givenAnnotationProcessed(String annotationId, String userId,
      String targetId) {
    var annotation = givenAnnotationProcessedWeb(annotationId, userId, targetId);
    return annotation.withOdsJobId(HANDLE_PROXY + JOB_ID);
  }

  public static Annotation givenAnnotationRequest(String targetId) {
    return new Annotation()
        .withOaBody(givenOaBody())
        .withOaMotivation(Motivation.COMMENTING).withOaTarget(givenOaTarget(targetId))
        .withDcTermsCreated(CREATED).withOaCreator(givenCreator(CREATOR))
        .withOdsAggregateRating(givenAggregationRating());
  }

  public static Annotation givenAnnotationRequest() {
    return givenAnnotationRequest(TARGET_ID);
  }

  public static Body givenOaBody() {
    return new Body().withOdsType("ods:specimenName")
        .withOaValue(new ArrayList<>(List.of("a comment")))
        .withDcTermsReference("https://medialib.naturalis.nl/file/id/ZMA.UROCH.P.1555/format/large")
        .withOdsScore(0.99);
  }

  public static Target givenOaTarget(String targetId) {
    return new Target()
        .withOdsId(HANDLE_PROXY + targetId)
        .withSelector(givenSelector())
        .withOdsType(AnnotationTargetType.DIGITAL_SPECIMEN);
  }

  public static FieldSelector givenSelector() {
    return new FieldSelector()
        .withOdsField("digitalSpecimenWrapper.occurrences[1].locality");
  }

  public static Creator givenCreator(String userId) {
    return new Creator().withFoafName("Test User").withOdsId(userId).withOdsType("ORCID");
  }

  public static Generator givenGenerator() {
    return new Generator().withFoafName("Annotation Processing Service")
        .withOdsId("https://hdl.handle.net/anno-process-service-pid")
        .withOdsType("oa:SoftwareAgent");
  }

  public static AggregateRating givenAggregationRating() {
    return new AggregateRating().withRatingValue(0.1).withOdsType("Score").withRatingCount(0.2);
  }


  public static AnnotationEvent givenAnnotationEvent() {
    return givenAnnotationEvent(givenAnnotationProcessed());
  }

  public static AnnotationEvent givenAnnotationEvent(Annotation annotation) {
    return new AnnotationEvent(List.of(annotation), JOB_ID, null, null);
  }

  public static List<JsonNode> givenPostRequest() throws Exception {
    return List.of(MAPPER.readTree("""
        {
            "data": {
              "type": "annotation",
              "attributes": {
               "fdoProfile": "https://hdl.handle.net/21.T11148/64396cf36b976ad08267",
               "digitalObjectType": "https://hdl.handle.net/21.T11148/64396cf36b976ad08267",
               "issuedForAgent": "https://ror.org/0566bfb96",
               "targetPid":"https://hdl.handle.net/20.5000.1025/QRS-123-ABC",
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
              "type": "annotation",
              "attributes": {
               "fdoProfile": "https://hdl.handle.net/21.T11148/64396cf36b976ad08267",
               "digitalObjectType": "https://hdl.handle.net/21.T11148/64396cf36b976ad08267",
               "issuedForAgent": "https://ror.org/0566bfb96",
               "targetPid":"https://hdl.handle.net/20.5000.1025/QRS-123-ABC",
               "targetType":"https://doi.org/21.T11148/894b1e6cad57e921764e",
               "motivation":"oa:commenting",
               "annotationHash":"b3dada2d-396b-cab2-665e-125659fc7ae6"
              }
            }
          }
        """);
    var jsonNode2 = MAPPER.readTree("""
        {
            "data": {
              "type": "annotation",
              "attributes": {
               "fdoProfile": "https://hdl.handle.net/21.T11148/64396cf36b976ad08267",
               "digitalObjectType": "https://hdl.handle.net/21.T11148/64396cf36b976ad08267",
               "issuedForAgent": "https://ror.org/0566bfb96",
               "targetPid":"https://hdl.handle.net/20.5000.1025/QRS-123-ABC",
               "targetType":"https://doi.org/21.T11148/894b1e6cad57e921764e",
               "motivation":"oa:editing",
               "annotationHash":"b3dada2d-396b-cab2-665e-125659fc7ae6"
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
              "type": "annotation",
              "attributes": {
               "fdoProfile": "https://hdl.handle.net/21.T11148/64396cf36b976ad08267",
                "digitalObjectType": "https://hdl.handle.net/21.T11148/64396cf36b976ad08267",
                "issuedForAgent": "https://ror.org/0566bfb96",
                "targetPid":"https://hdl.handle.net/20.5000.1025/QRS-123-ABC",
                "targetType":"https://doi.org/21.T11148/894b1e6cad57e921764e",
                "motivation":"oa:commenting"
            },
            "id":"20.5000.1025/KZL-VC0-ZK2"
          }
        }
        """));
  }

  public static List<JsonNode> givenPatchRequestBatch() throws Exception {
    var node1 = MAPPER.readTree("""
        {
            "data": {
              "type": "annotation",
              "attributes": {
               "fdoProfile": "https://hdl.handle.net/21.T11148/64396cf36b976ad08267",
                "digitalObjectType": "https://hdl.handle.net/21.T11148/64396cf36b976ad08267",
                "issuedForAgent": "https://ror.org/0566bfb96",
                "targetPid":"https://hdl.handle.net/20.5000.1025/QRS-123-ABC",
                "targetType":"https://doi.org/21.T11148/894b1e6cad57e921764e",
                "motivation":"oa:commenting",
                "annotationHash":"b3dada2d-396b-cab2-665e-125659fc7ae6"
              },
            "id":"20.5000.1025/KZL-VC0-ZK2"
          }
          }
        """);
    var node2 = MAPPER.readTree("""
          {
            "data": {
              "type": "annotation",
              "attributes": {
               "fdoProfile": "https://hdl.handle.net/21.T11148/64396cf36b976ad08267",
               "digitalObjectType": "https://hdl.handle.net/21.T11148/64396cf36b976ad08267",
               "issuedForAgent": "https://ror.org/0566bfb96",
               "targetPid":"https://hdl.handle.net/20.5000.1025/QRS-123-ABC",
               "targetType":"https://doi.org/21.T11148/894b1e6cad57e921764e",
               "motivation":"oa:editing",
                "annotationHash":"b3dada2d-396b-cab2-665e-125659fc7ae6"
              },
              "id":"20.5000.1025/KZL-VC0-ZK2"
          }
          }
        """);
    return List.of(node1, node2);
  }

  public static JsonNode givenRollbackCreationRequest() throws Exception {
    return MAPPER.readTree("""
        {
          "data": [
            {"id":"20.5000.1025/KZL-VC0-ZK2"}
          ]
        }
        """);
  }

  public static BatchMetadata givenBatchMetadataLatitudeSearch() {
    return new BatchMetadata(1,
        "digitalSpecimenWrapper.occurrences[*].location.georeference.dwc:decimalLatitude.dwc:value",
        "11");
  }

  public static BatchMetadata givenBatchMetadataCountrySearch() {
    return new BatchMetadata(1, "digitalSpecimenWrapper.occurrences[*].location.dwc:country",
        "Netherlands");
  }

  public static AnnotationEvent givenAnnotationEventBatchEnabled(){
    return new AnnotationEvent(List.of(givenAnnotationRequest().withPlaceInBatch(1)), JOB_ID,
        List.of(givenBatchMetadataLatitudeSearch()), null);
  }

  public static JsonNode givenElasticDocument() {
    return givenElasticDocument("Netherlands", ID);
  }

  public static JsonNode givenElasticDocument(String id) {
    return givenElasticDocument("Netherlands", id);
  }

  public static JsonNode givenElasticDocument(String country, String id) {
    try {

      return MAPPER.readTree("""
          {
            "id": \"""" + id +
          """
              ",
                  "digitalSpecimenWrapper": {
                    "other": ["a", "10"],
                    "fieldNum":1,
                    "occurrences": [
                      {
                        "dwc:occurrenceRemarks": "Correct",
                        "annotateTarget":"this",
                        "location": {
                          "dwc:country": \"""" + country + """
",
              "georeference": {
                "dwc:decimalLatitude": {
                  "dwc:value":11
                },
                "dwc:decimalLongitude": "10",
                "dwc":["1"]
              },
              "locality":"known"
            }
          },
          {
            "dwc:occurrenceRemarks": "Incorrect",
            "annotateTarget":"this",
            "location": {
              "dwc:country": "Unknown",
              "georeference": {
                "dwc:decimalLatitude": {
                  "dwc:value":10
                },
                "dwc:decimalLongitude": "10"
              },
              "locality":"unknown"
            }
          },
          {
            "dwc:occurrenceRemarks": "Correct",
            "blah":10,
            "annotateTarget":"this",
            "location": {
              "dwc:country": \"""" + country + """
",
                    "georeference": {
                      "dwc:decimalLatitude": {
                        "dwc:value":11
                      },
                      "dwc:decimalLongitude": "10.1",
                      "test":"hello"
                    },
                    "locality":"unknown"
                  }
                }
              ]
            }
          }""");
    } catch (JsonProcessingException e) {
      throw new RuntimeException();
    }
  }
}
