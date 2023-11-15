package eu.dissco.annotationprocessingservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.domain.annotation.AggregateRating;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
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

  public static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

  public static final String ID = "20.5000.1025/KZL-VC0-ZK2";
  public static final String TARGET_ID = "20.5000.1025/QRS-123-ABC";
  public static final Instant CREATED = Instant.parse("2023-02-17T09:50:27.391Z");
  public static final String CREATOR = "3fafe98f-1bf9-4927-b9c7-4ba070761a72";
  public static final UUID JOB_ID = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
  public static final String ANNOTATION_JSONB =
      """
        [{
          "annotationId":"20.5000.1025/KZL-VC0-ZK2"
         }]
        """;

  public static Annotation givenAnnotationProcessed(){
    return givenAnnotationProcessed(ID, CREATOR, TARGET_ID);
  }
  public static Annotation givenAnnotationProcessedAlt(){
    return givenAnnotationProcessed(ID, CREATOR, TARGET_ID).withOaMotivation(Motivation.EDITING);
  }

  public static Annotation givenAnnotationProcessed(String annotationId, String userId, String targetId) {
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

  public static Annotation givenAnnotationRequest(String targetId) {
    return new Annotation()
        .withOaBody(givenOaBody())
        .withOaMotivation(Motivation.COMMENTING)
        .withOaTarget(givenOaTarget(targetId))
        .withDcTermsCreated(CREATED)
        .withOaCreator(givenCreator(CREATOR))
        .withOdsAggregateRating(givenAggregationRating());
  }

  public static Annotation givenAnnotationUpdateRequest() {
    return new Annotation()
        .withOaBody(givenOaBody())
        .withOaMotivation(Motivation.COMMENTING)
        .withOaTarget(givenOaTarget(TARGET_ID))
        .withDcTermsCreated(CREATED)
        .withOaCreator(givenCreator(CREATOR))
        .withOdsAggregateRating(givenAggregationRating())
        .withOdsId(ID);
  }

  public static Annotation givenAnnotationRequest() {
    return givenAnnotationRequest(TARGET_ID);
  }

  public static Body givenOaBody() {
    return new Body()
        .withOdsType("ods:specimenName")
        .withOaValue(new ArrayList<>(List.of("a comment")))
        .withDcTermsReference("https://medialib.naturalis.nl/file/id/ZMA.UROCH.P.1555/format/large")
        .withOdsScore(0.99);
  }

  public static Target givenOaTarget(String targetId) {
    return new Target()
        .withOdsId(targetId)
        .withSelector(givenSelector())
        .withOdsType("digital_specimen");
  }

  public static FieldSelector givenSelector() {
    return new FieldSelector()
        .withOdsField("ods:specimenName");
  }

  public static Creator givenCreator(String userId) {
    return new Creator()
        .withFoafName("Test User")
        .withOdsId(userId)
        .withOdsType("ORCID");
  }

  public static Generator givenGenerator(){
    return new Generator()
        .withFoafName("Annotation Processing Service")
        .withOdsId("https://hdl.handle.net/anno-process-service-pid")
        .withOdsType("tool/Software");
  }
  public static AggregateRating givenAggregationRating(){
    return new AggregateRating()
        .withRatingValue(0.1)
        .withOdsType("Score")
        .withRatingCount(0.2);
  }


  public static AnnotationEvent givenAnnotationEvent() throws JsonProcessingException {
    return givenAnnotationEvent(givenAnnotationProcessed());
  }

  public static AnnotationEvent givenAnnotationEvent(Annotation annotation){
    return new AnnotationEvent(annotation, JOB_ID);
  }

  public static JsonNode generateTarget() throws JsonProcessingException {
    return MAPPER.readValue(
        """
            {
              "id": "https://hdl.handle.net/20.5000.1025/DW0-BNT-FM0",
              "type": "digital_specimen",
              "indvProp": "modified"
            }
            """, JsonNode.class
    );
  }

  private static JsonNode generateBody() throws JsonProcessingException {
    return MAPPER.readValue(
        """
            {
              "type": "modified",
              "value": [
                "Error correction"
              ],
              "description": "Test"
            }
            """, JsonNode.class
    );
  }

  public static JsonNode generateGenerator() throws JsonProcessingException {
    return MAPPER.readValue(
        """
            {
              "id": "https://hdl.handle.net/anno-process-service-pid",
              "name": "Annotation Procession Service",
              "type": "tool/Software"
            }
            """, JsonNode.class
    );
  }

  public static List<JsonNode> givenPostRequest() throws Exception {
    return List.of(MAPPER.readTree("""
        {
            "data": {
              "type": "handle",
              "attributes": {
               "fdoProfile": "https://hdl.handle.net/21.T11148/64396cf36b976ad08267",
                "issuedForAgent": "https://ror.org/0566bfb96",
                "digitalObjectType": "https://hdl.handle.net/21.T11148/64396cf36b976ad08267"
              }
            }
          }
        """));
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

}
