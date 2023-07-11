package eu.dissco.annotationprocessingservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.domain.Annotation;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.domain.AnnotationRecord;
import java.time.Instant;
import java.util.List;

public class TestUtils {

  public static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

  public static final String ID = "20.5000.1025/KZL-VC0-ZK2";
  public static final int VERSION = 1;
  public static final Instant CREATED_RECORD = Instant.parse("2023-02-17T09:52:27.391Z");
  public static final String TYPE = "Annotation";
  public static final String MOTIVATION = "20.5000.1025/460-A7R-QMJ";
  public static final int PREFERENCE_SCORE = 100;
  public static final Instant CREATED = Instant.parse("2023-02-17T09:50:27.391Z");
  public static final String CREATOR = "3fafe98f-1bf9-4927-b9c7-4ba070761a72";
  public static final Instant GENERATED = Instant.parse("2023-02-17T09:49:27.391Z");


  public static AnnotationEvent givenAnnotationEvent() throws JsonProcessingException {
    return new AnnotationEvent(
        TYPE,
        MOTIVATION,
        CREATOR,
        CREATED,
        generateTarget(),
        generateBody()
    );
  }
  public static AnnotationRecord givenAnnotationRecord()
      throws JsonProcessingException {
    return givenAnnotationRecord(MOTIVATION, CREATOR);
  }

  public static AnnotationRecord givenAnnotationRecord(String motivation)
      throws JsonProcessingException {
    return givenAnnotationRecord(motivation, CREATOR);
  }

  public static AnnotationRecord givenAnnotationRecord(String motivation, String creator)
      throws JsonProcessingException {
    return new AnnotationRecord(
        ID,
        VERSION,
        CREATED,
        givenAnnotation(motivation, creator));
  }

  public static AnnotationRecord givenAnnotationRecord(AnnotationEvent annotationEvent)
      throws JsonProcessingException {
    return new AnnotationRecord(
        ID, 1, CREATED,
        new Annotation(
            annotationEvent.type(),
            annotationEvent.motivation(),
            annotationEvent.target(),
            annotationEvent.body(),
            100,
            annotationEvent.creator(),
            annotationEvent.created(),
            generateGenerator(),
            CREATED
        ));
  }

  public static Annotation givenAnnotation(String motivation, String creator) throws JsonProcessingException{
    return new Annotation(
        TYPE,
        motivation,
        generateTarget(),
        generateBody(),
        PREFERENCE_SCORE,
        creator,
        CREATED,
        generateGenerator(),
        GENERATED);
  }

  public static Annotation givenAnnotation() throws JsonProcessingException{
    return new Annotation(
        TYPE,
        MOTIVATION,
        generateTarget(),
        generateBody(),
        PREFERENCE_SCORE,
        CREATOR,
        CREATED,
        generateGenerator(),
        GENERATED);
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
              "type": "annotation",
              "attributes": {
               "fdoProfile": "https://hdl.handle.net/21.T11148/64396cf36b976ad08267",
                "issuedForAgent": "https://ror.org/0566bfb96",
                "digitalObjectType": "https://hdl.handle.net/21.T11148/64396cf36b976ad08267",
                "subjectDigitalObjectId": "https://hdl.handle.net/20.5000.1025/DW0-BNT-FM0",
                "annotationTopic":"20.5000.1025/460-A7R-QMJ",
                "replaceOrAppend": "append",
                "accessRestricted":false,
                "linkedObjectUrl":"https://hdl.handle.net/anno-process-service-pid",
                "linkedObjectIsPid":true
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
