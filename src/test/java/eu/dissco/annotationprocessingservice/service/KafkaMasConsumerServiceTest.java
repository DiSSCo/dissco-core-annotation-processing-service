package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.JOB_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationEvent;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRequest;
import static eu.dissco.annotationprocessingservice.TestUtils.givenFailedMasEvent;
import static org.mockito.BDDMockito.then;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaMasConsumerServiceTest {

  @Mock
  private ProcessingKafkaService processingKafkaService;
  private KafkaMasConsumerService service;

  @BeforeEach
  void setup() {
    service = new KafkaMasConsumerService(MAPPER, processingKafkaService);
  }

  @Test
  void testGetMessages() throws Exception {
    // Given
    var message = givenMessage();

    // When
    service.getMessages(message);

    // Then
    then(processingKafkaService).should().handleMessage(givenAnnotationEvent());
  }

  @Test
  void testMasFailed() throws Exception {
    // Given
    var message = """
        {
          "jobId": "20.5000.1025/7YC-RGZ-LL1",
          "errorMessage":"MAS Failed"
        }
        """;

    // When
    service.masFailed(message);

    // Then
    then(processingKafkaService).should().masJobFailed(givenFailedMasEvent());
  }

  private String givenMessage() throws Exception {
    var annotationNode = MAPPER.valueToTree(List.of(givenAnnotationRequest()));
    var messageNode = MAPPER.createObjectNode();
    messageNode.set("annotations", annotationNode);
    messageNode.put("jobId", JOB_ID);
    return MAPPER.writeValueAsString(messageNode);
  }
}
