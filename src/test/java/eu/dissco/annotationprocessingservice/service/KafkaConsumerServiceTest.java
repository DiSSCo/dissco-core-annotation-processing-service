package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.JOB_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationEvent;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static org.mockito.BDDMockito.then;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaConsumerServiceTest {

  @Mock
  private ProcessingService processingService;

  private KafkaConsumerService service;

  @BeforeEach
  void setup() {
    service = new KafkaConsumerService(MAPPER, processingService);
  }

  @Test
  void testGetMessages() throws Exception {
    // Given
    var message = givenMessage();

    // When
    service.getMessages(message);

    // Then
    then(processingService).should().handleMessage(givenAnnotationEvent());
  }

  private String givenMessage() throws Exception {
    var annotationNode = MAPPER.valueToTree(givenAnnotationProcessed());
    var messageNode = MAPPER.createObjectNode();
    messageNode.set("annotation", annotationNode);
    messageNode.put("jobId", JOB_ID.toString());
    return MAPPER.writeValueAsString(messageNode);
  }
}
