package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.JOB_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationEvent;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRequest;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAutoAcceptedRequest;
import static org.mockito.BDDMockito.then;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaConsumerServiceTest {

  @Mock
  private ProcessingKafkaService processingKafkaService;
  @Mock
  private ProcessingAutoAcceptedService autoAcceptedService;

  private KafkaConsumerService service;

  @BeforeEach
  void setup() {
    service = new KafkaConsumerService(MAPPER, processingKafkaService, autoAcceptedService);
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
  void testGetAutoAcceptedMessages() throws Exception {
    // Given
    var message = givenAutoAcceptedMessage();

    // When
    service.getAutoAcceptedMessages(message);

    // Then
    then(autoAcceptedService).should().handleMessage(givenAutoAcceptedRequest());
  }

  private String givenAutoAcceptedMessage() throws JsonProcessingException {
    return MAPPER.writeValueAsString(givenAutoAcceptedRequest());
  }

  private String givenMessage() throws Exception {
    var annotationNode = MAPPER.valueToTree(List.of(givenAnnotationRequest()));
    var messageNode = MAPPER.createObjectNode();
    messageNode.set("annotations", annotationNode);
    messageNode.put("jobId", JOB_ID);
    return MAPPER.writeValueAsString(messageNode);
  }
}
