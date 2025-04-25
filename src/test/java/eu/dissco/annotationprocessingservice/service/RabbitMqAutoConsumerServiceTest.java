package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
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
class RabbitMqAutoConsumerServiceTest {

  @Mock
  private ProcessingAutoAcceptedService autoAcceptedService;
  @Mock
  private RabbitMqPublisherService rabbitMqPublisherService;

  private RabbitMqAutoConsumerService service;

  @BeforeEach
  void setup() {
    service = new RabbitMqAutoConsumerService(MAPPER, autoAcceptedService, rabbitMqPublisherService);
  }

  @Test
  void testGetAutoAcceptedMessages() throws Exception {
    // Given
    var message = List.of(givenAutoAcceptedMessage());

    // When
    service.getAutoAcceptedMessages(message);

    // Then
    then(autoAcceptedService).should().handleMessage(List.of(givenAutoAcceptedRequest()));
  }

  @Test
  void testInvalidAutoAcceptedMessages() throws Exception {
    // Given
    var invalidMessage = "invalid message";

    // When
    service.getAutoAcceptedMessages(List.of(invalidMessage));

    // Then
    then(rabbitMqPublisherService).should().deadLetterRaw(invalidMessage);
    then(autoAcceptedService).should().handleMessage(List.of());
  }

  private String givenAutoAcceptedMessage() throws JsonProcessingException {
    return MAPPER.writeValueAsString(givenAutoAcceptedRequest());
  }

}
