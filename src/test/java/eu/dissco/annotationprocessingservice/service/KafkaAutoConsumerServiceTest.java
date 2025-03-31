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
class KafkaAutoConsumerServiceTest {

  @Mock
  private ProcessingAutoAcceptedService autoAcceptedService;

  private KafkaAutoConsumerService service;

  @BeforeEach
  void setup() {
    service = new KafkaAutoConsumerService(MAPPER, autoAcceptedService);
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

  private String givenAutoAcceptedMessage() throws JsonProcessingException {
    return MAPPER.writeValueAsString(givenAutoAcceptedRequest());
  }

}
