package eu.dissco.annotationprocessingservice.service;


import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationEventBatchEnabled;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.times;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.annotationprocessingservice.properties.KafkaConsumerProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

@ExtendWith(MockitoExtension.class)
class KafkaPublisherServiceTest {

  @Mock
  private KafkaTemplate<String, String> kafkaTemplate;
  @Mock
  private KafkaConsumerProperties consumerProperties;
  private KafkaPublisherService service;

  @BeforeEach
  void setup() {
    service = new KafkaPublisherService(MAPPER, kafkaTemplate, consumerProperties);
  }

  @Test
  void testPublishBatchAnnotation() throws JsonProcessingException {
    // Given
    given(consumerProperties.getTopic()).willReturn("topic");
    var eventMessage = MAPPER.writeValueAsString(givenAnnotationEventBatchEnabled());

    // When
    service.publishBatchAnnotation(givenAnnotationEventBatchEnabled());

    // Then
    then(kafkaTemplate).should(times(1)).send("topic", eventMessage);
  }

}
