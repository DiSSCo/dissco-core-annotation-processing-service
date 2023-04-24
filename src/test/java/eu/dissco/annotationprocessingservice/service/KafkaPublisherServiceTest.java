package eu.dissco.annotationprocessingservice.service;


import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRecord;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.then;

import com.fasterxml.jackson.core.JsonProcessingException;
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

  private KafkaPublisherService service;


  @BeforeEach
  void setup() {
    service = new KafkaPublisherService(MAPPER, kafkaTemplate);
  }

  @Test
  void testPublishCreateEvent() throws JsonProcessingException {
    // Given

    // When
    service.publishCreateEvent(givenAnnotationRecord());

    // Then
    then(kafkaTemplate).should().send(eq("createUpdateDeleteTopic"), anyString());
  }

  @Test
  void testPublishUpdateEvent() throws JsonProcessingException {
    // Given

    // When
    service.publishUpdateEvent(givenAnnotationRecord("Another Motivation"),
        givenAnnotationRecord());

    // Then
    then(kafkaTemplate).should().send(eq("createUpdateDeleteTopic"), anyString());
  }
}
