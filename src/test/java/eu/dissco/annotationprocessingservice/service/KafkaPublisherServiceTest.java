package eu.dissco.annotationprocessingservice.service;


import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationEventBatchEnabled;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.times;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.annotationprocessingservice.properties.KafkaConsumerProperties;
import eu.dissco.annotationprocessingservice.schema.Annotation.OaMotivation;
import java.util.List;
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
  @Mock
  private ProvenanceService provenanceService;

  private KafkaPublisherService service;


  @BeforeEach
  void setup() {
    service = new KafkaPublisherService(MAPPER, kafkaTemplate, consumerProperties,
        provenanceService);
  }

  @Test
  void testPublishCreateEvent() throws JsonProcessingException {
    // Given

    // When
    service.publishCreateEvent(givenAnnotationProcessed());

    // Then
    then(kafkaTemplate).should().send(eq("createUpdateDeleteTopic"), anyString());
  }

  @Test
  void testPublishUpdateEvent() throws JsonProcessingException {
    // Given

    // When
    service.publishUpdateEvent(givenAnnotationProcessed().withOaMotivation(OaMotivation.OA_EDITING),
        givenAnnotationProcessed());

    // Then
    then(kafkaTemplate).should().send(eq("createUpdateDeleteTopic"), anyString());
  }

  @Test
  void testPublishBatchAnnotation() throws JsonProcessingException {
    // Given
    given(consumerProperties.getTopic()).willReturn("topic");
    var eventMessage = MAPPER.writeValueAsString(givenAnnotationEventBatchEnabled());

    // When
    service.publishBatchAnnotation(List.of(givenAnnotationEventBatchEnabled()));

    // Then
    then(kafkaTemplate).should(times(1)).send("topic", eventMessage);
  }

}
