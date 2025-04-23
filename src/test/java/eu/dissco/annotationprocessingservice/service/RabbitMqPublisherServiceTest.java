package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationEventBatchEnabled;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenTombstoneAnnotation;
import static eu.dissco.annotationprocessingservice.schema.Annotation.OaMotivation.OA_EDITING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.annotationprocessingservice.domain.ProcessedAnnotationBatch;
import eu.dissco.annotationprocessingservice.properties.RabbitMqProperties;
import eu.dissco.annotationprocessingservice.schema.CreateUpdateTombstoneEvent;
import java.io.IOException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@ExtendWith(MockitoExtension.class)
class RabbitMqPublisherServiceTest {

  private static final String ID = "123-123-123";

  private static RabbitMQContainer container;
  private static RabbitTemplate rabbitTemplate;
  private RabbitMqPublisherService rabbitMqPublisherService;
  @Mock
  private ProvenanceService provenanceService;

  @BeforeAll
  static void setupContainer() throws IOException, InterruptedException {
    container = new RabbitMQContainer("rabbitmq:4.0.8-management-alpine");
    container.start();
    // Declare auto annotation exchange, queue and binding
    declareRabbitResources("auto-accepted-annotation-exchange-dlq",
        "auto-accepted-annotation-queue-dlq",
        "auto-accepted-annotation-dlq");
    // Declare create update tombstone exchange, queue and binding
    declareRabbitResources("create-update-tombstone-exchange", "create-update-tombstone-queue",
        "create-update-tombstone");
    // Declare create mas annotation exchange, queue and binding
    declareRabbitResources("mas-annotation-exchange", "mas-annotation-queue",
        "mas-annotation");

    CachingConnectionFactory factory = new CachingConnectionFactory(container.getHost());
    factory.setPort(container.getAmqpPort());
    factory.setUsername(container.getAdminUsername());
    factory.setPassword(container.getAdminPassword());
    rabbitTemplate = new RabbitTemplate(factory);
    rabbitTemplate.setReceiveTimeout(100L);
  }


  private static void declareRabbitResources(String exchangeName, String queueName,
      String routingKey)
      throws IOException, InterruptedException {
    container.execInContainer("rabbitmqadmin", "declare", "exchange", "name=" + exchangeName,
        "type=direct", "durable=true");
    container.execInContainer("rabbitmqadmin", "declare", "queue", "name=" + queueName,
        "queue_type=quorum", "durable=true");
    container.execInContainer("rabbitmqadmin", "declare", "binding", "source=" + exchangeName,
        "destination_type=queue", "destination=" + queueName, "routing_key=" + routingKey);
  }

  @AfterAll
  static void shutdownContainer() {
    container.stop();
  }

  @BeforeEach
  void setup() {
    rabbitMqPublisherService = new RabbitMqPublisherService(MAPPER, rabbitTemplate, rabbitTemplate,
        new RabbitMqProperties(), provenanceService);
  }

  @Test
  void testPublishCreateEvent() throws JsonProcessingException {
    // Given
    given(provenanceService.generateCreateEvent(givenAnnotationProcessed())).willReturn(
        new CreateUpdateTombstoneEvent().withId(ID));

    // When
    rabbitMqPublisherService.publishCreateEvent(givenAnnotationProcessed());

    // Then
    var result = rabbitTemplate.receive("create-update-tombstone-queue");
    assertThat(MAPPER.readValue(new String(result.getBody()),
        CreateUpdateTombstoneEvent.class).getId()).isEqualTo(ID);
  }

  @Test
  void testPublishUpdateEvent() throws JsonProcessingException {
    // Given
    var updatedAnnotation = givenAnnotationProcessed().withOaMotivation(OA_EDITING);
    given(provenanceService.generateUpdateEvent(updatedAnnotation,
        givenAnnotationProcessed())).willReturn(new CreateUpdateTombstoneEvent().withId(ID));

    // When
    rabbitMqPublisherService.publishUpdateEvent(givenAnnotationProcessed(), updatedAnnotation);

    // Then
    var result = rabbitTemplate.receive("create-update-tombstone-queue");
    assertThat(MAPPER.readValue(new String(result.getBody()),
        CreateUpdateTombstoneEvent.class).getId()).isEqualTo(ID);
  }

  @Test
  void testPublishTombstoneAnnotation() throws JsonProcessingException {
    // Given
    given(provenanceService.generateTombstoneEvent(givenTombstoneAnnotation(),
        givenAnnotationProcessed())).willReturn(new CreateUpdateTombstoneEvent().withId(ID));

    // When
    rabbitMqPublisherService.publishTombstoneEvent(givenTombstoneAnnotation(),
        givenAnnotationProcessed());

    // Then
    var result = rabbitTemplate.receive("create-update-tombstone-queue");
    assertThat(MAPPER.readValue(new String(result.getBody()), CreateUpdateTombstoneEvent.class)
        .getId()).isEqualTo(ID);
  }

  @Test
  void testPublishRawMessage() {
    // Given
    var message = "DLQ Message";

    // When
    rabbitMqPublisherService.deadLetterRaw(message);

    // Then
    var result = rabbitTemplate.receive("auto-accepted-annotation-queue-dlq");
    assertThat(new String(result.getBody())).isEqualTo(message);
  }

  @Test
  void testPublishBatchAnnotation() throws JsonProcessingException {
    // Given
    var eventMessage = givenAnnotationEventBatchEnabled();

    // When
    rabbitMqPublisherService.publishBatchAnnotation(eventMessage);

    // Then
    var result = rabbitTemplate.receive("mas-annotation-queue");
    assertThat(
        MAPPER.readValue(new String(result.getBody()), ProcessedAnnotationBatch.class)).isEqualTo(
        eventMessage);
  }

}
