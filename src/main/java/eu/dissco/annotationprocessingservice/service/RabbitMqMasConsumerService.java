package eu.dissco.annotationprocessingservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.domain.FailedMasEvent;
import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import eu.dissco.annotationprocessingservice.exception.BatchingException;
import eu.dissco.annotationprocessingservice.exception.ConflictException;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingEvent;
import java.io.IOException;
import lombok.AllArgsConstructor;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Profile(Profiles.RABBIT_MQ_MAS)
@AllArgsConstructor
public class RabbitMqMasConsumerService {

  @Qualifier("objectMapper")
  private final ObjectMapper mapper;
  private final ProcessingMasService service;

  @RabbitListener(queues = "${rabbitmq.mas-annotation.queue-name:mas-annotation-queue}")
  public void getMessages(@Payload String message)
      throws IOException, DataBaseException, FailedProcessingException, AnnotationValidationException, ConflictException, BatchingException {
    var event = mapper.readValue(message, AnnotationProcessingEvent.class);
    service.handleMessage(event);
  }

  @RabbitListener(queues = "${rabbitmq.mas-failed.queue-name:mas-annotation-failed-queue}")
  public void masFailed(@Payload String message)
      throws IOException, DataBaseException {
    var event = mapper.readValue(message, FailedMasEvent.class);
    service.masJobFailed(event);
  }

}
