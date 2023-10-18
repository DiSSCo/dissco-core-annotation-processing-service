package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationEvent;
import static org.mockito.BDDMockito.then;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import javax.xml.transform.TransformerException;
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
  void testGetMessages()
      throws DataBaseException, FailedProcessingException, JsonProcessingException, TransformerException {
    // Given
    var message = givenMessage();

    // When
    service.getMessages(message);

    // Then
    then(processingService).should().handleMessage(givenAnnotationEvent());
  }

  private String givenMessage() {
    return
        """
            {
              "type": "Annotation",
              "motivation": "20.5000.1025/460-A7R-QMJ",
              "creator": "3fafe98f-1bf9-4927-b9c7-4ba070761a72",
              "created": "2023-02-17T09:50:27.391Z",
              "target": {
                "id": "https://hdl.handle.net/20.5000.1025/DW0-BNT-FM0",
                "type": "digital_specimen",
                "indvProp": "modified"
              },
              "body": {
                "type": "modified",
                "value": [
                  "Error correction"
                ],
                "description": "Test"
              }
            }
            """;
  }

}
