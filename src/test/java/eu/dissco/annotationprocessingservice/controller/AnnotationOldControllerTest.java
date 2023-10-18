package eu.dissco.annotationprocessingservice.controller;

import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationEvent;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;

import eu.dissco.annotationprocessingservice.service.ProcessingService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

@ExtendWith(MockitoExtension.class)
class AnnotationOldControllerTest {

  @Mock
  private ProcessingService service;

  private AnnotationController controller;

  @BeforeEach
  void setup() {
    controller = new AnnotationController(service);
  }

  @Test
  void testCreateAnnotation()
      throws Exception {
    // Given
    given(service.handleMessage(givenAnnotationEvent())).willReturn(givenAnnotationRecord());

    // When
    var result = controller.createAnnotation(givenAnnotationEvent());

    // Then
    assertThat(result.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(result.getBody()).isEqualTo(givenAnnotationRecord());
  }

  @Test
  void testArchiveAnnotation() throws Exception {
    // Given

    // When
    var result = controller.archiveAnnotation("20.5000.1025", "KZL-VC0-ZK2");

    // Then
    assertThat(result.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
    then(service).should().archiveAnnotation(ID);
  }

}
