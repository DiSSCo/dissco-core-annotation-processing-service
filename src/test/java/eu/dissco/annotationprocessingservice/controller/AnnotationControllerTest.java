package eu.dissco.annotationprocessingservice.controller;

import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationEvent;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRequest;
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
class AnnotationControllerTest {

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
    given(service.createNewAnnotation(givenAnnotationRequest())).willReturn(givenAnnotationProcessed());

    // When
    var result = controller.createAnnotation(givenAnnotationRequest());

    // Then
    assertThat(result.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(result.getBody()).isEqualTo(givenAnnotationProcessed());
  }

  @Test
  void testUpdateAnnotation()
      throws Exception {
    // Given
    given(service.updateAnnotation(givenAnnotationRequest())).willReturn(givenAnnotationProcessed());

    // When
    var result = controller.updateAnnotation(givenAnnotationRequest());

    // Then
    assertThat(result.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(result.getBody()).isEqualTo(givenAnnotationProcessed());
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
