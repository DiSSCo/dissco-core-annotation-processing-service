package eu.dissco.annotationprocessingservice.controller;

import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationEvent;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRequest;
import static eu.dissco.annotationprocessingservice.TestUtils.givenProcessingAgent;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;

import eu.dissco.annotationprocessingservice.exception.ConflictException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.service.ProcessingWebService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

@ExtendWith(MockitoExtension.class)
class AnnotationControllerTest {

  @Mock
  private ProcessingWebService service;

  private AnnotationController controller;

  @BeforeEach
  void setup() {
    controller = new AnnotationController(service);
  }

  @Test
  void testCreateAnnotation()
      throws Exception {
    // Given
    given(service.persistNewAnnotation(givenAnnotationRequest(), false)).willReturn(
        givenAnnotationProcessed());

    // When
    var result = controller.createAnnotation(givenAnnotationRequest());

    // Then
    assertThat(result.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(result.getBody()).isEqualTo(givenAnnotationProcessed());
  }

  @Test
  void testCreateAnnotationBatch()
      throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest();
    var event = givenAnnotationEvent(annotationRequest);
    given(service.persistNewAnnotation(annotationRequest, true))
        .willReturn(givenAnnotationProcessed());

    // When
    var result = controller.createAnnotationBatch(event);

    // Then
    assertThat(result.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(result.getBody()).isEqualTo(givenAnnotationProcessed());
    then(service).should().batchWebAnnotations(event, givenAnnotationProcessed());
  }

  @Test
  void testUpdateAnnotation()
      throws Exception {
    // Given
    var request = givenAnnotationRequest().withId(ID);
    given(service.updateAnnotation(request)).willReturn(givenAnnotationProcessed());

    // When
    var result = controller.updateAnnotation("20.5000.1025", "KZL-VC0-ZK2", request);

    // Then
    assertThat(result.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(result.getBody()).isEqualTo(givenAnnotationProcessed());
  }

  @Test
  void testUpdateAnnotationIdMismatch() {
    // Given
    var request = givenAnnotationRequest().withId(ID);
    var prefix = ID.split("/")[0];
    var suffix = "wrong";

    // Then
    assertThrows(ConflictException.class,
        () -> controller.updateAnnotation(prefix, suffix, request));

  }

  @Test
  void testArchiveAnnotation() throws Exception {
    // Given

    // When
    var result = controller.archiveAnnotation("20.5000.1025", "KZL-VC0-ZK2",
        givenAnnotationProcessed(), givenProcessingAgent());

    // Then
    assertThat(result.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
    then(service).should().archiveAnnotation(givenAnnotationProcessed(), givenProcessingAgent());
  }

  @Test
  void testArchiveAnnotationBadID() {
    // Given

    // When / Then
    assertThrowsExactly(FailedProcessingException.class,
        () -> controller.archiveAnnotation("20.5000.1025", "INVALID-SUFFIX",
            givenAnnotationProcessed(), givenProcessingAgent()));

  }

}
