package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationEvent;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.annotationprocessingservice.domain.Annotation;
import eu.dissco.annotationprocessingservice.domain.AnnotationRecord;
import eu.dissco.annotationprocessingservice.exception.DataBaseException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import java.io.IOException;
import java.util.Optional;
import javax.xml.transform.TransformerException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ProcessingTest {

  @Mock
  private AnnotationRepository repository;
  @Mock
  private HandleService handleService;
  @Mock
  private ElasticSearchRepository elasticRepository;
  @Mock
  private KafkaPublisherService kafkaPublisherService;


  private ProcessingService service;

  @BeforeEach
  void setup() {
    service = new ProcessingService(MAPPER, repository, handleService, elasticRepository,
        kafkaPublisherService);
  }

  @Test
  void testHandleNewMessage()
      throws IOException, DataBaseException, TransformerException, FailedProcessingException {
    // Given
    var annotationRecord = givenAnnotationRecord();
    given(repository.getAnnotation(annotationRecord.annotation().target(),
        annotationRecord.annotation()
            .creator(), annotationRecord.annotation().motivation())).willReturn(Optional.empty());
    given(handleService.createNewHandle(any(Annotation.class))).willReturn(ID);
    given(repository.createAnnotationRecord(any(AnnotationRecord.class))).willReturn(1);
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.Created);
    given(elasticRepository.indexAnnotation(any(AnnotationRecord.class))).willReturn(indexResponse);

    // When
    var result = service.handleMessage(givenAnnotationEvent());

    // Then
    assertThat(result).isNotNull().isInstanceOf(AnnotationRecord.class);
    assertThat(result.id()).isEqualTo(ID);
    then(kafkaPublisherService).should().publishCreateEvent(any(AnnotationRecord.class));
  }

  @Test
  void testHandleNewMessageKafkaException()
      throws IOException, DataBaseException, TransformerException {
    // Given
    var annotationRecord = givenAnnotationRecord();
    given(repository.getAnnotation(annotationRecord.annotation().target(),
        annotationRecord.annotation()
            .creator(), annotationRecord.annotation().motivation())).willReturn(Optional.empty());
    given(handleService.createNewHandle(any(Annotation.class))).willReturn(ID);
    given(repository.createAnnotationRecord(any(AnnotationRecord.class))).willReturn(1);
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.Created);
    given(elasticRepository.indexAnnotation(any(AnnotationRecord.class))).willReturn(indexResponse);
    doThrow(JsonProcessingException.class).when(kafkaPublisherService).publishCreateEvent(any(
        AnnotationRecord.class));

    // When
    assertThatThrownBy(() -> service.handleMessage(givenAnnotationEvent())).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(elasticRepository).should().archiveAnnotation(ID);
    then(repository).should().rollbackAnnotation(ID);
    then(handleService).should().rollbackHandleCreation(any(AnnotationRecord.class));
  }

  @Test
  void testHandleNewMessageElasticException()
      throws IOException, DataBaseException, TransformerException {
    // Given
    var annotationRecord = givenAnnotationRecord();
    given(repository.getAnnotation(annotationRecord.annotation().target(),
        annotationRecord.annotation()
            .creator(), annotationRecord.annotation().motivation())).willReturn(Optional.empty());
    given(handleService.createNewHandle(any(Annotation.class))).willReturn(ID);
    given(repository.createAnnotationRecord(any(AnnotationRecord.class))).willReturn(1);
    given(elasticRepository.indexAnnotation(any(AnnotationRecord.class))).willThrow(
        IOException.class);

    // When
    assertThatThrownBy(() -> service.handleMessage(givenAnnotationEvent())).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(repository).should().rollbackAnnotation(ID);
    then(handleService).should().rollbackHandleCreation(any(AnnotationRecord.class));
    then(kafkaPublisherService).shouldHaveNoInteractions();
  }

  @Test
  void testHandleEqualMessage()
      throws IOException, DataBaseException, TransformerException, FailedProcessingException {
    // Given
    var annotationRecord = givenAnnotationRecord();
    given(repository.getAnnotation(annotationRecord.annotation().target(),
        annotationRecord.annotation()
            .creator(), annotationRecord.annotation().motivation())).willReturn(
        Optional.of(annotationRecord));
    given(repository.updateLastChecked(any(AnnotationRecord.class))).willReturn(1);

    // When
    var result = service.handleMessage(givenAnnotationEvent());

    // Then
    assertThat(result).isNull();
    then(kafkaPublisherService).shouldHaveNoInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
    then(handleService).shouldHaveNoInteractions();
  }

  @Test
  void testHandleUpdateMessage()
      throws IOException, DataBaseException, TransformerException, FailedProcessingException {
    // Given
    var annotationRecord = givenAnnotationRecord();
    given(repository.getAnnotation(annotationRecord.annotation().target(),
        annotationRecord.annotation()
            .creator(), annotationRecord.annotation().motivation())).willReturn(
        Optional.of(givenAnnotationRecord("Another Motivation", "Another Creator")));
    given(repository.createAnnotationRecord(any(AnnotationRecord.class))).willReturn(1);
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.Updated);
    given(elasticRepository.indexAnnotation(any(AnnotationRecord.class))).willReturn(indexResponse);

    // When
    var result = service.handleMessage(givenAnnotationEvent());

    // Then
    assertThat(result).isNotNull().isInstanceOf(AnnotationRecord.class);
    assertThat(result.id()).isEqualTo(ID);
    then(handleService).should().updateHandle(eq(ID), any(Annotation.class));
    then(kafkaPublisherService).should()
        .publishUpdateEvent(any(AnnotationRecord.class), any(AnnotationRecord.class));
  }

  @Test
  void testHandleUpdateMessageElasticException() throws IOException, DataBaseException{
    // Given
    var annotationRecord = givenAnnotationRecord();
    given(repository.getAnnotation(annotationRecord.annotation().target(),
        annotationRecord.annotation()
            .creator(), annotationRecord.annotation().motivation())).willReturn(
        Optional.of(givenAnnotationRecord("Another Motivation", "Another Creator")));
    given(repository.createAnnotationRecord(any(AnnotationRecord.class))).willReturn(1);
    given(elasticRepository.indexAnnotation(any(AnnotationRecord.class))).willThrow(
        IOException.class);

    // When
    assertThatThrownBy(() -> service.handleMessage(givenAnnotationEvent())).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(handleService).should().updateHandle(eq(ID), any(Annotation.class));
    then(handleService).should().deleteVersion(any(AnnotationRecord.class));
    then(repository).should(times(2)).createAnnotationRecord(any(AnnotationRecord.class));
    then(kafkaPublisherService).shouldHaveNoInteractions();
  }

  @Test
  void testHandleUpdateMessageKafkaException() throws IOException, DataBaseException{
    // Given
    var annotationRecord = givenAnnotationRecord();
    given(repository.getAnnotation(annotationRecord.annotation().target(),
        annotationRecord.annotation()
            .creator(), annotationRecord.annotation().motivation())).willReturn(
        Optional.of(givenAnnotationRecord("Another Motivation", "Another Creator")));
    given(repository.createAnnotationRecord(any(AnnotationRecord.class))).willReturn(1);
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.Updated);
    given(elasticRepository.indexAnnotation(any(AnnotationRecord.class))).willReturn(indexResponse);
    doThrow(JsonProcessingException.class).when(kafkaPublisherService).publishUpdateEvent(any(
        AnnotationRecord.class), any(AnnotationRecord.class));

    // When
    assertThatThrownBy(() -> service.handleMessage(givenAnnotationEvent())).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(handleService).should().updateHandle(eq(ID), any(Annotation.class));
    then(handleService).should().deleteVersion(any(AnnotationRecord.class));
    then(elasticRepository).should(times(2)).indexAnnotation(any(AnnotationRecord.class));
    then(repository).should(times(2)).createAnnotationRecord(any(AnnotationRecord.class));
  }


  @Test
  void testArchiveAnnotation() throws IOException {
    // Given
    given(repository.getAnnotationById(ID)).willReturn(Optional.of(ID));
    var deleteResponse = mock(DeleteResponse.class);
    given(deleteResponse.result()).willReturn(Result.Deleted);
    given(elasticRepository.archiveAnnotation(ID)).willReturn(deleteResponse);

    // When
    service.archiveAnnotation(ID);

    // Then
    then(repository).should().archiveAnnotation(ID);
    then(handleService).should().archiveRecord(eq(ID), anyString());
  }

  @Test
  void testArchiveMissingAnnotation() throws IOException {
    // Given
    given(repository.getAnnotationById(ID)).willReturn(Optional.empty());

    // When
    service.archiveAnnotation(ID);

    // Then
    then(repository).shouldHaveNoMoreInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
    then(handleService).shouldHaveNoInteractions();
  }

}
