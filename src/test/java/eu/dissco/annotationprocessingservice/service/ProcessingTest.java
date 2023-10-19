package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.CREATED;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationEvent;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessedAlt;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.web.HandleComponent;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.env.Environment;

@ExtendWith(MockitoExtension.class)
class ProcessingTest {

  @Mock
  private AnnotationRepository repository;
  @Mock
  private ElasticSearchRepository elasticRepository;
  @Mock
  private KafkaPublisherService kafkaPublisherService;
  @Mock
  private FdoRecordService fdoRecordService;
  @Mock
  private HandleComponent handleComponent;
  @Mock
  private Environment environment;
  private MockedStatic<Instant> mockedStatic;
  private final Instant instant = Instant.now(Clock.fixed(CREATED, ZoneOffset.UTC));
  private ProcessingService service;
  Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
  MockedStatic<Clock> mockedClock = mockStatic(Clock.class);

  @BeforeEach
  void setup() {
    service = new ProcessingService(MAPPER, repository, elasticRepository,
        kafkaPublisherService, fdoRecordService, handleComponent, environment);
    mockedStatic = mockStatic(Instant.class);
    mockedStatic.when(Instant::now).thenReturn(instant);
    mockedClock.when(Clock::systemUTC).thenReturn(clock);
  }

  @AfterEach
  void destroy() {
    mockedStatic.close();
    mockedClock.close();
  }

  @Test
  void testNewMessage()
      throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest();
    given(repository.getAnnotation(annotationRequest)).willReturn(Collections.emptyList());
    given(handleComponent.postHandle(any())).willReturn(ID);
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.Created);
    given(elasticRepository.indexAnnotation(any(Annotation.class))).willReturn(indexResponse);

    // When
    var result = service.handleMessage(givenAnnotationEvent(annotationRequest));

    // Then
    assertThat(result).isNotNull().isInstanceOf(Annotation.class);
    assertThat(result.getOdsId()).isEqualTo(ID);
    then(kafkaPublisherService).should().publishCreateEvent(any(Annotation.class));
  }

  @Test
  void testNewMessageWebProfile() throws Exception {
    // Given
    given(handleComponent.postHandle(any())).willReturn(ID);
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.Created);
    given(elasticRepository.indexAnnotation(any(Annotation.class))).willReturn(indexResponse);

    // When
    var result = service.handleMessage(givenAnnotationEvent());

    // Then
    assertThat(result).isNotNull().isInstanceOf(Annotation.class);
    assertThat(result.getOdsId()).isEqualTo(ID);
    then(kafkaPublisherService).should().publishCreateEvent(any(Annotation.class));
  }

  @Test
  void testNewMessageHandleFailure()
      throws Exception {
    // Given
    var annotation = givenAnnotationProcessed();
    given(repository.getAnnotation(annotation)).willReturn(Collections.emptyList());
    given(handleComponent.postHandle(any())).willThrow(PidCreationException.class);

    // Then
    assertThrows(FailedProcessingException.class, () -> service.handleMessage(givenAnnotationEvent()));
  }

  @Test
  void testHandleNewMessageKafkaException()
      throws Exception {
    // Given
    var annotation = givenAnnotationProcessed();
    given(repository.getAnnotation(annotation)).willReturn(Collections.emptyList());
    given(handleComponent.postHandle(any())).willReturn(ID);
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.Created);
    given(elasticRepository.indexAnnotation(any(Annotation.class))).willReturn(indexResponse);
    doThrow(JsonProcessingException.class).when(kafkaPublisherService).publishCreateEvent(any(
        Annotation.class));

    // When
    assertThatThrownBy(() -> service.handleMessage(givenAnnotationEvent())).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(elasticRepository).should().archiveAnnotation(ID);
    then(repository).should().rollbackAnnotation(ID);
    then(fdoRecordService).should().buildRollbackCreationRequest(annotation);
    then(handleComponent).should().rollbackHandleCreation(any());
  }

  @Test
  void testHandleNewMessageElasticException()
      throws Exception {
    // Given
    var annotation = givenAnnotationProcessed();
    given(repository.getAnnotation(annotation)).willReturn(Collections.emptyList());
    given(handleComponent.postHandle(any())).willReturn(ID);
    given(elasticRepository.indexAnnotation(any(Annotation.class))).willThrow(
        IOException.class);

    // When
    assertThatThrownBy(() -> service.handleMessage(givenAnnotationEvent())).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(repository).should().rollbackAnnotation(ID);
    then(fdoRecordService).should().buildRollbackCreationRequest(annotation);
    then(handleComponent).should().rollbackHandleCreation(any());
    then(kafkaPublisherService).shouldHaveNoInteractions();
  }

  @Test
  void testHandleEqualMessage()
      throws Exception {
    // Given
    var annotation = givenAnnotationProcessed();
    given(repository.getAnnotation(annotation)).willReturn(List.of(givenAnnotationProcessed()));

    // When
    var result = service.handleMessage(givenAnnotationEvent(annotation));

    // Then
    assertThat(result).isNull();
    then(kafkaPublisherService).shouldHaveNoInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
    then(handleComponent).shouldHaveNoInteractions();
  }

  @Test
  void testUpdateMessage()
      throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest();
    var currentAnnotation = givenAnnotationProcessedAlt();
    given(repository.getAnnotation(annotationRequest)).willReturn(List.of(currentAnnotation));
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.Updated);
    given(elasticRepository.indexAnnotation(any(Annotation.class))).willReturn(indexResponse);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    var result = service.handleMessage(givenAnnotationEvent(annotationRequest));

    // Then
    assertThat(result).isNotNull().isInstanceOf(Annotation.class);
    assertThat(result.getOdsId()).isEqualTo(ID);
    then(fdoRecordService).should()
        .buildPatchRollbackHandleRequest(annotationRequest, ID);
    then(handleComponent).should().updateHandle(any());
    then(kafkaPublisherService).should()
        .publishUpdateEvent(any(Annotation.class), any(Annotation.class));
  }

  @Test
  void testUpdateMessageHandleDoesNotNeedUpdate()
      throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest();
    var annotationProcessed = givenAnnotationProcessed();
    var currentAnnotation = givenAnnotationProcessedAlt();
    given(repository.getAnnotation(annotationRequest)).willReturn(List.of(currentAnnotation));
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.Updated);
    given(elasticRepository.indexAnnotation(any(Annotation.class))).willReturn(indexResponse);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(false);

    // When
    var result = service.handleMessage(givenAnnotationEvent(annotationRequest));

    // Then
    assertThat(result).isNotNull().isInstanceOf(Annotation.class);
    assertThat(result.getOdsId()).isEqualTo(ID);
    then(fdoRecordService).shouldHaveNoMoreInteractions();
    then(handleComponent).shouldHaveNoInteractions();
    then(kafkaPublisherService).should()
        .publishUpdateEvent(any(Annotation.class), any(Annotation.class));
  }

  @Test
  void testUpdateMessageElasticException() throws Exception {
    // Given
    var annotation = givenAnnotationRequest();
    var currentAnnotation = givenAnnotationProcessedAlt();
    given(repository.getAnnotation(annotation)).willReturn(List.of(currentAnnotation));
    given(elasticRepository.indexAnnotation(any(Annotation.class))).willThrow(
        IOException.class);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    assertThatThrownBy(() -> service.handleMessage(givenAnnotationEvent(annotation))).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(fdoRecordService).should((times(2)))
        .buildPatchRollbackHandleRequest(annotation, ID);
    then(handleComponent).should().updateHandle(any());
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(repository).should(times(2)).createAnnotationRecord(any(Annotation.class));
    then(kafkaPublisherService).shouldHaveNoInteractions();
  }

  @Test
  void testHandleUpdateMessageKafkaException() throws Exception {
    // Given
    var annotation = givenAnnotationRequest().withOdsId(ID);
    given(repository.getAnnotation(annotation)).willReturn(List.of(givenAnnotationProcessedAlt()));
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.Updated);
    given(elasticRepository.indexAnnotation(any(Annotation.class))).willReturn(indexResponse);
    doThrow(JsonProcessingException.class).when(kafkaPublisherService).publishUpdateEvent(any(
        Annotation.class), any(Annotation.class));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    assertThatThrownBy(() -> service.handleMessage(givenAnnotationEvent(annotation))).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(fdoRecordService).should((times(2)))
        .buildPatchRollbackHandleRequest(annotation, ID);
    then(handleComponent).should().updateHandle(any());
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(elasticRepository).should(times(2)).indexAnnotation(any(Annotation.class));
    then(repository).should(times(2)).createAnnotationRecord(any(Annotation.class));
  }

  @Test
  void testUpdateMessageKafkaExceptionHandleRollbackFailed() throws Exception {
    // Given
    var annotationRequest = givenAnnotationProcessed();
    given(repository.getAnnotation(annotationRequest)).willReturn(List.of(givenAnnotationProcessedAlt()));
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.Updated);
    given(elasticRepository.indexAnnotation(any(Annotation.class))).willReturn(indexResponse);
    doThrow(JsonProcessingException.class).when(kafkaPublisherService).publishUpdateEvent(any(
        Annotation.class), any(Annotation.class));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    doThrow(PidCreationException.class).when(handleComponent).rollbackHandleUpdate(any());

    // When
    assertThatThrownBy(() -> service.handleMessage(givenAnnotationEvent())).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(fdoRecordService).should(times(2)).buildPatchRollbackHandleRequest(annotationRequest.withOdsVersion(2), ID);
    then(handleComponent).should().updateHandle(any());
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(elasticRepository).should(times(2)).indexAnnotation(any(Annotation.class));
    then(repository).should(times(2)).createAnnotationRecord(any(Annotation.class));
  }


  @Test
  void testArchiveAnnotation() throws Exception {
    // Given
    given(repository.getAnnotationById(ID)).willReturn(Optional.of(ID));
    var deleteResponse = mock(DeleteResponse.class);
    given(deleteResponse.result()).willReturn(Result.Deleted);
    given(elasticRepository.archiveAnnotation(ID)).willReturn(deleteResponse);

    // When
    service.archiveAnnotation(ID);

    // Then
    then(repository).should().archiveAnnotation(ID);
    then(fdoRecordService).should().buildArchiveHandleRequest(ID);
    then(handleComponent).should().archiveHandle(any(), eq(ID));
  }

  @Test
  void testArchiveAnnotationHandleFailed() throws Exception {
    // Given
    given(repository.getAnnotationById(ID)).willReturn(Optional.of(ID));
    doThrow(PidCreationException.class).when(handleComponent).archiveHandle(any(), eq(ID));

    // When
    assertThrows(FailedProcessingException.class, () -> service.archiveAnnotation(ID));

    // Then
    then(elasticRepository).shouldHaveNoInteractions();
    then(repository).shouldHaveNoMoreInteractions();
  }

  @Test
  void testArchiveMissingAnnotation() throws Exception {
    // Given
    given(repository.getAnnotationById(ID)).willReturn(Optional.empty());

    // When
    service.archiveAnnotation(ID);

    // Then
    then(repository).shouldHaveNoMoreInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
    then(handleComponent).shouldHaveNoInteractions();
  }

}
