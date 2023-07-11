package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.CREATED;
import static eu.dissco.annotationprocessingservice.TestUtils.GENERATED;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.generateGenerator;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotation;
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
import static org.mockito.Mockito.mockStatic;
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
import eu.dissco.annotationprocessingservice.web.HandleComponent;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import javax.xml.transform.TransformerException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

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
  private MockedStatic<Instant> mockedStatic;
  private final Instant instant = Instant.now(Clock.fixed(CREATED, ZoneOffset.UTC));
  private ProcessingService service;
  Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
  MockedStatic<Clock> mockedClock = mockStatic(Clock.class);

  @BeforeEach
  void setup() {
    service = new ProcessingService(MAPPER, repository, elasticRepository,
        kafkaPublisherService, fdoRecordService, handleComponent);
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
  void testHandleNewMessage()
      throws Exception {
    // Given
    var annotationRecord = givenAnnotationRecord();
    given(repository.getAnnotation(annotationRecord.annotation().target(),
        annotationRecord.annotation()
            .creator(), annotationRecord.annotation().motivation())).willReturn(Optional.empty());
    given(handleComponent.postHandle(any())).willReturn(ID);
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
      throws Exception {
    // Given
    var annotationRecord = givenAnnotationRecord(givenAnnotationEvent());
    given(repository.getAnnotation(annotationRecord.annotation().target(),
        annotationRecord.annotation()
            .creator(), annotationRecord.annotation().motivation())).willReturn(Optional.empty());
    given(handleComponent.postHandle(any())).willReturn(ID);
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
    then(fdoRecordService).should().buildRollbackCreationRequest(annotationRecord);
    then(handleComponent).should().rollbackHandleCreation(any());
  }

  @Test
  void testHandleNewMessageElasticException()
      throws Exception {
    // Given
    var annotationEvent = givenAnnotationEvent();
    var annotationRecord =givenAnnotationRecord(annotationEvent);
    given(repository.getAnnotation(annotationRecord.annotation().target(),
        annotationRecord.annotation()
            .creator(), annotationRecord.annotation().motivation())).willReturn(Optional.empty());
    given(handleComponent.postHandle(any())).willReturn(ID);
    given(repository.createAnnotationRecord(any(AnnotationRecord.class))).willReturn(1);
    given(elasticRepository.indexAnnotation(any(AnnotationRecord.class))).willThrow(
        IOException.class);

    // When
    assertThatThrownBy(() -> service.handleMessage(givenAnnotationEvent())).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(repository).should().rollbackAnnotation(ID);
    then(fdoRecordService).should().buildRollbackCreationRequest(annotationRecord);
    then(handleComponent).should().rollbackHandleCreation(any());
    then(kafkaPublisherService).shouldHaveNoInteractions();
  }

  @Test
  void testHandleEqualMessage()
      throws Exception {
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
    then(handleComponent).shouldHaveNoInteractions();
  }

  @Test
  void testHandleUpdateMessage()
      throws Exception {
    // Given
    var annotationRecord = givenAnnotationRecord(givenAnnotationEvent());
    given(repository.getAnnotation(annotationRecord.annotation().target(),
        annotationRecord.annotation()
            .creator(), annotationRecord.annotation().motivation())).willReturn(
        Optional.of(givenAnnotationRecord("Another Motivation", "Another Creator")));
    given(repository.createAnnotationRecord(any(AnnotationRecord.class))).willReturn(1);
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.Updated);
    given(elasticRepository.indexAnnotation(any(AnnotationRecord.class))).willReturn(indexResponse);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    var result = service.handleMessage(givenAnnotationEvent());

    // Then
    assertThat(result).isNotNull().isInstanceOf(AnnotationRecord.class);
    assertThat(result.id()).isEqualTo(ID);
    then(fdoRecordService).should()
        .buildPatchRollbackHandleRequest(annotationRecord.annotation(), ID);
    then(handleComponent).should().updateHandle(any());
    then(kafkaPublisherService).should()
        .publishUpdateEvent(any(AnnotationRecord.class), any(AnnotationRecord.class));
  }

  @Test
  void testHandleUpdateMessageElasticException() throws Exception {
    // Given
    var annotationRecord = givenAnnotationRecord(givenAnnotationEvent());
    given(repository.getAnnotation(annotationRecord.annotation().target(),
        annotationRecord.annotation()
            .creator(), annotationRecord.annotation().motivation())).willReturn(
        Optional.of(givenAnnotationRecord("Another Motivation", "Another Creator")));
    given(repository.createAnnotationRecord(any(AnnotationRecord.class))).willReturn(1);
    given(elasticRepository.indexAnnotation(any(AnnotationRecord.class))).willThrow(
        IOException.class);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    assertThatThrownBy(() -> service.handleMessage(givenAnnotationEvent())).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(fdoRecordService).should((times(2)))
        .buildPatchRollbackHandleRequest(annotationRecord.annotation(), ID);
    then(handleComponent).should().updateHandle(any());
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(repository).should(times(2)).createAnnotationRecord(any(AnnotationRecord.class));
    then(kafkaPublisherService).shouldHaveNoInteractions();
  }

  @Test
  void testHandleUpdateMessageKafkaException() throws Exception {
    // Given
    var annotationRecord = givenAnnotationRecord(givenAnnotationEvent());
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
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    assertThatThrownBy(() -> service.handleMessage(givenAnnotationEvent())).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(fdoRecordService).should((times(2)))
        .buildPatchRollbackHandleRequest(annotationRecord.annotation(), ID);
    then(handleComponent).should().updateHandle(any());
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(elasticRepository).should(times(2)).indexAnnotation(any(AnnotationRecord.class));
    then(repository).should(times(2)).createAnnotationRecord(any(AnnotationRecord.class));
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
    then(handleComponent).should().archiveHandle(any());
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
    then(handleComponent).shouldHaveNoInteractions();
  }

}
