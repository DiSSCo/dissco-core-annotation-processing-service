package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.ANNOTATION_HASH;
import static eu.dissco.annotationprocessingservice.TestUtils.CREATED;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.JOB_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAggregationRating;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationEvent;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessedAlt;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRequest;
import static eu.dissco.annotationprocessingservice.TestUtils.givenCreator;
import static eu.dissco.annotationprocessingservice.TestUtils.givenHashedAnnotation;
import static eu.dissco.annotationprocessingservice.TestUtils.givenHashedAnnotationAlt;
import static eu.dissco.annotationprocessingservice.TestUtils.givenOaBody;
import static eu.dissco.annotationprocessingservice.TestUtils.givenOaTarget;
import static eu.dissco.annotationprocessingservice.TestUtils.givenRollbackCreationRequest;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.annotationprocessingservice.domain.HashedAnnotation;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.domain.annotation.Motivation;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.web.HandleComponent;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ProcessingKafkaServiceTest {

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
  private MasJobRecordService masJobRecordService;
  @Mock
  private ApplicationProperties applicationProperties;
  @Mock
  private BulkResponse bulkResponse;
  private MockedStatic<Instant> mockedStatic;
  private final Instant instant = Instant.now(Clock.fixed(CREATED, ZoneOffset.UTC));
  private ProcessingKafkaService service;
  Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
  MockedStatic<Clock> mockedClock = mockStatic(Clock.class);

  @BeforeEach
  void setup() {
    service = new ProcessingKafkaService(repository, elasticRepository,
        kafkaPublisherService, fdoRecordService, handleComponent, applicationProperties,
        masJobRecordService);
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
    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postHandle(any())).willReturn(List.of(ID));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(anyList())).willReturn(bulkResponse);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");

    // When
    service.handleMessage(givenAnnotationEvent(annotationRequest));

    // Then
    then(repository).should().createAnnotationRecord(List.of(givenHashedAnnotation()));
    then(kafkaPublisherService).should().publishCreateEvent(givenAnnotationProcessed());
    then(masJobRecordService).should().markMasJobRecordAsComplete(JOB_ID, List.of(ID));
  }

  @Test
  void testNewMessageHandleFailure()
      throws Exception {
    // Given

    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postHandle(any())).willThrow(PidCreationException.class);

    // Then
    assertThrows(FailedProcessingException.class,
        () -> service.handleMessage(givenAnnotationEvent()));
    then(masJobRecordService).should().markMasJobRecordAsFailed(JOB_ID);
  }

  @Test
  void testHandleNewMessageKafkaException()
      throws Exception {
    // Given
    var annotation = givenAnnotationProcessed();
    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postHandle(any())).willReturn(List.of(ID));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(anyList())).willReturn(bulkResponse);
    doThrow(JsonProcessingException.class).when(kafkaPublisherService).publishCreateEvent(any(
        Annotation.class));
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");

    // When
    assertThatThrownBy(() -> service.handleMessage(givenAnnotationEvent())).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(elasticRepository).should().archiveAnnotations(List.of(ID));
    then(repository).should().rollbackAnnotations(List.of(ID));
    then(fdoRecordService).should().buildRollbackCreationRequest(List.of(ID));
    then(handleComponent).should().rollbackHandleCreation(any());
    then(masJobRecordService).should().markMasJobRecordAsFailed(JOB_ID);
  }

  @Test
  void testHandleNewMessageElasticException()
      throws Exception {
    // Given
    var annotation = givenAnnotationProcessed();
    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postHandle(any())).willReturn(List.of(ID));
    given(elasticRepository.indexAnnotations(anyList())).willThrow(
        IOException.class);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");

    // When
    assertThatThrownBy(() -> service.handleMessage(givenAnnotationEvent())).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(repository).should().rollbackAnnotations(List.of(ID));
    then(fdoRecordService).should().buildRollbackCreationRequest(List.of(ID));
    then(handleComponent).should().rollbackHandleCreation(any());
    then(kafkaPublisherService).shouldHaveNoInteractions();
    then(masJobRecordService).should().markMasJobRecordAsFailed(JOB_ID);
  }

  @Test
  void testHandleEqualMessage()
      throws Exception {
    // Given
    var annotation = givenAnnotationProcessed();

    given(repository.getAnnotationFromHash(any())).willReturn(List.of(givenHashedAnnotation()));

    // When
    service.handleMessage(givenAnnotationEvent(annotation));

    // Then
    then(kafkaPublisherService).shouldHaveNoInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
    then(handleComponent).shouldHaveNoInteractions();
  }

  @Test
  void testUpdateMessage()
      throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest();
    given(repository.getAnnotationFromHash(any())).willReturn(List.of(givenHashedAnnotationAlt()));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(any())).willReturn(bulkResponse);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(fdoRecordService.buildPatchRollbackHandleRequest(anyList())).willReturn(List.of(givenRollbackCreationRequest()));

    // When
    service.handleMessage(givenAnnotationEvent(annotationRequest));

    // Then
    then(fdoRecordService).should()
        .buildPatchRollbackHandleRequest(List.of(annotationRequest));
    then(handleComponent).should().updateHandle(any());
    then(kafkaPublisherService).should()
        .publishUpdateEvent(any(Annotation.class), any(Annotation.class));
    then(masJobRecordService).should().markMasJobRecordAsComplete(JOB_ID, List.of(ID));
  }

  @Test
  void testUpdateMessagePidCreationException()
      throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest();
    given(repository.getAnnotationFromHash(any())).willReturn(List.of(givenHashedAnnotationAlt()));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(fdoRecordService.buildPatchRollbackHandleRequest(anyList())).willReturn(List.of(givenRollbackCreationRequest()));
    doThrow(PidCreationException.class).when(handleComponent).updateHandle(any());

    // Then
    assertThrows(FailedProcessingException.class,
        () -> service.handleMessage(givenAnnotationEvent(annotationRequest)));
    then(masJobRecordService).should()
        .markMasJobRecordAsFailed(JOB_ID);
  }

  @ParameterizedTest
  @MethodSource("unequalAnnotations")
  void testAnnotationsAreNotEqual(Annotation currentAnnotation) throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest();
    given(repository.getAnnotationFromHash(any())).willReturn(List.of(givenHashedAnnotationAlt()));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(anyList())).willReturn(bulkResponse);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    service.handleMessage(givenAnnotationEvent(annotationRequest));

    // Then
    then(fdoRecordService).should().buildPatchRollbackHandleRequest(anyList());
    then(kafkaPublisherService).should()
        .publishUpdateEvent(any(Annotation.class), any(Annotation.class));
    then(masJobRecordService).should().markMasJobRecordAsComplete(JOB_ID, List.of(ID));
  }

  private static Stream<Arguments> unequalAnnotations() {
    return Stream.of(
        Arguments.of(
            givenAnnotationProcessed().withOaBody(givenOaBody().withOdsType("differentType"))),
        Arguments.of(givenAnnotationProcessed().withOaCreator(givenCreator("different creator"))),
        Arguments.of(givenAnnotationProcessed().withOaTarget(givenOaTarget("different target"))),
        Arguments.of(givenAnnotationProcessed().withOaMotivatedBy("different motivated by")),
        Arguments.of(givenAnnotationProcessed().withOdsAggregateRating(
            givenAggregationRating().withRatingValue(0.99))),
        Arguments.of(givenAnnotationProcessed().withOaMotivation(Motivation.EDITING))
    );
  }

  @Test
  void testUpdateMessageHandleDoesNotNeedUpdate()
      throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest();
    given(repository.getAnnotationFromHash(any())).willReturn(List.of(givenHashedAnnotationAlt()));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(anyList())).willReturn(bulkResponse);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(false);

    // When
    service.handleMessage(givenAnnotationEvent(annotationRequest));

    // Then
    then(fdoRecordService).shouldHaveNoMoreInteractions();
    then(handleComponent).shouldHaveNoInteractions();
    then(kafkaPublisherService).should()
        .publishUpdateEvent(any(Annotation.class), any(Annotation.class));
    then(masJobRecordService).should().markMasJobRecordAsComplete(JOB_ID, List.of(ID));
  }

  @Test
  void testUpdateMessageElasticException() throws Exception {
    // Given
    var annotation = givenAnnotationRequest();
    given(repository.getAnnotationFromHash(any())).willReturn(List.of(givenHashedAnnotationAlt()));
    given(elasticRepository.indexAnnotations(anyList())).willThrow(
        IOException.class);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(fdoRecordService.buildPatchRollbackHandleRequest(anyList())).willReturn(List.of(givenRollbackCreationRequest()));

    // When
    assertThatThrownBy(() -> service.handleMessage(givenAnnotationEvent(annotation))).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(fdoRecordService).should((times(2)))
        .buildPatchRollbackHandleRequest(List.of(annotation));
    then(handleComponent).should().updateHandle(any());
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(repository).should(times(2)).createAnnotationRecord(anyList());
    then(kafkaPublisherService).shouldHaveNoInteractions();
    then(masJobRecordService).should().markMasJobRecordAsFailed(JOB_ID);
  }

  @Test
  void testHandleUpdateMessageKafkaException() throws Exception {
    // Given
    var annotation = givenAnnotationRequest().withOdsId(ID);
    given(repository.getAnnotationFromHash(any())).willReturn(List.of(givenHashedAnnotationAlt()));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(any())).willReturn(bulkResponse);
    doThrow(JsonProcessingException.class).when(kafkaPublisherService).publishUpdateEvent(any(
        Annotation.class), any(Annotation.class));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(fdoRecordService.buildPatchRollbackHandleRequest(anyList())).willReturn(List.of(givenRollbackCreationRequest()));

    // When
    assertThatThrownBy(() -> service.handleMessage(givenAnnotationEvent(annotation))).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(fdoRecordService).should((times(2)))
        .buildPatchRollbackHandleRequest(List.of(annotation));
    then(handleComponent).should().updateHandle(any());
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(elasticRepository).should(times(2)).indexAnnotations(anyList());
    then(repository).should(times(2)).createAnnotationRecord(anyList());
    then(masJobRecordService).should().markMasJobRecordAsFailed(JOB_ID);
  }

  @Test
  void testUpdateMessageKafkaExceptionHandleRollbackFailed() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest().withOdsId(ID);
    given(repository.getAnnotationFromHash(any())).willReturn(List.of(givenHashedAnnotationAlt()));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(any())).willReturn(bulkResponse);
    doThrow(JsonProcessingException.class).when(kafkaPublisherService).publishUpdateEvent(any(
        Annotation.class), any(Annotation.class));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(fdoRecordService.buildPatchRollbackHandleRequest(anyList())).willReturn(List.of(givenRollbackCreationRequest()));
    doThrow(PidCreationException.class).when(handleComponent).rollbackHandleUpdate(any());

    // When
    assertThatThrownBy(
        () -> service.handleMessage(givenAnnotationEvent(annotationRequest))).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(masJobRecordService).should().markMasJobRecordAsFailed(any());
    then(fdoRecordService).should(times(2))
        .buildPatchRollbackHandleRequest(List.of(annotationRequest.withOdsVersion(2)));
    then(handleComponent).should().updateHandle(any());
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(elasticRepository).should(times(2)).indexAnnotations(anyList());
    then(repository).should(times(2)).createAnnotationRecord(anyList());
    then(masJobRecordService).should().markMasJobRecordAsFailed(JOB_ID);
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
    then(masJobRecordService).shouldHaveNoInteractions();
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

  private void givenBulkResponse(){
    /*
    var positiveResponse = mock(BulkResponseItem.class);
    given(positiveResponse.error()).willReturn(null);
    given(positiveResponse.id()).willReturn(HANDLE).willReturn(THIRD_HANDLE);
     */
    var negativeResponse = mock(BulkResponseItem.class);
    given(negativeResponse.error()).willReturn(new ErrorCause.Builder().reason("Crashed").build());
    given(negativeResponse.id()).willReturn(ID);
    given(bulkResponse.errors()).willReturn(true);
    given(bulkResponse.items()).willReturn(List.of(negativeResponse));
  }

}
