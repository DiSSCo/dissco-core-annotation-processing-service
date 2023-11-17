package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.*;
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
import static org.mockito.Mockito.verify;

import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.annotationprocessingservice.component.SchemaValidatorComponent;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.domain.HashedAnnotation;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.domain.annotation.Body;
import eu.dissco.annotationprocessingservice.domain.annotation.Motivation;
import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.component.AnnotationHasher;
import eu.dissco.annotationprocessingservice.web.HandleComponent;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;

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
  @Mock
  AnnotationHasher annotationHasher;
  @Mock
  SchemaValidatorComponent schemaValidator;
  @Captor
  ArgumentCaptor<List<Annotation>> captor;
  private MockedStatic<Instant> mockedStatic;
  private final Instant instant = Instant.now(Clock.fixed(CREATED, ZoneOffset.UTC));
  private ProcessingKafkaService service;
  Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
  MockedStatic<Clock> mockedClock = mockStatic(Clock.class);

  @BeforeEach
  void setup() {
    service = new ProcessingKafkaService(repository, elasticRepository,
        kafkaPublisherService, fdoRecordService, handleComponent, applicationProperties,
        masJobRecordService, annotationHasher, schemaValidator);
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
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postBatchHandle(any())).willReturn(Map.of(ANNOTATION_HASH, ID));
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
  void testNewMessagePartialElasticFailure() throws Exception {
    // Given
    var annotation = givenAnnotationRequest();
    var secondAnnotation = givenAnnotationRequest()
        .withOaTarget(givenOaTarget("alt target"));
    var event = new AnnotationEvent(List.of(annotation, secondAnnotation), JOB_ID);
    Map<UUID, String> idMap = Map.of(ANNOTATION_HASH, ID, ANNOTATION_HASH_2, ID_ALT);
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH).willReturn(ANNOTATION_HASH_2);
    given(handleComponent.postBatchHandle(any())).willReturn(idMap);
    given(repository.getAnnotationFromHash(any())).willReturn(Collections.emptyList());
    givenBulkResponse();
    given(elasticRepository.indexAnnotations(anyList())).willReturn(bulkResponse);

    // When
    assertThatThrownBy(() -> service.handleMessage(event)).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(fdoRecordService).should().buildPostHandleRequest(anyList());
    then(fdoRecordService).should().buildRollbackCreationRequest(anyList());
    then(handleComponent).should().rollbackHandleCreation(any());
    then(repository).should(times(1)).createAnnotationRecord(anyList());
    then(repository).should(times(2)).rollbackAnnotations(anyList());
    then(kafkaPublisherService).shouldHaveNoInteractions();
    then(masJobRecordService).should().markMasJobRecordAsFailed(JOB_ID);
    then(elasticRepository).should().archiveAnnotations(List.of(ID));
  }

  @Test
  void testNewMessageHandleFailure()
      throws Exception {
    // Given

    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postBatchHandle(any())).willThrow(PidCreationException.class);

    // Then
    assertThrows(FailedProcessingException.class,
        () -> service.handleMessage(givenAnnotationEvent()));
    then(masJobRecordService).should().markMasJobRecordAsFailed(JOB_ID);
  }

  @Test
  void testHandleNewMessageKafkaException()
      throws Exception {
    // Given
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postBatchHandle(any())).willReturn(Map.of(ANNOTATION_HASH, ID));
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
  void testHandleNewMessageElasticIOException()
      throws Exception {
    // Given
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postBatchHandle(any())).willReturn(Map.of(ANNOTATION_HASH, ID));
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
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
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
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(any())).willReturn(List.of(givenHashedAnnotationAlt()));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(any())).willReturn(bulkResponse);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(fdoRecordService.buildPatchRollbackHandleRequest(anyList())).willReturn(
        List.of(givenRollbackCreationRequest()));

    // When
    service.handleMessage(givenAnnotationEvent(annotationRequest));

    // Then
    then(fdoRecordService).should()
        .buildPatchRollbackHandleRequest(anyList());
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
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(any())).willReturn(List.of(givenHashedAnnotationAlt()));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(fdoRecordService.buildPatchRollbackHandleRequest(anyList())).willReturn(
        List.of(givenRollbackCreationRequest()));
    doThrow(PidCreationException.class).when(handleComponent).updateHandle(any());

    // Then
    assertThrows(FailedProcessingException.class,
        () -> service.handleMessage(givenAnnotationEvent(annotationRequest)));
    then(masJobRecordService).should()
        .markMasJobRecordAsFailed(JOB_ID);
  }

  @Test
  void testHandleMessageOneOfEach() throws Exception {
    // Given
    String changedId = "changedId";
    var equalId = "equalId";
    var newAnnotation = givenAnnotationRequest();
    var changedAnnotationNew = (givenAnnotationRequest().withOaTarget(
        givenOaTarget("changedTarget")))
        .withOaBody(new Body().withOaValue(List.of("new value")));
    var changedAnnotationOriginal = (givenAnnotationProcessed().withOaTarget(
        givenOaTarget("changedTarget")).withOdsId(changedId));
    var changedAnnotationOriginalHashed = new HashedAnnotation(
        changedAnnotationOriginal, ANNOTATION_HASH_2);
    var equalAnnotation = givenAnnotationRequest().withOaTarget(givenOaTarget("equalTarget"));
    var equalAnnotationHashed = new HashedAnnotation(
        equalAnnotation.withOdsId(equalId).withOdsVersion(1), ANNOTATION_HASH_3);

    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH).willReturn(ANNOTATION_HASH_2).willReturn(ANNOTATION_HASH_3);
    given(repository.getAnnotationFromHash(any())).willReturn(
        List.of(changedAnnotationOriginalHashed, equalAnnotationHashed));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(fdoRecordService.buildPatchRollbackHandleRequest(List.of(new HashedAnnotation(changedAnnotationNew,any()))))
            .willReturn(givenPatchRequest());
    given(fdoRecordService.buildPostHandleRequest(
        List.of(new HashedAnnotation(newAnnotation, any())))).willReturn(
        givenPostRequest());
    given(handleComponent.postBatchHandle(any())).willReturn(Map.of(ANNOTATION_HASH, ID));
    given(elasticRepository.indexAnnotations(anyList())).willReturn(bulkResponse);
    given(bulkResponse.errors()).willReturn(false);

    var event = new AnnotationEvent(List.of(newAnnotation, changedAnnotationNew, equalAnnotation),
        JOB_ID);

    // When
    service.handleMessage(event);

    // Then
    then(handleComponent).should().updateHandle(givenPatchRequest());
    then(repository).should().updateLastChecked(List.of(equalId));
    then(repository).should(times(2)).createAnnotationRecord(anyList());
    then(kafkaPublisherService).should()
        .publishUpdateEvent(changedAnnotationOriginal, changedAnnotationNew);
    then(kafkaPublisherService).should().publishCreateEvent(newAnnotation);
    then(masJobRecordService).should()
        .markMasJobRecordAsComplete(JOB_ID, List.of(equalId, changedId, ID));
  }

  @ParameterizedTest
  @MethodSource("unequalAnnotations")
  void testAnnotationsAreNotEqual(Annotation currentAnnotation) throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest();
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(any())).willReturn(
        List.of(new HashedAnnotation(currentAnnotation, ANNOTATION_HASH)));
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
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
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
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(any())).willReturn(List.of(givenHashedAnnotationAlt()));
    given(elasticRepository.indexAnnotations(anyList())).willThrow(
        IOException.class);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(fdoRecordService.buildPatchRollbackHandleRequest(anyList())).willReturn(
        List.of(givenRollbackCreationRequest()));

    // When
    assertThatThrownBy(() -> service.handleMessage(givenAnnotationEvent(annotation))).isInstanceOf(
        FailedProcessingException.class);

    // Then

    then(fdoRecordService).should((times(2)))
        .buildPatchRollbackHandleRequest(
            List.of(new HashedAnnotation(annotation, ANNOTATION_HASH)));
    then(handleComponent).should().updateHandle(any());
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(repository).should(times(2)).createAnnotationRecord(anyList());
    then(kafkaPublisherService).shouldHaveNoInteractions();
    then(masJobRecordService).should().markMasJobRecordAsFailed(JOB_ID);
  }

  @Test
  void testUpdateMessagePartialElasticFailure() throws Exception {
    // Given
    var annotation = givenAnnotationRequest();
    var secondAnnotation = givenAnnotationProcessed()
        .withOdsId(ID_ALT)
        .withOaMotivatedBy("nature")
        .withOaTarget(givenOaTarget("alt target"));
    var secondAnnotationCurrent = givenAnnotationProcessed()
        .withOdsId(ID_ALT)
        .withOaMotivatedBy("science")
        .withOaTarget(givenOaTarget("alt target"));
    var secondAnnotationCurrentHashed = new HashedAnnotation(secondAnnotationCurrent, ANNOTATION_HASH_2);
    var event = new AnnotationEvent(List.of(annotation, secondAnnotation), JOB_ID);
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH).willReturn(ANNOTATION_HASH_2);
    given(repository.getAnnotationFromHash(any())).willReturn(
        List.of(givenHashedAnnotationAlt(), secondAnnotationCurrentHashed));
    givenBulkResponse();
    given(elasticRepository.indexAnnotations(any())).willReturn(bulkResponse);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(fdoRecordService.buildPatchRollbackHandleRequest(anyList())).willReturn(
        List.of(givenRollbackCreationRequest()));

    // When
    assertThatThrownBy(() -> service.handleMessage(event)).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(fdoRecordService).should((times(2)))
        .buildPatchRollbackHandleRequest(anyList());
    then(handleComponent).should(times(1)).updateHandle(any());
    then(handleComponent).should(times(1)).rollbackHandleUpdate(any());
    then(repository).should(times(3)).createAnnotationRecord(anyList());
    then(kafkaPublisherService).shouldHaveNoInteractions();
    then(masJobRecordService).should().markMasJobRecordAsFailed(JOB_ID);
    verify(elasticRepository, times(2)).indexAnnotations(captor.capture());
    assertThat(captor.getAllValues().get(0)).hasSameElementsAs(
        List.of(annotation, secondAnnotation));
    assertThat(captor.getAllValues().get(1)).isEqualTo(List.of(givenAnnotationProcessedAlt()));
  }

  @Test
  void testHandleUpdateMessageKafkaException() throws Exception {
    // Given
    var annotation = givenAnnotationRequest().withOdsId(ID);
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(any())).willReturn(List.of(givenHashedAnnotationAlt()));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(any())).willReturn(bulkResponse);
    doThrow(JsonProcessingException.class).when(kafkaPublisherService).publishUpdateEvent(any(
        Annotation.class), any(Annotation.class));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(fdoRecordService.buildPatchRollbackHandleRequest(anyList())).willReturn(
        List.of(givenRollbackCreationRequest()));

    // When
    assertThatThrownBy(() -> service.handleMessage(givenAnnotationEvent(annotation))).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(fdoRecordService).should((times(2)))
        .buildPatchRollbackHandleRequest(
            List.of(new HashedAnnotation(annotation, ANNOTATION_HASH)));
    then(handleComponent).should().updateHandle(anyList());
    then(handleComponent).should().rollbackHandleUpdate(anyList());
    then(elasticRepository).should(times(2)).indexAnnotations(anyList());
    then(repository).should(times(2)).createAnnotationRecord(anyList());
    then(masJobRecordService).should().markMasJobRecordAsFailed(JOB_ID);
  }

  @Test
  void testUpdateMessageKafkaExceptionHandleRollbackFailed() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest().withOdsId(ID);
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(any())).willReturn(List.of(givenHashedAnnotationAlt()));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(any())).willReturn(bulkResponse);
    doThrow(JsonProcessingException.class).when(kafkaPublisherService).publishUpdateEvent(any(
        Annotation.class), any(Annotation.class));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(fdoRecordService.buildPatchRollbackHandleRequest(anyList())).willReturn(
        List.of(givenRollbackCreationRequest()));
    doThrow(PidCreationException.class).when(handleComponent).rollbackHandleUpdate(any());

    // When
    assertThatThrownBy(
        () -> service.handleMessage(givenAnnotationEvent(annotationRequest))).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(masJobRecordService).should().markMasJobRecordAsFailed(any());
    then(fdoRecordService).should(times(2))
        .buildPatchRollbackHandleRequest(
            List.of(new HashedAnnotation(annotationRequest.withOdsVersion(2), ANNOTATION_HASH)));
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

  @Test
  void testSchemaValidationFailed() throws Exception {
    //Given
    doThrow(AnnotationValidationException.class).when(schemaValidator).validateProcessResult(any());

    // Then
    assertThrows(AnnotationValidationException.class, () -> service.handleMessage(givenAnnotationEvent()));
  }

  private void givenBulkResponse() {
    var positiveResponse = mock(BulkResponseItem.class);
    given(positiveResponse.error()).willReturn(null);
    given(positiveResponse.id()).willReturn(ID);
    var negativeResponse = mock(BulkResponseItem.class);
    given(negativeResponse.error()).willReturn(new ErrorCause.Builder().reason("Crashed").build());
    given(negativeResponse.id()).willReturn(ID_ALT);
    given(bulkResponse.errors()).willReturn(true);
    given(bulkResponse.items()).willReturn(List.of(positiveResponse, negativeResponse));
  }

}
