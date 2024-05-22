package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.ANNOTATION_HASH;
import static eu.dissco.annotationprocessingservice.TestUtils.ANNOTATION_HASH_2;
import static eu.dissco.annotationprocessingservice.TestUtils.ANNOTATION_HASH_3;
import static eu.dissco.annotationprocessingservice.TestUtils.CREATED;
import static eu.dissco.annotationprocessingservice.TestUtils.HANDLE_PROXY;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.ID_ALT;
import static eu.dissco.annotationprocessingservice.TestUtils.JOB_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAggregationRating;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationEvent;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessedAlt;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRequest;
import static eu.dissco.annotationprocessingservice.TestUtils.givenBatchMetadataExtendedLatitudeSearch;
import static eu.dissco.annotationprocessingservice.TestUtils.givenCreator;
import static eu.dissco.annotationprocessingservice.TestUtils.givenHashedAnnotation;
import static eu.dissco.annotationprocessingservice.TestUtils.givenHashedAnnotationAlt;
import static eu.dissco.annotationprocessingservice.TestUtils.givenOaBodySetType;
import static eu.dissco.annotationprocessingservice.TestUtils.givenOaTarget;
import static eu.dissco.annotationprocessingservice.TestUtils.givenPatchRequest;
import static eu.dissco.annotationprocessingservice.TestUtils.givenPostRequest;
import static eu.dissco.annotationprocessingservice.TestUtils.givenRollbackCreationRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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
import eu.dissco.annotationprocessingservice.component.AnnotationHasher;
import eu.dissco.annotationprocessingservice.component.SchemaValidatorComponent;
import eu.dissco.annotationprocessingservice.database.jooq.enums.ErrorCode;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.domain.HashedAnnotation;
import eu.dissco.annotationprocessingservice.domain.MasJobRecord;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.domain.annotation.Body;
import eu.dissco.annotationprocessingservice.domain.annotation.Motivation;
import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import eu.dissco.annotationprocessingservice.exception.BatchingException;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.repository.AnnotationBatchRecordRepository;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.jooq.exception.DataAccessException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
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
  @Mock
  AnnotationHasher annotationHasher;
  @Mock
  SchemaValidatorComponent schemaValidator;
  @Mock
  BatchAnnotationService batchAnnotationService;
  @Mock
  AnnotationBatchRecordService annotationBatchRecordService;
  @Captor
  ArgumentCaptor<List<Annotation>> captor;
  private MockedStatic<Instant> mockedStatic;
  private final Instant instant = Instant.now(Clock.fixed(CREATED, ZoneOffset.UTC));
  private ProcessingKafkaService service;
  Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
  MockedStatic<Clock> mockedClock = mockStatic(Clock.class);
  MockedStatic<UUID> mockedUuid;

  @BeforeEach
  void setup() {
    service = new ProcessingKafkaService(repository, elasticRepository,
        kafkaPublisherService, fdoRecordService, handleComponent, applicationProperties,
        masJobRecordService, annotationHasher, schemaValidator, batchAnnotationService,
        annotationBatchRecordService);
    mockedStatic = mockStatic(Instant.class);
    mockedStatic.when(Instant::now).thenReturn(instant);
    mockedClock.when(Clock::systemUTC).thenReturn(clock);
    mockedUuid = mockStatic(UUID.class);
    mockedUuid.when(UUID::randomUUID).thenReturn(ANNOTATION_HASH_3);
  }

  @AfterEach
  void destroy() {
    mockedStatic.close();
    mockedClock.close();
    mockedUuid.close();
    ;
  }

  @Test
  void testNewMessage()
      throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest();
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postHandles(any())).willReturn(Map.of(ANNOTATION_HASH, ID));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(anyList())).willReturn(bulkResponse);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    given(applicationProperties.getHandleProxy()).willReturn(HANDLE_PROXY);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, false, null));

    // When
    service.handleMessage(givenAnnotationEvent(annotationRequest));

    // Then
    then(repository).should().createAnnotationRecord(List.of(givenHashedAnnotation()));
    then(kafkaPublisherService).should().publishCreateEvent(givenAnnotationProcessed());
    then(masJobRecordService).should().markMasJobRecordAsComplete(JOB_ID, List.of(ID), false);
  }

  @Test
  void testNewMessageIsBatchResult() throws Exception {
    // Given
    var event = new AnnotationEvent(List.of(givenAnnotationRequest()), JOB_ID, null, true);
    var mjr = new MasJobRecord(JOB_ID, true, null);
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postHandles(any())).willReturn(Map.of(ANNOTATION_HASH, ID));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(anyList())).willReturn(bulkResponse);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    given(applicationProperties.getHandleProxy()).willReturn(HANDLE_PROXY);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(mjr);
    given(annotationBatchRecordService.getBatchId(eq(mjr), eq(true), anyList())).willReturn(
        Optional.of(ANNOTATION_HASH_3));

    // When
    service.handleMessage(event);

    // Then
    then(repository).should()
        .createAnnotationRecord(List.of(givenHashedAnnotation(ANNOTATION_HASH_3)));
    then(kafkaPublisherService).should()
        .publishCreateEvent(givenAnnotationProcessed().setOdsBatchId(ANNOTATION_HASH_3));
    then(masJobRecordService).should().markMasJobRecordAsComplete(JOB_ID, List.of(ID), true);
    then(batchAnnotationService).shouldHaveNoInteractions();
  }

  @Test
  void testEmptyAnnotations() throws Exception {
    // Given
    var event = new AnnotationEvent(Collections.emptyList(), JOB_ID, null, null);

    // When
    service.handleMessage(event);

    // Then
    then(masJobRecordService).should().markEmptyMasJobRecordAsComplete(JOB_ID, false);
    then(kafkaPublisherService).shouldHaveNoInteractions();
    then(repository).shouldHaveNoInteractions();
    then(fdoRecordService).shouldHaveNoInteractions();
    then(handleComponent).shouldHaveNoInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
  }

  @Test
  void testNewMessagePartialElasticFailure() throws Exception {
    // Given
    var annotation = givenAnnotationRequest();
    var secondAnnotation = givenAnnotationRequest()
        .setOaTarget(givenOaTarget("alt target"));
    var event = new AnnotationEvent(List.of(annotation, secondAnnotation), JOB_ID, null, null);
    Map<UUID, String> idMap = Map.of(ANNOTATION_HASH, ID, ANNOTATION_HASH_2, ID_ALT);
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH)
        .willReturn(ANNOTATION_HASH_2);
    given(handleComponent.postHandles(any())).willReturn(idMap);
    given(repository.getAnnotationFromHash(any())).willReturn(Collections.emptyList());
    givenBulkResponse();
    given(elasticRepository.indexAnnotations(anyList())).willReturn(bulkResponse);
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, false, null));

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
    then(masJobRecordService).should().markMasJobRecordAsFailed(JOB_ID, false);
    then(elasticRepository).should().archiveAnnotations(List.of(ID));
  }

  @Test
  void testNewMessageHandleFailureBatchingRequested() throws Exception {
    // Given
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postHandles(any())).willThrow(PidCreationException.class);
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, false, null));

    // Then
    assertThrows(FailedProcessingException.class,
        () -> service.handleMessage(givenAnnotationEvent()));
    then(masJobRecordService).should().markMasJobRecordAsFailed(JOB_ID, false);
  }

  @Test
  void testHandleNewMessageKafkaException()
      throws Exception {
    // Given
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postHandles(any())).willReturn(Map.of(ANNOTATION_HASH, ID));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(anyList())).willReturn(bulkResponse);
    doThrow(JsonProcessingException.class).when(kafkaPublisherService).publishCreateEvent(any(
        Annotation.class));
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, false, null));

    // When
    assertThatThrownBy(() -> service.handleMessage(givenAnnotationEvent())).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(elasticRepository).should().archiveAnnotations(List.of(ID));
    then(repository).should().rollbackAnnotations(List.of(ID));
    then(fdoRecordService).should().buildRollbackCreationRequest(List.of(ID));
    then(handleComponent).should().rollbackHandleCreation(any());
    then(masJobRecordService).should().markMasJobRecordAsFailed(JOB_ID, false);
  }

  @Test
  void testHandleNewMessageKafkaExceptionBatchingRequested() throws Exception {
    // Given
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postHandles(any())).willReturn(Map.of(ANNOTATION_HASH, ID));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(anyList())).willReturn(bulkResponse);
    doThrow(JsonProcessingException.class).when(kafkaPublisherService).publishCreateEvent(any(
        Annotation.class));
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, true, null));

    // When
    assertThatThrownBy(() -> service.handleMessage(givenAnnotationEvent())).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(elasticRepository).should().archiveAnnotations(List.of(ID));
    then(repository).should().rollbackAnnotations(List.of(ID));
    then(fdoRecordService).should().buildRollbackCreationRequest(List.of(ID));
    then(handleComponent).should().rollbackHandleCreation(any());
    then(masJobRecordService).should().markMasJobRecordAsFailed(JOB_ID, false);
    then(annotationBatchRecordService).should()
        .rollbackAnnotationBatchRecord(Optional.of(ANNOTATION_HASH_3), false);
  }

  @Test
  void testHandleNewMessageElasticIOException()
      throws Exception {
    // Given
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postHandles(any())).willReturn(Map.of(ANNOTATION_HASH, ID));
    given(elasticRepository.indexAnnotations(anyList())).willThrow(
        IOException.class);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, false, null));

    // When
    assertThatThrownBy(() -> service.handleMessage(givenAnnotationEvent())).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(repository).should().rollbackAnnotations(List.of(ID));
    then(fdoRecordService).should().buildRollbackCreationRequest(List.of(ID));
    then(handleComponent).should().rollbackHandleCreation(any());
    then(kafkaPublisherService).shouldHaveNoInteractions();
    then(masJobRecordService).should().markMasJobRecordAsFailed(JOB_ID, false);
  }

  @Test
  void testHandleEqualMessage()
      throws Exception {
    // Given
    var annotation = givenAnnotationProcessed();
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(any())).willReturn(List.of(givenHashedAnnotation()));
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, false, null));

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
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, false, null));

    // When
    service.handleMessage(givenAnnotationEvent(annotationRequest));

    // Then
    then(fdoRecordService).should()
        .buildPatchRollbackHandleRequest(anyList());
    then(handleComponent).should().updateHandle(any());
    then(kafkaPublisherService).should()
        .publishUpdateEvent(any(Annotation.class), any(Annotation.class));
    then(masJobRecordService).should().markMasJobRecordAsComplete(JOB_ID, List.of(ID), false);
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
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, false, null));

    // Then
    assertThrows(FailedProcessingException.class,
        () -> service.handleMessage(givenAnnotationEvent(annotationRequest)));
    then(masJobRecordService).should()
        .markMasJobRecordAsFailed(JOB_ID, false);
  }

  @Test
  void testHandleMessageOneOfEach() throws Exception {
    // Given
    String changedId = "changedId";
    var equalId = "equalId";
    var newAnnotation = givenAnnotationRequest();
    var changedAnnotationNew = (givenAnnotationRequest().setOaTarget(
        givenOaTarget("changedTarget")))
        .setOaBody(Body.builder().oaValue(List.of("new value")).build());
    var changedAnnotationOriginal = (givenAnnotationProcessed().setOaTarget(
        givenOaTarget("changedTarget")).setOdsId(changedId));
    var changedAnnotationOriginalHashed = new HashedAnnotation(
        changedAnnotationOriginal, ANNOTATION_HASH_2);
    var equalAnnotation = givenAnnotationRequest().setOaTarget(givenOaTarget("equalTarget"));
    var equalAnnotationHashed = new HashedAnnotation(
        equalAnnotation.setOdsId(equalId).setOdsVersion(1), ANNOTATION_HASH_3);

    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH)
        .willReturn(ANNOTATION_HASH_2).willReturn(ANNOTATION_HASH_3);
    given(repository.getAnnotationFromHash(any())).willReturn(
        List.of(changedAnnotationOriginalHashed, equalAnnotationHashed));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(fdoRecordService.buildPatchRollbackHandleRequest(
        List.of(new HashedAnnotation(changedAnnotationNew, any()))))
        .willReturn(givenPatchRequest());
    given(fdoRecordService.buildPostHandleRequest(
        List.of(new HashedAnnotation(newAnnotation, any())))).willReturn(
        givenPostRequest());
    given(handleComponent.postHandles(any())).willReturn(Map.of(ANNOTATION_HASH, ID));
    given(elasticRepository.indexAnnotations(anyList())).willReturn(bulkResponse);
    given(bulkResponse.errors()).willReturn(false);
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, false, null));

    var event = new AnnotationEvent(List.of(newAnnotation, changedAnnotationNew, equalAnnotation),
        JOB_ID, null, null);

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
        .markMasJobRecordAsComplete(JOB_ID, List.of(equalId, changedId, ID), false);
    then(schemaValidator).should().validateEvent(event);
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
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, false, null));

    // When
    service.handleMessage(givenAnnotationEvent(annotationRequest));

    // Then
    then(fdoRecordService).should().buildPatchRollbackHandleRequest(anyList());
    then(kafkaPublisherService).should()
        .publishUpdateEvent(any(Annotation.class), any(Annotation.class));
    then(masJobRecordService).should().markMasJobRecordAsComplete(JOB_ID, List.of(ID), false);
  }

  private static Stream<Arguments> unequalAnnotations() {
    return Stream.of(
        Arguments.of(
            givenAnnotationProcessed().setOaBody(givenOaBodySetType("differentType"))),
        Arguments.of(givenAnnotationProcessed().setOaCreator(givenCreator("different creator"))),
        Arguments.of(givenAnnotationProcessed().setOaTarget(givenOaTarget("different target"))),
        Arguments.of(givenAnnotationProcessed().setOaMotivatedBy("different motivated by")),
        Arguments.of(givenAnnotationProcessed().setOdsAggregateRating(
            givenAggregationRating(0.99))),
        Arguments.of(givenAnnotationProcessed().setOaMotivation(Motivation.EDITING))
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
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, false, null));

    // When
    service.handleMessage(givenAnnotationEvent(annotationRequest));

    // Then
    then(fdoRecordService).shouldHaveNoMoreInteractions();
    then(handleComponent).shouldHaveNoInteractions();
    then(kafkaPublisherService).should()
        .publishUpdateEvent(any(Annotation.class), any(Annotation.class));
    then(masJobRecordService).should().markMasJobRecordAsComplete(JOB_ID, List.of(ID), false);
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
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, false, null));

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
    then(masJobRecordService).should().markMasJobRecordAsFailed(JOB_ID, false);
  }

  @Test
  void testUpdateMessagePartialElasticFailure() throws Exception {
    // Given
    var annotation = givenAnnotationRequest();
    var secondAnnotation = givenAnnotationProcessed()
        .setOdsId(ID_ALT)
        .setOaMotivatedBy("nature")
        .setOaTarget(givenOaTarget("alt target"));
    var secondAnnotationCurrent = givenAnnotationProcessed()
        .setOdsId(ID_ALT)
        .setOaMotivatedBy("science")
        .setOaTarget(givenOaTarget("alt target"));
    var secondAnnotationCurrentHashed = new HashedAnnotation(secondAnnotationCurrent,
        ANNOTATION_HASH_2);
    var event = new AnnotationEvent(List.of(annotation, secondAnnotation), JOB_ID, null, null);
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH)
        .willReturn(ANNOTATION_HASH_2);
    given(repository.getAnnotationFromHash(any())).willReturn(
        List.of(givenHashedAnnotationAlt(), secondAnnotationCurrentHashed));
    givenBulkResponse();
    given(elasticRepository.indexAnnotations(any())).willReturn(bulkResponse);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(fdoRecordService.buildPatchRollbackHandleRequest(anyList())).willReturn(
        List.of(givenRollbackCreationRequest()));
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, false, null));

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
    then(masJobRecordService).should().markMasJobRecordAsFailed(JOB_ID, false);
    verify(elasticRepository, times(2)).indexAnnotations(captor.capture());
    assertThat(captor.getAllValues().get(0)).hasSameElementsAs(
        List.of(annotation, secondAnnotation));
    assertThat(captor.getAllValues().get(1)).isEqualTo(List.of(givenAnnotationProcessedAlt()));
  }

  @Test
  void testHandleUpdateMessageKafkaException() throws Exception {
    // Given
    var annotation = givenAnnotationRequest().setOdsId(ID);
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(any())).willReturn(List.of(givenHashedAnnotationAlt()));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(any())).willReturn(bulkResponse);
    doThrow(JsonProcessingException.class).when(kafkaPublisherService).publishUpdateEvent(any(
        Annotation.class), any(Annotation.class));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(fdoRecordService.buildPatchRollbackHandleRequest(anyList())).willReturn(
        List.of(givenRollbackCreationRequest()));
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, false, null));

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
    then(masJobRecordService).should().markMasJobRecordAsFailed(JOB_ID, false);
  }

  @Test
  void testUpdateMessageKafkaExceptionHandleRollbackFailed() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest().setOdsId(ID);
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
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, false, null));

    // When
    assertThatThrownBy(
        () -> service.handleMessage(givenAnnotationEvent(annotationRequest))).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(masJobRecordService).should().markMasJobRecordAsFailed(any(), eq(false));
    then(fdoRecordService).should(times(2))
        .buildPatchRollbackHandleRequest(
            List.of(new HashedAnnotation(annotationRequest.setOdsVersion(2), ANNOTATION_HASH)));
    then(handleComponent).should().updateHandle(any());
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(elasticRepository).should(times(2)).indexAnnotations(anyList());
    then(repository).should(times(2)).createAnnotationRecord(anyList());
    then(masJobRecordService).should().markMasJobRecordAsFailed(JOB_ID, false);
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
    doThrow(AnnotationValidationException.class).when(schemaValidator).validateEvent(any());

    // Then
    assertThrows(AnnotationValidationException.class,
        () -> service.handleMessage(givenAnnotationEvent()));
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

  @Test
  void testNewMessageBatchEnabled() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest();
    var event = new AnnotationEvent(List.of(annotationRequest), JOB_ID,
        List.of(givenBatchMetadataExtendedLatitudeSearch()),
        false);
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postHandles(any())).willReturn(Map.of(ANNOTATION_HASH, ID));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(anyList())).willReturn(bulkResponse);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    given(applicationProperties.getHandleProxy()).willReturn(HANDLE_PROXY);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, true, null));

    // When
    service.handleMessage(event);

    // Then
    then(repository).should()
        .createAnnotationRecord(List.of(givenHashedAnnotation(ANNOTATION_HASH_3)));
    then(kafkaPublisherService).should()
        .publishCreateEvent(givenAnnotationProcessed().setOdsBatchId(ANNOTATION_HASH_3));
    then(masJobRecordService).should().markMasJobRecordAsComplete(JOB_ID, List.of(ID), false);
    then(batchAnnotationService).should().applyBatchAnnotations(event, ANNOTATION_HASH_3);
  }

  @Test
  void testNewMessageBatchEnabledNoMetadata() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest();
    var event = new AnnotationEvent(List.of(annotationRequest), JOB_ID, null,
        false);
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postHandles(any())).willReturn(Map.of(ANNOTATION_HASH, ID));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(anyList())).willReturn(bulkResponse);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    given(applicationProperties.getHandleProxy()).willReturn(HANDLE_PROXY);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, true, null));

    // When
    service.handleMessage(event);

    // Then
    then(repository).should()
        .createAnnotationRecord(List.of(givenHashedAnnotation(ANNOTATION_HASH_3)));
    then(kafkaPublisherService).should()
        .publishCreateEvent(givenAnnotationProcessed().setOdsBatchId(ANNOTATION_HASH_3));
    then(masJobRecordService).should().markMasJobRecordAsComplete(JOB_ID, List.of(ID), false);
    then(batchAnnotationService).shouldHaveNoInteractions();
  }

  @Test
  void testNewMessageBatchEnabledBatchingException() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest();
    var event = new AnnotationEvent(List.of(annotationRequest), JOB_ID,
        List.of(givenBatchMetadataExtendedLatitudeSearch()),
        false);
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postHandles(any())).willReturn(Map.of(ANNOTATION_HASH, ID));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(anyList())).willReturn(bulkResponse);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    given(applicationProperties.getHandleProxy()).willReturn(HANDLE_PROXY);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, true, null));
    doThrow(BatchingException.class).when(batchAnnotationService).applyBatchAnnotations(event,
        ANNOTATION_HASH_3);

    // When
    assertDoesNotThrow(() -> service.handleMessage(event));

    // Then
    then(repository).should()
        .createAnnotationRecord(List.of(givenHashedAnnotation(ANNOTATION_HASH_3)));
    then(kafkaPublisherService).should()
        .publishCreateEvent(givenAnnotationProcessed().setOdsBatchId(ANNOTATION_HASH_3));
    then(masJobRecordService).should().markMasJobRecordAsComplete(JOB_ID, List.of(ID), false);
  }

  @Test
  void testNewMessageDataAccessException() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest();
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postHandles(any())).willReturn(Map.of(ANNOTATION_HASH, ID));
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    given(applicationProperties.getHandleProxy()).willReturn(HANDLE_PROXY);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    doThrow(DataAccessException.class).when(repository).createAnnotationRecord(anyList());
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, false, null));

    // When / Then
    assertThrows(FailedProcessingException.class,
        () -> service.handleMessage(givenAnnotationEvent(annotationRequest)));
    then(handleComponent).should().rollbackHandleCreation(any());
  }

  @Test
  void testUpdateMessageDataAccessException() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest();
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(any())).willReturn(List.of(givenHashedAnnotationAlt()));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(fdoRecordService.buildPatchRollbackHandleRequest(anyList())).willReturn(
        List.of(givenRollbackCreationRequest()));
    doThrow(DataAccessException.class).when(repository).createAnnotationRecord(anyList());
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, false, null));

    // When / Then
    assertThrows(FailedProcessingException.class,
        () -> service.handleMessage(givenAnnotationEvent(annotationRequest)));
    then(handleComponent).should().rollbackHandleUpdate(any());
  }

  @Test
  void testNewMessageResolveTimeout()
      throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest();
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postHandles(any())).willReturn(Map.of(ANNOTATION_HASH, ID));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(anyList())).willReturn(bulkResponse);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    given(applicationProperties.getHandleProxy()).willReturn(HANDLE_PROXY);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, true, ErrorCode.TIMEOUT));

    // When
    service.handleMessage(givenAnnotationEvent(annotationRequest));

    // Then
    then(repository).should()
        .createAnnotationRecord(List.of(givenHashedAnnotation(ANNOTATION_HASH_3)));
    then(kafkaPublisherService).should()
        .publishCreateEvent(givenAnnotationProcessed().setOdsBatchId(ANNOTATION_HASH_3));
    then(masJobRecordService).should().markMasJobRecordAsComplete(JOB_ID, List.of(ID), false);
  }
}

