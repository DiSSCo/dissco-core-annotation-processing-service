package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.ANNOTATION_HASH;
import static eu.dissco.annotationprocessingservice.TestUtils.ANNOTATION_HASH_2;
import static eu.dissco.annotationprocessingservice.TestUtils.ANNOTATION_HASH_3;
import static eu.dissco.annotationprocessingservice.TestUtils.BARE_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.BATCH_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.CREATED;
import static eu.dissco.annotationprocessingservice.TestUtils.FDO_TYPE;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.ID_ALT;
import static eu.dissco.annotationprocessingservice.TestUtils.JOB_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.PROCESSOR_HANDLE;
import static eu.dissco.annotationprocessingservice.TestUtils.UPDATED;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAggregationRating;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationBatchMetadataLatitudeSearch;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationBatchMetadataTwoParam;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationEvent;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRequest;
import static eu.dissco.annotationprocessingservice.TestUtils.givenBaseAnnotationForBatch;
import static eu.dissco.annotationprocessingservice.TestUtils.givenBatchIdMap;
import static eu.dissco.annotationprocessingservice.TestUtils.givenCreator;
import static eu.dissco.annotationprocessingservice.TestUtils.givenHashedAnnotation;
import static eu.dissco.annotationprocessingservice.TestUtils.givenHashedAnnotationAlt;
import static eu.dissco.annotationprocessingservice.TestUtils.givenOaBodySetType;
import static eu.dissco.annotationprocessingservice.TestUtils.givenOaTarget;
import static eu.dissco.annotationprocessingservice.TestUtils.givenPatchRequest;
import static eu.dissco.annotationprocessingservice.TestUtils.givenPostRequest;
import static eu.dissco.annotationprocessingservice.TestUtils.givenProcessingAgent;
import static eu.dissco.annotationprocessingservice.TestUtils.givenRequestOaTarget;
import static eu.dissco.annotationprocessingservice.TestUtils.givenRollbackCreationRequest;
import static eu.dissco.annotationprocessingservice.TestUtils.givenTombstoneAnnotation;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
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
import eu.dissco.annotationprocessingservice.component.AnnotationHasher;
import eu.dissco.annotationprocessingservice.component.AnnotationValidatorComponent;
import eu.dissco.annotationprocessingservice.database.jooq.enums.ErrorCode;
import eu.dissco.annotationprocessingservice.domain.HashedAnnotation;
import eu.dissco.annotationprocessingservice.domain.HashedAnnotationRequest;
import eu.dissco.annotationprocessingservice.domain.MasJobRecord;
import eu.dissco.annotationprocessingservice.domain.ProcessedAnnotationBatch;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.MethodNotAllowedException;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.properties.FdoProperties;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.schema.Annotation;
import eu.dissco.annotationprocessingservice.schema.Annotation.OaMotivation;
import eu.dissco.annotationprocessingservice.schema.Annotation.OdsMergingDecisionStatus;
import eu.dissco.annotationprocessingservice.schema.AnnotationBody;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingEvent;
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
class ProcessingMasServiceTest {

  private final Instant instant = Instant.now(Clock.fixed(CREATED, ZoneOffset.UTC));
  @Mock
  AnnotationHasher annotationHasher;
  @Mock
  AnnotationValidatorComponent schemaValidator;
  @Mock
  BatchAnnotationService batchAnnotationService;
  @Mock
  AnnotationBatchRecordService annotationBatchRecordService;
  @Captor
  ArgumentCaptor<List<Annotation>> captor;
  Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
  Clock updatedClock = Clock.fixed(UPDATED, ZoneOffset.UTC);
  MockedStatic<Clock> mockedClock = mockStatic(Clock.class);
  MockedStatic<UUID> mockedUuid;
  @Mock
  private AnnotationRepository repository;
  @Mock
  private ElasticSearchRepository elasticRepository;
  @Mock
  private RabbitMqPublisherService rabbitMqPublisherService;
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
  private FdoProperties fdoProperties;
  @Mock
  RollbackService rollbackService;
  private MockedStatic<Instant> mockedStatic;
  private ProcessingMasService service;

  private static Stream<Arguments> unequalAnnotations() {
    return Stream.of(
        Arguments.of(
            givenAnnotationProcessed().withOaHasBody(givenOaBodySetType("differentType"))),
        Arguments.of(
            givenAnnotationProcessed().withDctermsCreator(givenCreator("different creator"))),
        Arguments.of(givenAnnotationProcessed().withOaHasTarget(givenOaTarget("different target"))),
        Arguments.of(givenAnnotationProcessed().withOaMotivatedBy("different motivated by")),
        Arguments.of(givenAnnotationProcessed().withOdsHasAggregateRating(
            givenAggregationRating(0.99))),
        Arguments.of(givenAnnotationProcessed().withOaMotivation(OaMotivation.OA_EDITING))
    );
  }

  @BeforeEach
  void setup() {
    service = new ProcessingMasService(repository, elasticRepository,
        rabbitMqPublisherService, fdoRecordService, handleComponent, applicationProperties,
        masJobRecordService, annotationHasher, schemaValidator, batchAnnotationService,
        annotationBatchRecordService, fdoProperties, rollbackService);
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
  }

  @Test
  void testNewMessage()
      throws Exception {
    // Given
    boolean batchingRequested = false;
    var annotationRequest = givenAnnotationRequest();
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postHandlesHashed(any())).willReturn(Map.of(ANNOTATION_HASH, BARE_ID));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(anyList())).willReturn(bulkResponse);
    given(applicationProperties.getProcessorHandle()).willReturn(
        PROCESSOR_HANDLE);
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, batchingRequested, null));
    given(
        annotationBatchRecordService.mintBatchIds(any(), eq(batchingRequested), any())).willReturn(
        Optional.empty());
    given(applicationProperties.getProcessorName()).willReturn(
        "annotation-processing-service");
    given(fdoProperties.getType()).willReturn(FDO_TYPE);

    // When
    service.handleMessage(givenAnnotationEvent(annotationRequest));

    // Then
    then(repository).should().createAnnotationRecordsHashed(List.of(givenHashedAnnotation()));
    then(rabbitMqPublisherService).should().publishCreateEvent(givenAnnotationProcessed());
    then(masJobRecordService).should().markMasJobRecordAsComplete(JOB_ID, List.of(ID), false);
    then(annotationBatchRecordService).shouldHaveNoMoreInteractions();
  }

  @Test
  void testNewMessageIsBatchResult() throws Exception {
    // Given
    boolean batchingRequested = true;
    var event = new AnnotationProcessingEvent(JOB_ID, List.of(givenAnnotationRequest()), null,
        BATCH_ID);
    var mjr = new MasJobRecord(JOB_ID, batchingRequested, null);
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postHandlesHashed(any())).willReturn(Map.of(ANNOTATION_HASH, BARE_ID));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(anyList())).willReturn(bulkResponse);
    given(applicationProperties.getProcessorHandle()).willReturn(
        PROCESSOR_HANDLE);
    given(applicationProperties.getProcessorHandle()).willReturn(
        PROCESSOR_HANDLE);
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(mjr);
    given(annotationBatchRecordService.mintBatchIds(anyList(), eq(batchingRequested),
        eq(event))).willReturn(Optional.empty());
    given(applicationProperties.getProcessorName()).willReturn(
        "annotation-processing-service");
    given(fdoProperties.getType()).willReturn(FDO_TYPE);

    // When
    service.handleMessage(event);

    // Then
    then(repository).should()
        .createAnnotationRecordsHashed(List.of(givenHashedAnnotation(BATCH_ID)));
    then(rabbitMqPublisherService).should()
        .publishCreateEvent(givenAnnotationProcessed().withOdsBatchID(BATCH_ID));
    then(masJobRecordService).should().markMasJobRecordAsComplete(JOB_ID, List.of(ID), true);
    then(annotationBatchRecordService).should().updateAnnotationBatchRecord(BATCH_ID, 1);
    then(batchAnnotationService).shouldHaveNoInteractions();
  }

  @Test
  void testEmptyAnnotations() throws Exception {
    // Given
    var event = new AnnotationProcessingEvent(JOB_ID, Collections.emptyList(), null, null);

    // When
    service.handleMessage(event);

    // Then
    then(masJobRecordService).should().markEmptyMasJobRecordAsComplete(JOB_ID, false);
    then(rabbitMqPublisherService).shouldHaveNoInteractions();
    then(repository).shouldHaveNoInteractions();
    then(fdoRecordService).shouldHaveNoInteractions();
    then(handleComponent).shouldHaveNoInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
    then(annotationBatchRecordService).shouldHaveNoInteractions();
  }

  @Test
  void testNewMessagePartialElasticFailure() throws Exception {
    // Given
    var annotation = givenAnnotationRequest();
    boolean batchingRequested = false;
    var secondAnnotation = givenAnnotationRequest()
        .withOaHasTarget(givenRequestOaTarget("alt target"));
    var event = new AnnotationProcessingEvent(JOB_ID, List.of(annotation, secondAnnotation), null,
        null);
    Map<UUID, String> idMap = Map.of(ANNOTATION_HASH, BARE_ID, ANNOTATION_HASH_2,
        "20.5000.1025/ZZZ-YYY-XXX");
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH)
        .willReturn(ANNOTATION_HASH_2);
    given(handleComponent.postHandlesHashed(any())).willReturn(idMap);
    given(repository.getAnnotationFromHash(any())).willReturn(Collections.emptyList());
    givenBulkResponse();
    given(elasticRepository.indexAnnotations(anyList())).willReturn(bulkResponse);
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, batchingRequested, null));
    given(
        annotationBatchRecordService.mintBatchIds(any(), eq(batchingRequested), any())).willReturn(
        Optional.empty());

    // When
    assertThatThrownBy(() -> service.handleMessage(event)).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(rollbackService).should().rollbackNewAnnotations(anyList(), eq(true), eq(true));
    then(rollbackService).should().rollbackBatchIds(Optional.empty());
    then(masJobRecordService).should().markMasJobRecordAsFailed(eq(JOB_ID), eq(false),
        eq(ErrorCode.DISSCO_EXCEPTION), any());
  }

  @Test
  void testNewMessageHandleFailureBatchingRequested() throws Exception {
    // Given
    boolean batchingRequested = true;
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postHandlesHashed(any())).willThrow(PidCreationException.class);
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, batchingRequested, null));

    // Then
    assertThrows(FailedProcessingException.class,
        () -> service.handleMessage(givenAnnotationEvent()));
    then(masJobRecordService).should().markMasJobRecordAsFailed(eq(JOB_ID), eq(false),
        eq(ErrorCode.DISSCO_EXCEPTION), any());
  }

  @Test
  void testHandleNewMessageKafkaException()
      throws Exception {
    // Given
    boolean batchingRequested = false;
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postHandlesHashed(any())).willReturn(Map.of(ANNOTATION_HASH, BARE_ID));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(anyList())).willReturn(bulkResponse);
    doThrow(JsonProcessingException.class).when(rabbitMqPublisherService).publishCreateEvent(any(
        Annotation.class));
    given(applicationProperties.getProcessorHandle()).willReturn(
        PROCESSOR_HANDLE);
    given(applicationProperties.getProcessorHandle()).willReturn(
        PROCESSOR_HANDLE);
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, batchingRequested, null));

    // When
    assertThatThrownBy(() -> service.handleMessage(givenAnnotationEvent())).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(rollbackService).should().rollbackNewAnnotations(anyList(), eq(true), eq(true));
    then(masJobRecordService).should().markMasJobRecordAsFailed(eq(JOB_ID), eq(false), eq(ErrorCode.DISSCO_EXCEPTION), any());
    then(rollbackService).should().rollbackBatchIds(Optional.empty());
    then(masJobRecordService).should().markMasJobRecordAsFailed(eq(JOB_ID), eq(false),
        eq(ErrorCode.DISSCO_EXCEPTION), any());
  }

  @Test
  void testHandleNewMessageKafkaExceptionBatchingRequested() throws Exception {
    // Given
    boolean batchingRequested = true;
    var mjr = new MasJobRecord(JOB_ID, batchingRequested, null);
    var event = givenAnnotationEvent();
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postHandlesHashed(any())).willReturn(Map.of(ANNOTATION_HASH, BARE_ID));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(anyList())).willReturn(bulkResponse);
    doThrow(JsonProcessingException.class).when(rabbitMqPublisherService).publishCreateEvent(any(
        Annotation.class));
    given(applicationProperties.getProcessorHandle()).willReturn(
        PROCESSOR_HANDLE);
    given(applicationProperties.getProcessorHandle()).willReturn(
        PROCESSOR_HANDLE);
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(mjr);
    given(annotationBatchRecordService.mintBatchIds(anyList(), eq(batchingRequested),
        eq(event))).willReturn(
        Optional.of(givenBatchIdMap()));

    // When
    assertThatThrownBy(() -> service.handleMessage(event)).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(rollbackService).should()
        .rollbackNewAnnotations(anyList(), eq(true), eq(true));
    then(rollbackService).should().rollbackBatchIds(Optional.of(givenBatchIdMap()));
    then(masJobRecordService).should().markMasJobRecordAsFailed(eq(JOB_ID), eq(false), eq(ErrorCode.DISSCO_EXCEPTION), any());
    then(masJobRecordService).should().markMasJobRecordAsFailed(eq(JOB_ID), eq(false),
        eq(ErrorCode.DISSCO_EXCEPTION), any());
  }

  @Test
  void testHandleNewMessageElasticIOException()
      throws Exception {
    // Given
    boolean batchingRequested = false;
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postHandlesHashed(any())).willReturn(Map.of(ANNOTATION_HASH, BARE_ID));
    given(elasticRepository.indexAnnotations(anyList())).willThrow(
        IOException.class);
    given(applicationProperties.getProcessorHandle()).willReturn(
        PROCESSOR_HANDLE);
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, batchingRequested, null));
    given(
        annotationBatchRecordService.mintBatchIds(any(), eq(batchingRequested), any())).willReturn(
        Optional.empty());

    // When
    assertThatThrownBy(() -> service.handleMessage(givenAnnotationEvent())).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(rollbackService).should().rollbackNewAnnotations(anyList(), eq(false), eq(true));
    then(rollbackService).should().rollbackBatchIds(Optional.empty());
    then(masJobRecordService).should().markMasJobRecordAsFailed(eq(JOB_ID), eq(false),
        eq(ErrorCode.DISSCO_EXCEPTION), any());
  }

  @Test
  void testHandleEqualMessage()
      throws Exception {
    // Given
    boolean batchingRequested = false;
    var annotation = givenAnnotationRequest();
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(any())).willReturn(List.of(givenHashedAnnotation()));
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, batchingRequested, null));

    // When
    service.handleMessage(givenAnnotationEvent(annotation));

    // Then
    then(rabbitMqPublisherService).shouldHaveNoInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
    then(handleComponent).shouldHaveNoInteractions();
    then(annotationBatchRecordService).shouldHaveNoMoreInteractions();
  }

  @Test
  void testUpdateMessage()
      throws Exception {
    // Given
    boolean batchingRequested = false;
    var annotationRequest = givenAnnotationRequest();
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(any())).willReturn(List.of(givenHashedAnnotationAlt()));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(any())).willReturn(bulkResponse);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(fdoRecordService.buildPatchHandleRequest(anyList())).willReturn(
        List.of(givenRollbackCreationRequest()));
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, batchingRequested, null));

    // When
    service.handleMessage(givenAnnotationEvent(annotationRequest));

    // Then
    then(fdoRecordService).should()
        .buildPatchHandleRequest(anyList());
    then(handleComponent).should().updateHandle(any());
    then(rabbitMqPublisherService).should()
        .publishUpdateEvent(any(Annotation.class), any(Annotation.class));
    then(masJobRecordService).should().markMasJobRecordAsComplete(JOB_ID, List.of(ID), false);
    then(annotationBatchRecordService).shouldHaveNoMoreInteractions();
  }

  @Test
  void testUpdateMessagePidCreationException()
      throws Exception {
    // Given
    boolean batchingRequested = false;
    var annotationRequest = givenAnnotationRequest();
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(any())).willReturn(List.of(givenHashedAnnotationAlt()));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(fdoRecordService.buildPatchHandleRequest(anyList())).willReturn(
        List.of(givenRollbackCreationRequest()));
    doThrow(PidCreationException.class).when(handleComponent).updateHandle(any());
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, batchingRequested, null));

    // Then
    assertThrows(FailedProcessingException.class,
        () -> service.handleMessage(givenAnnotationEvent(annotationRequest)));
    then(masJobRecordService).should()
        .markMasJobRecordAsFailed(eq(JOB_ID), eq(false), eq(ErrorCode.DISSCO_EXCEPTION), any());
  }

  @Test
  void testHandleMessageOneOfEach() throws Exception {
    // Given
    var equalId = "equalId";
    boolean batchingRequested = false;
    var newAnnotationRequest = givenAnnotationRequest();
    var changedAnnotationRequest = givenAnnotationRequest().withOaHasTarget(
            givenRequestOaTarget("changedTarget"))
        .withOaHasBody(new AnnotationBody().withOaValue(List.of("new value")));
    var changedAnnotation = givenAnnotationProcessed().withOdsVersion(2).withOaHasTarget(
            givenOaTarget("changedTarget"))
        .withOaHasBody(new AnnotationBody().withOaValue(List.of("new value")));
    var changedAnnotationOriginal = givenAnnotationProcessed().withOaHasTarget(
        givenOaTarget("changedTarget"));
    var changedAnnotationOriginalHashed = new HashedAnnotation(
        changedAnnotationOriginal, ANNOTATION_HASH_2);
    var equalAnnotation = givenAnnotationRequest().withOaHasTarget(
        givenRequestOaTarget("equalTarget"));
    var equalAnnotationHashed = new HashedAnnotation(
        givenAnnotationProcessed().withId(equalId).withOdsVersion(1)
            .withOaHasTarget(givenOaTarget("equalTarget")), ANNOTATION_HASH_3);

    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH)
        .willReturn(ANNOTATION_HASH_2).willReturn(ANNOTATION_HASH_3);
    given(repository.getAnnotationFromHash(any())).willReturn(
        List.of(changedAnnotationOriginalHashed, equalAnnotationHashed));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(fdoRecordService.buildPatchHandleRequest(
        List.of(new HashedAnnotation(changedAnnotation, any()))))
        .willReturn(givenPatchRequest());
    given(fdoRecordService.buildPostHandleRequestHash(
        List.of(new HashedAnnotationRequest(newAnnotationRequest, any())))).willReturn(
        givenPostRequest());
    given(handleComponent.postHandlesHashed(any())).willReturn(Map.of(ANNOTATION_HASH, BARE_ID));
    given(elasticRepository.indexAnnotations(anyList())).willReturn(bulkResponse);
    given(bulkResponse.errors()).willReturn(false);
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, batchingRequested, null));
    given(annotationBatchRecordService.mintBatchIds(anyList(),
        eq(batchingRequested), any())).willReturn(Optional.empty());
    given(applicationProperties.getProcessorName()).willReturn(
        "annotation-processing-service");
    given(fdoProperties.getType()).willReturn(FDO_TYPE);

    var event = new AnnotationProcessingEvent(JOB_ID,
        List.of(newAnnotationRequest, changedAnnotationRequest, equalAnnotation),
        null, null);

    // When
    service.handleMessage(event);

    // Then
    then(handleComponent).should().updateHandle(givenPatchRequest());
    then(repository).should().updateLastChecked(List.of(equalId));
    then(repository).should(times(2)).createAnnotationRecordsHashed(anyList());
    then(rabbitMqPublisherService).should()
        .publishUpdateEvent(changedAnnotationOriginal, changedAnnotation);
    then(rabbitMqPublisherService).should().publishCreateEvent(givenAnnotationProcessed());
    then(masJobRecordService).should()
        .markMasJobRecordAsComplete(JOB_ID, List.of(equalId, ID, ID), false);
    then(annotationBatchRecordService).shouldHaveNoMoreInteractions();
  }

  @Test
  void testHandleMessageOneOfEachBatchingRequested() throws Exception {
    // Given
    var equalId = "equalId";
    boolean batchingRequested = false;
    var newAnnotationRequest = givenAnnotationRequest().withOdsPlaceInBatch(1);
    var newAnnotation = givenAnnotationProcessed().withOdsPlaceInBatch(1);
    var changedAnnotationRequest = givenAnnotationRequest().withOaHasTarget(
            givenRequestOaTarget("changedTarget"))
        .withOaHasBody(new AnnotationBody().withOaValue(List.of("new value")));
    var changedAnnotation = givenAnnotationProcessed().withOdsVersion(2).withOaHasTarget(
            givenOaTarget("changedTarget"))
        .withOaHasBody(new AnnotationBody().withOaValue(List.of("new value")));
    var changedAnnotationOriginal = givenAnnotationProcessed().withOaHasTarget(
        givenOaTarget("changedTarget"));
    var changedAnnotationOriginalHashed = new HashedAnnotation(
        changedAnnotationOriginal, ANNOTATION_HASH_2);
    var equalAnnotation = givenAnnotationRequest().withOaHasTarget(
        givenRequestOaTarget("equalTarget"));
    var equalAnnotationHashed = new HashedAnnotation(
        givenAnnotationProcessed().withId(equalId).withOdsVersion(1)
            .withOaHasTarget(givenOaTarget("equalTarget")), ANNOTATION_HASH_3);
    var batchMetadata = givenAnnotationBatchMetadataTwoParam();
    var event = new AnnotationProcessingEvent(JOB_ID,
        List.of(newAnnotationRequest, changedAnnotationRequest, equalAnnotation),
        List.of(batchMetadata), null);
    var processedEvent = new ProcessedAnnotationBatch(
        List.of(givenBaseAnnotationForBatch(1, ID, BATCH_ID)),
        JOB_ID, List.of(batchMetadata), null);

    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH)
        .willReturn(ANNOTATION_HASH_2).willReturn(ANNOTATION_HASH_3);
    given(repository.getAnnotationFromHash(any())).willReturn(
        List.of(changedAnnotationOriginalHashed, equalAnnotationHashed));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(fdoRecordService.buildPatchHandleRequest(
        List.of(new HashedAnnotation(changedAnnotation, any()))))
        .willReturn(givenPatchRequest());
    given(applicationProperties.getProcessorHandle()).willReturn(PROCESSOR_HANDLE);
    given(fdoRecordService.buildPostHandleRequestHash(
        List.of(new HashedAnnotationRequest(newAnnotationRequest, any())))).willReturn(
        givenPostRequest());
    given(handleComponent.postHandlesHashed(any())).willReturn(Map.of(ANNOTATION_HASH, BARE_ID));
    given(elasticRepository.indexAnnotations(anyList())).willReturn(bulkResponse);
    given(bulkResponse.errors()).willReturn(false);
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, batchingRequested, null));
    given(annotationBatchRecordService.mintBatchIds(eq(List.of(newAnnotation)),
        eq(batchingRequested), any())).willReturn(Optional.of(givenBatchIdMap()));
    given(applicationProperties.getProcessorName()).willReturn(
        "annotation-processing-service");
    given(fdoProperties.getType()).willReturn(FDO_TYPE);

    // When
    service.handleMessage(event);

    // Then
    then(handleComponent).should().updateHandle(givenPatchRequest());
    then(repository).should().updateLastChecked(List.of(equalId));
    then(repository).should(times(2)).createAnnotationRecordsHashed(anyList());
    then(rabbitMqPublisherService).should()
        .publishUpdateEvent(changedAnnotationOriginal, changedAnnotation);
    then(rabbitMqPublisherService).should()
        .publishCreateEvent(newAnnotation.withOdsBatchID(BATCH_ID));
    then(masJobRecordService).should()
        .markMasJobRecordAsComplete(JOB_ID, List.of(equalId, ID, ID), false);
    then(batchAnnotationService).should().applyBatchAnnotations(processedEvent);
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
    then(fdoRecordService).should().buildPatchHandleRequest(anyList());
    then(rabbitMqPublisherService).should()
        .publishUpdateEvent(any(Annotation.class), any(Annotation.class));
    then(masJobRecordService).should().markMasJobRecordAsComplete(JOB_ID, List.of(ID), false);
  }

  @Test
  void testUpdateMessageHandleDoesNotNeedUpdate() throws Exception {
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
    then(rabbitMqPublisherService).should()
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
    given(fdoRecordService.buildPatchHandleRequest(anyList())).willReturn(
        List.of(givenRollbackCreationRequest()));
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, false, null));
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");

    // When
    assertThatThrownBy(() -> service.handleMessage(givenAnnotationEvent(annotation))).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(rollbackService).should().rollbackUpdatedAnnotations(anySet(), eq(false), eq(true));
    then(masJobRecordService).should().markMasJobRecordAsFailed(eq(JOB_ID), eq(false), eq(ErrorCode.DISSCO_EXCEPTION), any());
    then(masJobRecordService).should().markMasJobRecordAsFailed(eq(JOB_ID), eq(false), eq(ErrorCode.DISSCO_EXCEPTION), any());
  }

  @Test
  void testUpdateMessagePartialElasticFailure() throws Exception {
    // Given
    var annotation = givenAnnotationRequest();
    var secondAnnotation = givenAnnotationRequest()
        .withId(ID_ALT)
        .withOaMotivatedBy("nature")
        .withOaHasTarget(givenRequestOaTarget("alt target"));
    var secondAnnotationCurrent = givenAnnotationProcessed()
        .withId(ID_ALT)
        .withDctermsIdentifier(ID_ALT)
        .withOaMotivatedBy("science")
        .withOaHasTarget(givenOaTarget("alt target"));
    var secondAnnotationCurrentHashed = new HashedAnnotation(secondAnnotationCurrent,
        ANNOTATION_HASH_2);
    var event = new AnnotationProcessingEvent(JOB_ID, List.of(annotation, secondAnnotation), null,
        null);
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH)
        .willReturn(ANNOTATION_HASH_2);
    given(repository.getAnnotationFromHash(any())).willReturn(
        List.of(givenHashedAnnotationAlt(), secondAnnotationCurrentHashed));
    givenBulkResponse();
    given(elasticRepository.indexAnnotations(any())).willReturn(bulkResponse);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(fdoRecordService.buildPatchHandleRequest(anyList())).willReturn(
        List.of(givenRollbackCreationRequest()));
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, false, null));
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    given(applicationProperties.getProcessorName()).willReturn(
        "annotation-processing-service");
    given(fdoProperties.getType()).willReturn(FDO_TYPE);

    // When
    assertThatThrownBy(() -> service.handleMessage(event)).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(rollbackService).should().rollbackUpdatedAnnotations(anySet(), eq(true), eq(true));
    then(rollbackService).should().rollbackUpdatedAnnotations(anySet(), eq(false), eq(true));
    then(masJobRecordService).should().markMasJobRecordAsFailed(eq(JOB_ID), eq(false), eq(ErrorCode.DISSCO_EXCEPTION), any());
    then(masJobRecordService).should().markMasJobRecordAsFailed(eq(JOB_ID), eq(false), eq(ErrorCode.DISSCO_EXCEPTION), any());
  }

  @Test
  void testHandleUpdateMessageKafkaException() throws Exception {
    // Given
    boolean batchingRequested = false;
    var annotation = givenAnnotationRequest().withId(ID);
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(any())).willReturn(List.of(givenHashedAnnotationAlt()));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(any())).willReturn(bulkResponse);
    doThrow(JsonProcessingException.class).when(rabbitMqPublisherService).publishUpdateEvent(any(
        Annotation.class), any(Annotation.class));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(fdoRecordService.buildPatchHandleRequest(anyList())).willReturn(
        List.of(givenRollbackCreationRequest()));
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, batchingRequested, null));
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");

    // When
    assertThatThrownBy(() -> service.handleMessage(givenAnnotationEvent(annotation))).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(handleComponent).should().updateHandle(anyList());
    then(rollbackService).should().rollbackUpdatedAnnotations(anySet(), eq(true), eq(true));
    then(masJobRecordService).should().markMasJobRecordAsFailed(eq(JOB_ID), eq(false), eq(ErrorCode.DISSCO_EXCEPTION), any());
  }

  @Test
  void testUpdateMessageKafkaExceptionHandleRollbackFailed() throws Exception {
    // Given
    boolean batchingRequested = false;
    var annotationRequest = givenAnnotationRequest().withId(ID);
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(any())).willReturn(List.of(givenHashedAnnotationAlt()));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(any())).willReturn(bulkResponse);
    doThrow(JsonProcessingException.class).when(rabbitMqPublisherService).publishUpdateEvent(any(
        Annotation.class), any(Annotation.class));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(fdoRecordService.buildPatchHandleRequest(anyList())).willReturn(
        List.of(givenRollbackCreationRequest()));
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, batchingRequested, null));

    // When
    assertThatThrownBy(
        () -> service.handleMessage(givenAnnotationEvent(annotationRequest))).isInstanceOf(
        FailedProcessingException.class);

    // Then
    then(masJobRecordService).should().markMasJobRecordAsFailed(eq(JOB_ID), eq(false), eq(ErrorCode.DISSCO_EXCEPTION), any());
    then(rollbackService).should().rollbackUpdatedAnnotations(anySet(), eq(true), eq(true));
  }

  @Test
  void testArchiveAnnotation() throws Exception {
    // Given
    var deleteResponse = mock(DeleteResponse.class);
    given(deleteResponse.result()).willReturn(Result.Deleted);
    given(elasticRepository.archiveAnnotation(ID)).willReturn(deleteResponse);
    var tombstonedAnnotation = givenTombstoneAnnotation();
    mockedStatic.when(Instant::now).thenReturn(UPDATED);
    mockedClock.when(Clock::systemUTC).thenReturn(updatedClock);

    // When
    service.archiveAnnotation(givenAnnotationProcessed(), givenProcessingAgent());

    // Then
    then(repository).should().archiveAnnotation(tombstonedAnnotation);
    then(fdoRecordService).should().buildTombstoneHandleRequest(BARE_ID);
    then(handleComponent).should().archiveHandle(any(), eq(BARE_ID));
    then(masJobRecordService).shouldHaveNoInteractions();
  }

  @Test
  void testArchiveAnnotationHandleFailed() throws Exception {
    // Given
    doThrow(PidCreationException.class).when(handleComponent).archiveHandle(any(), eq(BARE_ID));

    // When
    assertThrows(FailedProcessingException.class,
        () -> service.archiveAnnotation(givenAnnotationProcessed(), givenProcessingAgent()));

    // Then
    then(elasticRepository).shouldHaveNoInteractions();
    then(repository).shouldHaveNoMoreInteractions();
  }

  @Test
  void testArchiveAnnotationNotPermitted() {
    // Given

    // When
    assertThrows(MethodNotAllowedException.class,
        () -> service.archiveAnnotation(givenAnnotationProcessed().withOdsMergingDecisionStatus(
            OdsMergingDecisionStatus.APPROVED), givenProcessingAgent()));

    // Then
    then(elasticRepository).shouldHaveNoInteractions();
    then(repository).shouldHaveNoMoreInteractions();
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
    var batchingRequested = true;
    var annotationRequest = givenAnnotationRequest();
    var event = new AnnotationProcessingEvent(JOB_ID, List.of(annotationRequest),
        List.of(givenAnnotationBatchMetadataLatitudeSearch()), null);
    var batchMetadata = new ProcessedAnnotationBatch(
        List.of(givenAnnotationProcessed().withOdsBatchID(BATCH_ID)),
        JOB_ID, List.of(givenAnnotationBatchMetadataLatitudeSearch()), null);
    var mjr = new MasJobRecord(JOB_ID, batchingRequested, null);
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postHandlesHashed(any())).willReturn(Map.of(ANNOTATION_HASH, BARE_ID));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(anyList())).willReturn(bulkResponse);
    given(applicationProperties.getProcessorHandle()).willReturn(
        PROCESSOR_HANDLE);
    given(applicationProperties.getProcessorHandle()).willReturn(
        PROCESSOR_HANDLE);
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(mjr);
    given(annotationBatchRecordService.mintBatchIds(anyList(), eq(batchingRequested),
        eq(event))).willReturn(
        Optional.of(givenBatchIdMap()));
    given(applicationProperties.getProcessorName()).willReturn(
        "annotation-processing-service");
    given(fdoProperties.getType()).willReturn(FDO_TYPE);

    // When
    service.handleMessage(event);

    // Then
    then(repository).should()
        .createAnnotationRecordsHashed(List.of(givenHashedAnnotation(BATCH_ID)));
    then(rabbitMqPublisherService).should()
        .publishCreateEvent(givenAnnotationProcessed().withOdsBatchID(BATCH_ID));
    then(masJobRecordService).should().markMasJobRecordAsComplete(JOB_ID, List.of(ID), false);
    then(batchAnnotationService).should().applyBatchAnnotations(batchMetadata);
    then(annotationBatchRecordService).shouldHaveNoMoreInteractions();
  }

  @Test
  void testNewMessageBatchEnabledNoMetadata() throws Exception {
    // Given
    boolean batchingRequested = true;
    var annotationRequest = givenAnnotationRequest();
    var event = new AnnotationProcessingEvent(JOB_ID, List.of(annotationRequest), null, null);
    var mjr = new MasJobRecord(JOB_ID, batchingRequested, null);
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postHandlesHashed(any())).willReturn(Map.of(ANNOTATION_HASH, BARE_ID));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(anyList())).willReturn(bulkResponse);
    given(applicationProperties.getProcessorHandle()).willReturn(
        PROCESSOR_HANDLE);
    given(applicationProperties.getProcessorHandle()).willReturn(
        PROCESSOR_HANDLE);
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(mjr);
    given(annotationBatchRecordService.mintBatchIds(anyList(), eq(batchingRequested),
        eq(event))).willReturn(
        Optional.of(givenBatchIdMap()));
    given(applicationProperties.getProcessorName()).willReturn(
        "annotation-processing-service");
    given(fdoProperties.getType()).willReturn(FDO_TYPE);

    // When
    service.handleMessage(event);

    // Then
    then(repository).should()
        .createAnnotationRecordsHashed(List.of(givenHashedAnnotation(BATCH_ID)));
    then(rabbitMqPublisherService).should()
        .publishCreateEvent(givenAnnotationProcessed().withOdsBatchID(BATCH_ID));
    then(masJobRecordService).should().markMasJobRecordAsComplete(JOB_ID, List.of(ID), false);
    then(batchAnnotationService).shouldHaveNoInteractions();
    then(annotationBatchRecordService).shouldHaveNoMoreInteractions();
  }

  @Test
  void testNewMessageDataAccessException() throws Exception {
    // Given
    boolean batchingRequested = false;
    var annotationRequest = givenAnnotationRequest();
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postHandlesHashed(any())).willReturn(Map.of(ANNOTATION_HASH, ID));
    given(applicationProperties.getProcessorHandle()).willReturn(
        PROCESSOR_HANDLE);
    given(applicationProperties.getProcessorHandle()).willReturn(
        PROCESSOR_HANDLE);
    doThrow(DataAccessException.class).when(repository).createAnnotationRecordsHashed(anyList());
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, batchingRequested, null));
    given(annotationBatchRecordService.mintBatchIds(anyList(), eq(batchingRequested),
        any())).willReturn(Optional.empty());

    // When
    assertThrows(FailedProcessingException.class,
        () -> service.handleMessage(givenAnnotationEvent(annotationRequest)));

    // Then
    then(rollbackService).should().rollbackNewAnnotationsHash(anyList(), eq(false), eq(false), eq(Optional.empty()));
  }

  @Test
  void testUpdateMessageDataAccessException() throws Exception {
    // Given
    boolean batchingRequested = false;
    var annotationRequest = givenAnnotationRequest();
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(any())).willReturn(List.of(givenHashedAnnotationAlt()));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(fdoRecordService.buildPatchHandleRequest(anyList())).willReturn(
        List.of(givenRollbackCreationRequest()));
    doThrow(DataAccessException.class).when(repository).createAnnotationRecordsHashed(anyList());
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(
        new MasJobRecord(JOB_ID, batchingRequested, null));

    // When
    assertThrows(FailedProcessingException.class,
        () -> service.handleMessage(givenAnnotationEvent(annotationRequest)));

    // Then
    then(rollbackService).should().rollbackUpdatedAnnotations(anySet(), eq(false), eq(false));
  }

  @Test
  void testNewMessageResolveTimeout()
      throws Exception {
    // Given
    boolean batchingRequested = true;
    var annotationRequest = givenAnnotationRequest();
    var mjr = new MasJobRecord(JOB_ID, batchingRequested, ErrorCode.TIMEOUT);
    var event = givenAnnotationEvent(annotationRequest);
    given(annotationHasher.getAnnotationHash(any())).willReturn(ANNOTATION_HASH);
    given(repository.getAnnotationFromHash(Set.of(ANNOTATION_HASH))).willReturn(new ArrayList<>());
    given(handleComponent.postHandlesHashed(any())).willReturn(Map.of(ANNOTATION_HASH, BARE_ID));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexAnnotations(anyList())).willReturn(bulkResponse);
    given(applicationProperties.getProcessorHandle()).willReturn(
        PROCESSOR_HANDLE);
    given(applicationProperties.getProcessorHandle()).willReturn(
        PROCESSOR_HANDLE);
    given(masJobRecordService.getMasJobRecord(JOB_ID)).willReturn(mjr
    );
    given(annotationBatchRecordService.mintBatchIds(anyList(), eq(batchingRequested),
        eq(event))).willReturn(
        Optional.of(givenBatchIdMap()));
    given(applicationProperties.getProcessorName()).willReturn(
        "annotation-processing-service");
    given(fdoProperties.getType()).willReturn(FDO_TYPE);

    // When
    service.handleMessage(event);

    // Then
    then(repository).should()
        .createAnnotationRecordsHashed(List.of(givenHashedAnnotation(BATCH_ID)));
    then(rabbitMqPublisherService).should()
        .publishCreateEvent(givenAnnotationProcessed().withOdsBatchID(BATCH_ID));
    then(masJobRecordService).should().markMasJobRecordAsComplete(JOB_ID, List.of(ID), false);
    then(annotationBatchRecordService).shouldHaveNoMoreInteractions();
  }
}

