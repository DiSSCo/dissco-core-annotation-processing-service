package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.BARE_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.BATCH_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.CREATED;
import static eu.dissco.annotationprocessingservice.TestUtils.CREATOR;
import static eu.dissco.annotationprocessingservice.TestUtils.FDO_TYPE;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationBatchMetadataTwoParam;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessedAlt;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessedWeb;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessedWebBatch;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRequest;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.annotationprocessingservice.component.AnnotationHasher;
import eu.dissco.annotationprocessingservice.component.AnnotationValidatorComponent;
import eu.dissco.annotationprocessingservice.domain.ProcessedAnnotationBatch;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.NotFoundException;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.properties.FdoProperties;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.schema.Annotation;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingEvent;
import eu.dissco.annotationprocessingservice.web.HandleComponent;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import org.jooq.exception.DataAccessException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ProcessingWebServiceTest {

  private final Instant instant = Instant.now(Clock.fixed(CREATED, ZoneOffset.UTC));
  Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
  MockedStatic<Clock> mockedClock = mockStatic(Clock.class);
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
  private ApplicationProperties applicationProperties;
  @Mock
  private AnnotationValidatorComponent schemaValidator;
  @Mock
  private MasJobRecordService masJobRecordService;
  @Mock
  private BatchAnnotationService batchAnnotationService;
  @Mock
  private AnnotationBatchRecordService annotationBatchRecordService;
  @Mock
  private FdoProperties fdoProperties;
  @Mock
  RollbackService rollbackService;
  @Mock
  private AnnotationHasher annotationHasher;
  private MockedStatic<Instant> mockedStatic;
  private ProcessingWebService service;

  @BeforeEach
  void setup() {
    service = new ProcessingWebService(repository, elasticRepository,
        rabbitMqPublisherService, fdoRecordService, handleComponent, applicationProperties,
        schemaValidator, masJobRecordService, batchAnnotationService, annotationBatchRecordService,
        fdoProperties, rollbackService, annotationHasher);
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
  void testCreateAnnotation() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest();
    given(handleComponent.postHandle(any())).willReturn(BARE_ID);
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.Created);
    given(elasticRepository.indexAnnotation(any(Annotation.class))).willReturn(indexResponse);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    given(applicationProperties.getProcessorName()).willReturn(
        "annotation-processing-service");
    given(fdoProperties.getType()).willReturn(FDO_TYPE);

    // When
    var result = service.persistNewAnnotation(annotationRequest, false);

    // Then
    assertThat(result).isNotNull().isInstanceOf(Annotation.class);
    assertThat(result.getId()).isEqualTo(ID);
    then(repository).should().createAnnotationRecord(givenAnnotationProcessedWeb());
    then(rabbitMqPublisherService).should().publishCreateEvent(any(Annotation.class));
  }

  @Test
  void testCreateAnnotationBatch() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest();
    given(handleComponent.postHandle(any())).willReturn(BARE_ID);
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.Created);
    given(elasticRepository.indexAnnotation(any(Annotation.class))).willReturn(indexResponse);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    doAnswer(invocation -> {
      var args = invocation.getArguments();
      ((Annotation) args[0]).withOdsBatchID(BATCH_ID);
      return null;
    }).when(annotationBatchRecordService).mintBatchId(any());
    given(applicationProperties.getProcessorName()).willReturn(
        "annotation-processing-service");
    given(fdoProperties.getType()).willReturn(FDO_TYPE);

    // When
    var result = service.persistNewAnnotation(annotationRequest, true);

    // Then
    assertThat(result).isNotNull().isInstanceOf(Annotation.class);
    assertThat(result.getId()).isEqualTo(ID);
    then(repository).should().createAnnotationRecord(givenAnnotationProcessedWebBatch());
    then(rabbitMqPublisherService).should().publishCreateEvent(any(Annotation.class));
  }

  @Test
  void batchWebAnnotations() throws Exception {
    // Given
    var processedAnnotation = givenAnnotationProcessedWebBatch();
    var requestEvent = new AnnotationProcessingEvent(null, List.of(givenAnnotationRequest()),
        List.of(givenAnnotationBatchMetadataTwoParam()), null);
    var processedEvent = new ProcessedAnnotationBatch(List.of(givenAnnotationProcessedWebBatch()),
        null,
        List.of(givenAnnotationBatchMetadataTwoParam()), null);

    // When
    service.batchWebAnnotations(requestEvent, processedAnnotation);

    // Then
    then(batchAnnotationService).should()
        .applyBatchAnnotations(processedEvent);
  }

  @Test
  void testCreateAnnotationElasticIOException() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest();
    given(handleComponent.postHandle(any())).willReturn(BARE_ID);
    doThrow(IOException.class).when(elasticRepository).indexAnnotation(any(Annotation.class));
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    given(applicationProperties.getProcessorName()).willReturn(
        "annotation-processing-service");
    given(fdoProperties.getType()).willReturn(FDO_TYPE);

    // When
    assertThrows(FailedProcessingException.class,
        () -> service.persistNewAnnotation(annotationRequest, false));

    // Then
    then(rollbackService).should().rollbackNewAnnotations(anyList(), eq(false), eq(true));
  }

  @Test
  void testCreateAnnotationElasticFailure() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest();
    given(handleComponent.postHandle(any())).willReturn(BARE_ID);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.NotFound);
    given(elasticRepository.indexAnnotation(givenAnnotationProcessedWeb())).willReturn(
        indexResponse);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    given(applicationProperties.getProcessorName()).willReturn(
        "annotation-processing-service");
    given(fdoProperties.getType()).willReturn(FDO_TYPE);

    // When
    assertThrows(FailedProcessingException.class,
        () -> service.persistNewAnnotation(annotationRequest, false));

    // Then
    then(rollbackService).should().rollbackNewAnnotations(anyList(), eq(false), eq(true));
  }

  @Test
  void testCreateAnnotationKafkaFailure() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest();
    given(handleComponent.postHandle(any())).willReturn(BARE_ID);
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.Created);
    given(elasticRepository.indexAnnotation(any(Annotation.class))).willReturn(indexResponse);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    doThrow(JsonProcessingException.class).when(rabbitMqPublisherService)
        .publishCreateEvent(givenAnnotationProcessedWeb());
    given(applicationProperties.getProcessorName()).willReturn(
        "annotation-processing-service");
    given(fdoProperties.getType()).willReturn(FDO_TYPE);

    // When
    assertThrows(FailedProcessingException.class,
        () -> service.persistNewAnnotation(annotationRequest, false));

    // Then
    then(rollbackService).should().rollbackNewAnnotations(anyList(), eq(true), eq(true));
  }

  @Test
  void testCreateAnnotationPidFailure() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest();
    doThrow(PidCreationException.class).when(handleComponent).postHandle(any());

    // When
    assertThrows(FailedProcessingException.class,
        () -> service.persistNewAnnotation(annotationRequest, false));

    // Then
    then(repository).shouldHaveNoInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
    then(rabbitMqPublisherService).shouldHaveNoInteractions();
  }

  @Test
  void testUpdateAnnotation() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest().withId(BARE_ID);
    var annotation = givenAnnotationProcessed().withOdsVersion(2).withOdsJobID(null);
    given(repository.getAnnotationForUser(BARE_ID, CREATOR)).willReturn(
        Optional.of(givenAnnotationProcessedAlt()));
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.Updated);
    given(elasticRepository.indexAnnotation(any(Annotation.class))).willReturn(indexResponse);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    given(applicationProperties.getProcessorName()).willReturn(
        "annotation-processing-service");
    given(fdoProperties.getType()).willReturn(FDO_TYPE);

    // When
    var result = service.updateAnnotation(annotationRequest);

    // Then
    assertThat(result).isEqualTo(givenAnnotationProcessedWeb().withOdsVersion(2));
    assertThat(result.getId()).isEqualTo(ID);
    then(fdoRecordService).should()
        .buildPatchHandleRequest(annotation);
    then(handleComponent).should().updateHandle(any());
    then(rabbitMqPublisherService).should()
        .publishUpdateEvent(givenAnnotationProcessedAlt(),
            givenAnnotationProcessedWeb().withOdsVersion(2));
  }

  @Test
  void testUpdateEqualAnnotation() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest().withId(ID);
    var currentResult = givenAnnotationProcessedWeb();
    given(repository.getAnnotationForUser(ID, CREATOR)).willReturn(
        Optional.of(currentResult));

    // When
    var result = service.updateAnnotation(annotationRequest);

    // Then
    assertThat(result).isEqualTo(currentResult);
    then(repository).should().updateLastChecked(List.of(ID));
    then(fdoRecordService).shouldHaveNoInteractions();
    then(handleComponent).shouldHaveNoInteractions();
    then(rabbitMqPublisherService).shouldHaveNoInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
  }

  @Test
  void testUpdateAnnotationNotFound() {
    // Given
    var annotationRequest = givenAnnotationRequest().withId(ID);
    given(repository.getAnnotationForUser(ID, CREATOR)).willReturn(Optional.empty());

    // Then
    assertThrows(NotFoundException.class, () -> service.updateAnnotation(annotationRequest));
  }

  @Test
  void testUpdateAnnotationPidException() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest().withId(ID);
    given(repository.getAnnotationForUser(ID, CREATOR)).willReturn(
        Optional.of(givenAnnotationProcessedAlt()));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    doThrow(PidCreationException.class).when(handleComponent).updateHandle(any());

    // When
    assertThrows(FailedProcessingException.class,
        () -> service.updateAnnotation(annotationRequest));

    // Then
    then(repository).shouldHaveNoMoreInteractions();
    then(rabbitMqPublisherService).shouldHaveNoInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
  }

  @Test
  void testUpdateAnnotationElasticIOException() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest().withId(ID);
    given(repository.getAnnotationForUser(ID, CREATOR)).willReturn(
        Optional.of(givenAnnotationProcessedAlt()));
    doThrow(IOException.class).when(elasticRepository).indexAnnotation(any(Annotation.class));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    assertThrows(FailedProcessingException.class,
        () -> service.updateAnnotation(annotationRequest));

    // Then
    then(rollbackService).should().rollbackUpdatedAnnotation(any(), any(), eq(false), eq(true));
    then(rabbitMqPublisherService).shouldHaveNoInteractions();
  }

  @Test
  void testUpdateAnnotationElasticFailure() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest().withId(ID);
    given(repository.getAnnotationForUser(ID, CREATOR)).willReturn(
        Optional.of(givenAnnotationProcessedAlt()));
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.NotFound);
    given(elasticRepository.indexAnnotation(any())).willReturn(indexResponse);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    assertThrows(FailedProcessingException.class,
        () -> service.updateAnnotation(annotationRequest));

    // Then
    then(rollbackService).should().rollbackUpdatedAnnotation(any(), any(), eq(false), eq(true));

  }

  @Test
  void testUpdateAnnotationKafkaFailure() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest().withId(BARE_ID);
    given(repository.getAnnotationForUser(BARE_ID, CREATOR)).willReturn(
        Optional.of(givenAnnotationProcessedAlt()));
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.Updated);
    given(elasticRepository.indexAnnotation(any())).willReturn(indexResponse);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    doThrow(JsonProcessingException.class).when(rabbitMqPublisherService)
        .publishUpdateEvent(givenAnnotationProcessedAlt(),
            givenAnnotationProcessedWeb().withOdsVersion(2));
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    given(applicationProperties.getProcessorName()).willReturn(
        "annotation-processing-service");
    given(fdoProperties.getType()).willReturn(FDO_TYPE);

    // When
    assertThrows(FailedProcessingException.class,
        () -> service.updateAnnotation(annotationRequest));

    // Then
    then(rollbackService).should().rollbackUpdatedAnnotation(any(), any(), eq(true), eq(true));

  }

  @Test
  void testDataAccessExceptionNewAnnotation() throws Exception {
    var annotationRequest = givenAnnotationRequest();
    given(handleComponent.postHandle(any())).willReturn(ID);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    doThrow(DataAccessException.class).when(repository)
        .createAnnotationRecord(any(Annotation.class));

    // When
    assertThrows(FailedProcessingException.class,
        () -> service.persistNewAnnotation(annotationRequest,
            false));

    // Then
    then(rollbackService).should().rollbackNewAnnotations(anyList(), eq(false), eq(false));
  }

  @Test
  void testDataAccessExceptionUpdateAnnotation() {
    var annotationRequest = givenAnnotationRequest().withId(ID);
    given(repository.getAnnotationForUser(ID, CREATOR)).willReturn(
        Optional.of(givenAnnotationProcessedAlt()));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    doThrow(DataAccessException.class).when(repository)
        .createAnnotationRecord(any(Annotation.class));

    // When
    assertThrows(FailedProcessingException.class,
        () -> service.updateAnnotation(annotationRequest));

    // Then
    then(rollbackService).should().rollbackUpdatedAnnotation(any(), any(), eq(false), eq(false));
  }

}
