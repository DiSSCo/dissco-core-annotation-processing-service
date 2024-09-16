package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.BARE_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.CREATED;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAcceptedAnnotation;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAutoAcceptedRequest;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.annotationprocessingservice.component.AnnotationValidatorComponent;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.schema.Annotation;
import eu.dissco.annotationprocessingservice.web.HandleComponent;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.jooq.exception.DataAccessException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ProcessingAutoAcceptedServiceTest {

  private final Instant instant = Instant.now(Clock.fixed(CREATED, ZoneOffset.UTC));
  Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
  MockedStatic<Clock> mockedClock = mockStatic(Clock.class);
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
  private ApplicationProperties applicationProperties;
  @Mock
  private AnnotationValidatorComponent schemaValidator;
  @Mock
  private MasJobRecordService masJobRecordService;
  @Mock
  private BatchAnnotationService batchAnnotationService;
  @Mock
  private AnnotationBatchRecordService annotationBatchRecordService;
  private MockedStatic<Instant> mockedStatic;
  private ProcessingAutoAcceptedService service;

  @BeforeEach
  void setup() {
    service = new ProcessingAutoAcceptedService(repository, elasticRepository,
        kafkaPublisherService, fdoRecordService, handleComponent, applicationProperties,
        schemaValidator, masJobRecordService, batchAnnotationService, annotationBatchRecordService);
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
    var annotationRequest = givenAutoAcceptedRequest();
    given(handleComponent.postHandle(any())).willReturn(List.of(BARE_ID));
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.Created);
    given(elasticRepository.indexAnnotation(any(Annotation.class))).willReturn(indexResponse);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");

    // When
    service.handleMessage(annotationRequest);

    // Then
    then(repository).should().createAnnotationRecord(givenAcceptedAnnotation());
    then(kafkaPublisherService).should().publishCreateEvent(any(Annotation.class));
  }

  @Test
  void testDataAccessExceptionNewAnnotation() throws Exception {
    var annotationRequest = givenAutoAcceptedRequest();
    given(handleComponent.postHandle(any())).willReturn(List.of(ID));
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    doThrow(DataAccessException.class).when(repository)
        .createAnnotationRecord(any(Annotation.class));

    // When / Then
    assertThrows(FailedProcessingException.class,
        () -> service.handleMessage(annotationRequest));
    then(handleComponent).should().rollbackHandleCreation(any());
  }

  @Test
  void testCreateAnnotationElasticIOException() throws Exception {
    // Given
    var annotationRequest = givenAutoAcceptedRequest();
    given(handleComponent.postHandle(any())).willReturn(List.of(BARE_ID));
    doThrow(IOException.class).when(elasticRepository).indexAnnotation(any(Annotation.class));
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");

    // When
    assertThrows(FailedProcessingException.class,
        () -> service.handleMessage(annotationRequest));

    // Then
    then(repository).should().rollbackAnnotation(ID);
    then(fdoRecordService).should().buildRollbackCreationRequest(givenAcceptedAnnotation());
    then(handleComponent).should().rollbackHandleCreation(any());
    then(kafkaPublisherService).shouldHaveNoInteractions();
  }

  @Test
  void testCreateAnnotationElasticFailure() throws Exception {
    // Given
    var annotationRequest = givenAutoAcceptedRequest();
    given(handleComponent.postHandle(any())).willReturn(List.of(BARE_ID));
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.NotFound);
    given(elasticRepository.indexAnnotation(givenAcceptedAnnotation())).willReturn(
        indexResponse);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");

    // When
    assertThrows(FailedProcessingException.class, () -> service.handleMessage(annotationRequest));

    // Then
    then(repository).should().rollbackAnnotation(ID);
    then(fdoRecordService).should().buildRollbackCreationRequest(givenAcceptedAnnotation());
    then(handleComponent).should().rollbackHandleCreation(any());
    then(kafkaPublisherService).shouldHaveNoInteractions();
  }

  @Test
  void testCreateAnnotationElasticFailureNoArchive() throws Exception {
    // Given
    var annotationRequest = givenAutoAcceptedRequest();
    given(handleComponent.postHandle(any())).willReturn(List.of(BARE_ID));
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.NotFound);
    given(elasticRepository.indexAnnotation(givenAcceptedAnnotation())).willReturn(
        indexResponse);
    doThrow(PidCreationException.class).when(handleComponent).rollbackHandleCreation(any());
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");

    // When
    assertThrows(FailedProcessingException.class, () -> service.handleMessage(annotationRequest));

    // Then
    then(repository).should().rollbackAnnotation(ID);
    then(fdoRecordService).should().buildRollbackCreationRequest(givenAcceptedAnnotation());
    then(handleComponent).should().rollbackHandleCreation(any());
    then(kafkaPublisherService).shouldHaveNoInteractions();
  }

  @Test
  void testCreateAnnotationKafkaFailure() throws Exception {
    // Given
    var annotationRequest = givenAutoAcceptedRequest();
    given(handleComponent.postHandle(any())).willReturn(List.of(BARE_ID));
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.Created);
    given(elasticRepository.indexAnnotation(any(Annotation.class))).willReturn(indexResponse);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    doThrow(JsonProcessingException.class).when(kafkaPublisherService)
        .publishCreateEvent(givenAcceptedAnnotation());

    // When
    assertThrows(FailedProcessingException.class, () -> service.handleMessage(annotationRequest));

    // Then
    then(repository).should().rollbackAnnotation(ID);
    then(elasticRepository).should().archiveAnnotation(ID);
    then(fdoRecordService).should().buildRollbackCreationRequest(givenAcceptedAnnotation());
    then(handleComponent).should().rollbackHandleCreation(any());
    then(kafkaPublisherService).shouldHaveNoMoreInteractions();
  }

  @Test
  void testCreateAnnotationPidFailure() throws Exception {
    // Given
    var annotationRequest = givenAutoAcceptedRequest();
    doThrow(PidCreationException.class).when(handleComponent).postHandle(any());

    // When
    assertThrows(FailedProcessingException.class, () -> service.handleMessage(annotationRequest));

    // Then
    then(repository).shouldHaveNoInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
    then(kafkaPublisherService).shouldHaveNoInteractions();
  }

}
