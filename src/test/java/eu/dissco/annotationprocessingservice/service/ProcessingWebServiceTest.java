package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.CREATED;
import static eu.dissco.annotationprocessingservice.TestUtils.CREATOR;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessedAlt;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRequest;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.annotationprocessingservice.component.SchemaValidatorComponent;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.NotFoundException;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.web.HandleComponent;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ProcessingWebServiceTest {

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
  private SchemaValidatorComponent schemaValidator;
  @Mock
  private MasJobRecordService masJobRecordService;
  @Mock
  private BatchAnnotationService batchAnnotationService;
  private MockedStatic<Instant> mockedStatic;
  private final Instant instant = Instant.now(Clock.fixed(CREATED, ZoneOffset.UTC));
  private ProcessingWebService service;
  Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
  MockedStatic<Clock> mockedClock = mockStatic(Clock.class);

  @BeforeEach
  void setup() {
    service = new ProcessingWebService(repository, elasticRepository,
        kafkaPublisherService, fdoRecordService, handleComponent, applicationProperties,
        schemaValidator, masJobRecordService, batchAnnotationService);
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
    given(handleComponent.postHandle(any())).willReturn(List.of(ID));
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.Created);
    given(elasticRepository.indexAnnotation(any(Annotation.class))).willReturn(indexResponse);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");

    // When
    var result = service.persistNewAnnotation(annotationRequest);

    // Then
    assertThat(result).isNotNull().isInstanceOf(Annotation.class);
    assertThat(result.getOdsId()).isEqualTo(ID);
    then(repository).should().createAnnotationRecord(givenAnnotationProcessed());
    then(kafkaPublisherService).should().publishCreateEvent(any(Annotation.class));
  }

  @Test
  void testCreateAnnotationElasticIOException() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest();
    given(handleComponent.postHandle(any())).willReturn(List.of(ID));
    doThrow(IOException.class).when(elasticRepository).indexAnnotation(any(Annotation.class));
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");

    // When
    assertThrows(FailedProcessingException.class,
        () -> service.persistNewAnnotation(annotationRequest));

    // Then
    then(repository).should().rollbackAnnotation(ID);
    then(fdoRecordService).should().buildRollbackCreationRequest(givenAnnotationProcessed());
    then(handleComponent).should().rollbackHandleCreation(any());
    then(kafkaPublisherService).shouldHaveNoInteractions();
  }

  @Test
  void testCreateAnnotationElasticFailure() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest();
    given(handleComponent.postHandle(any())).willReturn(List.of(ID));
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.NotFound);
    given(elasticRepository.indexAnnotation(givenAnnotationProcessed())).willReturn(indexResponse);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");

    // When
    assertThrows(FailedProcessingException.class,
        () -> service.persistNewAnnotation(annotationRequest));

    // Then
    then(repository).should().rollbackAnnotation(ID);
    then(fdoRecordService).should().buildRollbackCreationRequest(givenAnnotationProcessed());
    then(handleComponent).should().rollbackHandleCreation(any());
    then(kafkaPublisherService).shouldHaveNoInteractions();
  }

  @Test
  void testCreateAnnotationElasticFailureNoArchive() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest();
    given(handleComponent.postHandle(any())).willReturn(List.of(ID));
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.NotFound);
    given(elasticRepository.indexAnnotation(givenAnnotationProcessed())).willReturn(indexResponse);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");

    // When
    assertThrows(FailedProcessingException.class,
        () -> service.persistNewAnnotation(annotationRequest));

    // Then
    then(repository).should().rollbackAnnotation(ID);
    then(fdoRecordService).should().buildRollbackCreationRequest(givenAnnotationProcessed());
    then(handleComponent).should().rollbackHandleCreation(any());
    then(kafkaPublisherService).shouldHaveNoInteractions();
  }

  @Test
  void testCreateAnnotationKafkaFailure() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest();
    given(handleComponent.postHandle(any())).willReturn(List.of(ID));
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.Created);
    given(elasticRepository.indexAnnotation(any(Annotation.class))).willReturn(indexResponse);
    given(applicationProperties.getProcessorHandle()).willReturn(
        "https://hdl.handle.net/anno-process-service-pid");
    doThrow(JsonProcessingException.class).when(kafkaPublisherService)
        .publishCreateEvent(givenAnnotationProcessed());

    // When
    assertThrows(FailedProcessingException.class,
        () -> service.persistNewAnnotation(annotationRequest));

    // Then
    then(repository).should().rollbackAnnotation(ID);
    then(elasticRepository).should().archiveAnnotation(ID);
    then(fdoRecordService).should().buildRollbackCreationRequest(givenAnnotationProcessed());
    then(handleComponent).should().rollbackHandleCreation(any());
    then(kafkaPublisherService).shouldHaveNoMoreInteractions();
  }

  @Test
  void testCreateAnnotationPidFailure() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest();
    doThrow(PidCreationException.class).when(handleComponent).postHandle(any());

    // When
    assertThrows(FailedProcessingException.class,
        () -> service.persistNewAnnotation(annotationRequest));

    // Then
    then(repository).shouldHaveNoInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
    then(kafkaPublisherService).shouldHaveNoInteractions();
  }

  @Test
  void testUpdateAnnotation() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest().withOdsId(ID);
    given(repository.getAnnotationForUser(ID, CREATOR)).willReturn(
        Optional.of(givenAnnotationProcessedAlt()));
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.Updated);
    given(elasticRepository.indexAnnotation(any(Annotation.class))).willReturn(indexResponse);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    var result = service.updateAnnotation(annotationRequest);

    // Then
    assertThat(result).isEqualTo(givenAnnotationProcessed().withOdsVersion(2));
    assertThat(result.getOdsId()).isEqualTo(ID);
    then(fdoRecordService).should().buildPatchRollbackHandleRequest(annotationRequest);
    then(handleComponent).should().updateHandle(any());
    then(kafkaPublisherService).should()
        .publishUpdateEvent(givenAnnotationProcessedAlt(),
            givenAnnotationProcessed().withOdsVersion(2));
  }

  @Test
  void testUpdateEqualAnnotation() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest().withOdsId(ID);
    var currentResult = givenAnnotationProcessed();
    given(repository.getAnnotationForUser(ID, CREATOR)).willReturn(
        Optional.of(currentResult));

    // When
    var result = service.updateAnnotation(annotationRequest);

    // Then
    assertThat(result).isEqualTo(currentResult);
    then(repository).should().updateLastChecked(List.of(ID));
    then(fdoRecordService).shouldHaveNoInteractions();
    then(handleComponent).shouldHaveNoInteractions();
    then(kafkaPublisherService).shouldHaveNoInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
  }

  @Test
  void testUpdateAnnotationNotFound() {
    // Given
    var annotationRequest = givenAnnotationRequest().withOdsId(ID);
    given(repository.getAnnotationForUser(ID, CREATOR)).willReturn(Optional.empty());

    // Then
    assertThrows(NotFoundException.class, () -> service.updateAnnotation(annotationRequest));
  }

  @Test
  void testUpdateAnnotationPidException() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest().withOdsId(ID);
    given(repository.getAnnotationForUser(ID, CREATOR)).willReturn(
        Optional.of(givenAnnotationProcessedAlt()));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    doThrow(PidCreationException.class).when(handleComponent).updateHandle(any());

    // When
    assertThrows(FailedProcessingException.class,
        () -> service.updateAnnotation(annotationRequest));

    // Then
    then(repository).shouldHaveNoMoreInteractions();
    then(kafkaPublisherService).shouldHaveNoInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
  }

  @Test
  void testUpdateAnnotationElasticIOException() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest().withOdsId(ID);
    given(repository.getAnnotationForUser(ID, CREATOR)).willReturn(
        Optional.of(givenAnnotationProcessedAlt()));
    doThrow(IOException.class).when(elasticRepository).indexAnnotation(any(Annotation.class));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    assertThrows(FailedProcessingException.class,
        () -> service.updateAnnotation(annotationRequest));

    // Then
    then(fdoRecordService).should(times(2)).buildPatchRollbackHandleRequest(any(Annotation.class));
    then(handleComponent).should().updateHandle(any());
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(repository).should().createAnnotationRecord(givenAnnotationProcessedAlt());
    then(kafkaPublisherService).shouldHaveNoInteractions();
  }

  @Test
  void testUpdateAnnotationElasticFailure() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest().withOdsId(ID);
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
    then(fdoRecordService).should(times(2)).buildPatchRollbackHandleRequest(any(Annotation.class));
    then(handleComponent).should().updateHandle(any());
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(repository).should().createAnnotationRecord(givenAnnotationProcessedAlt());
    then(kafkaPublisherService).shouldHaveNoInteractions();
  }

  @Test
  void testUpdateAnnotationKafkaFailure() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest().withOdsId(ID);
    given(repository.getAnnotationForUser(ID, CREATOR)).willReturn(
        Optional.of(givenAnnotationProcessedAlt()));
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.Updated);
    given(elasticRepository.indexAnnotation(any())).willReturn(indexResponse);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    doThrow(JsonProcessingException.class).when(kafkaPublisherService)
        .publishUpdateEvent(givenAnnotationProcessedAlt(),
            givenAnnotationProcessed().withOdsVersion(2));

    // When
    assertThrows(FailedProcessingException.class,
        () -> service.updateAnnotation(annotationRequest));

    // Then
    then(fdoRecordService).should(times(2)).buildPatchRollbackHandleRequest(any(Annotation.class));
    then(handleComponent).should().updateHandle(any());
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(elasticRepository).should().indexAnnotation(givenAnnotationProcessedAlt());
    then(repository).should().createAnnotationRecord(givenAnnotationProcessedAlt());
  }


}
