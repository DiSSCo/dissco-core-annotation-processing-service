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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.exception.ForbiddenException;
import eu.dissco.annotationprocessingservice.exception.NotFoundException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.web.HandleComponent;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
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
  private MockedStatic<Instant> mockedStatic;
  private final Instant instant = Instant.now(Clock.fixed(CREATED, ZoneOffset.UTC));
  private ProcessingWebService service;
  Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
  MockedStatic<Clock> mockedClock = mockStatic(Clock.class);

  @BeforeEach
  void setup() {
    service = new ProcessingWebService(repository, elasticRepository,
        kafkaPublisherService, fdoRecordService, handleComponent, applicationProperties);
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
    given(handleComponent.postHandle(any())).willReturn(ID);
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.Created);
    given(elasticRepository.indexAnnotation(any(Annotation.class))).willReturn(indexResponse);

    // When
    var result = service.persistNewAnnotation(annotationRequest);

    // Then
    assertThat(result).isNotNull().isInstanceOf(Annotation.class);
    assertThat(result.getOdsId()).isEqualTo(ID);
    then(kafkaPublisherService).should().publishCreateEvent(any(Annotation.class));
  }

  @Test
  void testUpdateAnnotation() throws Exception {
    // Given
    var annotationRequest = givenAnnotationRequest().withOdsId(ID);
    given(repository.getAnnotationForUser(ID, CREATOR)).willReturn(Optional.of(givenAnnotationProcessedAlt()));
    var indexResponse = mock(IndexResponse.class);
    given(indexResponse.result()).willReturn(Result.Updated);
    given(elasticRepository.indexAnnotation(any(Annotation.class))).willReturn(indexResponse);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    var result = service.updateAnnotation(annotationRequest);

    // Then
    assertThat(result).isEqualTo(givenAnnotationProcessed().withOdsVersion(2));
    assertThat(result.getOdsId()).isEqualTo(ID);
    then(fdoRecordService).should()
        .buildPatchRollbackHandleRequest(annotationRequest, ID);
    then(handleComponent).should().updateHandle(any());
    then(kafkaPublisherService).should()
        .publishUpdateEvent(any(Annotation.class), any(Annotation.class));
  }

  @Test
  void testUpdateAnnotationNotFound() {
    // Given
    var annotationRequest = givenAnnotationRequest().withOdsId(ID);
    given(repository.getAnnotationForUser(ID, CREATOR)).willReturn(Optional.empty());

    // then
    assertThrows(ForbiddenException.class, () -> service.updateAnnotation(annotationRequest));
  }




}
