package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.BATCH_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.CREATED;
import static eu.dissco.annotationprocessingservice.TestUtils.CREATOR;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.JOB_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessedWeb;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRequest;
import static eu.dissco.annotationprocessingservice.TestUtils.givenBatchIdMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mockStatic;

import eu.dissco.annotationprocessingservice.domain.AnnotationBatchRecord;
import eu.dissco.annotationprocessingservice.repository.AnnotationBatchRecordRepository;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingEvent;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AnnotationBatchRecordServiceTest {

  @Mock
  private AnnotationBatchRecordRepository repository;
  private AnnotationBatchRecordService service;
  private MockedStatic<Clock> mockedClock;


  @BeforeEach
  void setup() {
    service = new AnnotationBatchRecordService(repository);
  }

  private MockedStatic<Instant> mockedStatic;

  @Test
  void testMintBatchIdsBatchingRequested() {
    var batchId = BATCH_ID; // Not redundant, we need to declare this outside our mock block
    try (MockedStatic<UUID> mockedUuid = mockStatic(UUID.class)) {
      // Given
      mockedUuid.when(UUID::randomUUID).thenReturn(batchId);
      var annotations = List.of(givenAnnotationRequest());

      // When
      var result = service.mintBatchIds(List.of(givenAnnotationProcessed()), true,
          new AnnotationProcessingEvent(JOB_ID, annotations, null, null));

      // Then
      assertThat(result).contains(givenBatchIdMap());
    }
  }

  @Test
  void TestMintBatchId() {
    // Given
    var batchId = BATCH_ID; // Not redundant, we need to declare this outside our mock block
    var expectedAnnotationRecord = new AnnotationBatchRecord(
        batchId,
        CREATOR,
        ID,
        CREATED,
        null
    );
    try (MockedStatic<UUID> mockedUuid = mockStatic(UUID.class)) {
      initTime();
      mockedUuid.when(UUID::randomUUID).thenReturn(batchId);

      // When
      service.mintBatchId(givenAnnotationProcessedWeb());

      // Then
      then(repository).should().createAnnotationBatchRecord(List.of(expectedAnnotationRecord));
    }
    mockedStatic.close();
    mockedClock.close();
  }

  @Test
  void testMintBatchIdsBatchingNotRequested() {
    var annotations = List.of(givenAnnotationProcessed());

    // When
    var result = service.mintBatchIds(annotations, false,
        new AnnotationProcessingEvent(JOB_ID, List.of(givenAnnotationRequest()), null, null));

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  void testMintBatchIdsIsBatchResult() {
    var annotations = List.of(givenAnnotationProcessed());
    var expected = Map.of(ID, BATCH_ID);

    // When
    var result = service.mintBatchIds(annotations, true,
        new AnnotationProcessingEvent(JOB_ID, List.of(givenAnnotationRequest()), null, BATCH_ID));

    // Then
    assertThat(result).contains(expected);
  }

  @Test
  void testUpdateAnnotationBatchRecord() {
    // When
    service.updateAnnotationBatchRecord(BATCH_ID, 1L);

    // Then
    then(repository).should().updateAnnotationBatchRecord(BATCH_ID, 1L);
  }

  @Test
  void testRollbackAnnotationBatchRecord() {
    // When
    service.rollbackAnnotationBatchRecord(Optional.of(givenBatchIdMap()));

    // Then
    then(repository).should().rollbackAnnotationBatchRecord(Set.of(BATCH_ID));
  }

  @Test
  void testRollbackAnnotationBatchRecordNoInteractions() {
    // When
    service.rollbackAnnotationBatchRecord(Optional.empty());

    // Then
    then(repository).shouldHaveNoInteractions();
  }

  private void initTime() {
    Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
    mockedClock = mockStatic(Clock.class);
    mockedClock.when(Clock::systemUTC).thenReturn(clock);
    Instant instant = Instant.now(clock);
    mockedStatic = mockStatic(Instant.class);
    mockedStatic.when(Instant::now).thenReturn(instant);
    mockedStatic.when(() -> Instant.from(any())).thenReturn(instant);
  }

}
