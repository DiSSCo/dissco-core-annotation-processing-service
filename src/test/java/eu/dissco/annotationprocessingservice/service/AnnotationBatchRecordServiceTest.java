package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.BATCH_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.JOB_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenBatchIdMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mockStatic;

import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.repository.AnnotationBatchRecordRepository;
import java.util.List;
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
  AnnotationBatchRecordRepository repository;
  AnnotationBatchRecordService service;

  @BeforeEach
  void setup() {
    service = new AnnotationBatchRecordService(repository);
  }

  @Test
  void testMintBatchIdsBatchingRequested() {
    var batchId = BATCH_ID; // Not redundant, we need to declare this outside our mock block
    try (MockedStatic<UUID> mockedStatic = mockStatic(UUID.class)) {
      // Given
      mockedStatic.when(UUID::randomUUID).thenReturn(batchId);
      var annotations = List.of(givenAnnotationProcessed());

      // When
      var result = service.mintBatchIds(annotations, true,
          new AnnotationEvent(annotations, JOB_ID, null, null));

      // Then
      assertThat(result).contains(givenBatchIdMap());
    }
  }

  @Test
  void testMintBatchIdsBatchingNotRequested() {
    var annotations = List.of(givenAnnotationProcessed());

    // When
    var result = service.mintBatchIds(annotations, false,
        new AnnotationEvent(annotations, JOB_ID, null, null));

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  void testMintBatchIdsIsBatchResult() {
    var annotations = List.of(givenAnnotationProcessed());

    // When
    var result = service.mintBatchIds(annotations, true,
        new AnnotationEvent(annotations, JOB_ID, null, BATCH_ID));

    // Then
    assertThat(result).isEmpty();
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


}
