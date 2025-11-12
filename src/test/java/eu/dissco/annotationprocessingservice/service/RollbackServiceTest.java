package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.BATCH_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessedAlt;
import static eu.dissco.annotationprocessingservice.TestUtils.givenBatchIdMap;
import static eu.dissco.annotationprocessingservice.TestUtils.givenHashedAnnotation;
import static eu.dissco.annotationprocessingservice.TestUtils.givenHashedAnnotationAlt;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.doThrow;

import eu.dissco.annotationprocessingservice.domain.HashedAnnotation;
import eu.dissco.annotationprocessingservice.domain.UpdatedAnnotation;
import eu.dissco.annotationprocessingservice.exception.PidCreationException;
import eu.dissco.annotationprocessingservice.repository.AnnotationBatchRecordRepository;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.web.HandleComponent;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.jooq.exception.DataAccessException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RollbackServiceTest {

  private RollbackService rollbackService;

  @Mock
  private FdoRecordService fdoRecordService;
  @Mock
  private HandleComponent handleComponent;
  @Mock
  private ElasticSearchRepository elasticRepository;
  @Mock
  private AnnotationRepository repository;
  @Mock
  private AnnotationBatchRecordRepository batchRecordRepository;

  @BeforeEach
  void setUp() {
    rollbackService = new RollbackService(fdoRecordService, handleComponent, elasticRepository,
        repository, batchRecordRepository);
  }

  @Test
  void testRollbackNewAnnotationsHashBatchIdsPresent() throws Exception {
    // Given
    var annotations = List.of(givenHashedAnnotation());
    var batchIds = Optional.of(givenBatchIdMap());

    // When
    rollbackService.rollbackNewAnnotationsHash(annotations, true, true, batchIds);

    // Then
    then(handleComponent).should().rollbackHandleCreation(any());
    then(repository).should().rollbackAnnotations(List.of(ID));
    then(elasticRepository).should().archiveAnnotations(List.of(ID));
    then(batchRecordRepository).should().rollbackAnnotationBatchRecord(Set.of(BATCH_ID));
  }

  @Test
  void testRollbackNewAnnotationsHashBatchIdsAbsent() throws Exception {
    // Given
    var annotations = List.of(givenHashedAnnotation());

    // When
    rollbackService.rollbackNewAnnotationsHash(annotations, true, true, Optional.empty());

    // Then
    then(handleComponent).should().rollbackHandleCreation(any());
    then(repository).should().rollbackAnnotations(List.of(ID));
    then(elasticRepository).should().archiveAnnotations(List.of(ID));
    then(batchRecordRepository).shouldHaveNoMoreInteractions();
  }

  @Test
  void testRollbackNewAnnotationsPid() throws Exception {
    // Given
    var annotations = List.of(givenAnnotationProcessed());

    // When
    rollbackService.rollbackNewAnnotations(annotations, false, false);

    // Then
    then(handleComponent).should().rollbackHandleCreation(any());
    then(repository).shouldHaveNoInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
  }

  @Test
  void testRollbackNewAnnotationsPidFailed() throws Exception {
    // Given
    var annotations = List.of(givenAnnotationProcessed());
    doThrow(PidCreationException.class).when(handleComponent).rollbackHandleCreation(any());

    // When
    rollbackService.rollbackNewAnnotations(annotations, false, false);

    // Then
    then(repository).shouldHaveNoInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
  }

  @Test
  void testRollbackNewAnnotationsPidRepo() throws Exception {
    // Given
    var annotations = List.of(givenAnnotationProcessed());

    // When
    rollbackService.rollbackNewAnnotations(annotations, false, true);

    // Then
    then(handleComponent).should().rollbackHandleCreation(any());
    then(repository).should().rollbackAnnotations(List.of(ID));
    then(elasticRepository).shouldHaveNoInteractions();
  }

  @Test
  void testRollbackNewAnnotationsPidRepoFailed() throws Exception {
    // Given
    var annotations = List.of(givenAnnotationProcessed());
    doThrow(DataAccessException.class).when(repository).rollbackAnnotations(List.of(ID));

    // When
    rollbackService.rollbackNewAnnotations(annotations, false, true);

    // Then
    then(handleComponent).should().rollbackHandleCreation(any());
    then(elasticRepository).shouldHaveNoInteractions();
  }

  @Test
  void testRollbackNewAnnotationsPidRepoElastic() throws Exception {
    // Given
    var annotations = List.of(givenAnnotationProcessed());

    // When
    rollbackService.rollbackNewAnnotations(annotations, true, true);

    // Then
    then(handleComponent).should().rollbackHandleCreation(any());
    then(repository).should().rollbackAnnotations(List.of(ID));
    then(elasticRepository).should().archiveAnnotations(List.of(ID));
  }

  @Test
  void testRollbackNewAnnotationsPidRepoElasticFailed() throws Exception {
    // Given
    var annotations = List.of(givenAnnotationProcessed());
    doThrow(IOException.class).when(elasticRepository).archiveAnnotations(List.of(ID));

    // When
    rollbackService.rollbackNewAnnotations(annotations, true, true);

    // Then
    then(handleComponent).should().rollbackHandleCreation(any());
    then(repository).should().rollbackAnnotations(List.of(ID));
  }

  @Test
  void testRollbackUpdatedAnnotationsPidDoesNotNeedUpdate() {
    // Given
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(false);

    // When
    rollbackService.rollbackUpdatedAnnotations(givenUpdatedAnnotations(), false, false);

    // Then
    then(handleComponent).shouldHaveNoInteractions();
    then(repository).shouldHaveNoInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
  }

  @Test
  void testRollbackUpdatedAnnotationsPid() throws Exception {
    // Given
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    rollbackService.rollbackUpdatedAnnotations(givenUpdatedAnnotations(), false, false);

    // Then
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(repository).shouldHaveNoInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
  }

  @Test
  void testRollbackUpdatedAnnotationsPidFailed() throws Exception {
    // Given
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    doThrow(PidCreationException.class).when(handleComponent).rollbackHandleUpdate(any());

    // When
    rollbackService.rollbackUpdatedAnnotations(givenUpdatedAnnotations(), false, false);

    // Then
    then(repository).shouldHaveNoInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
  }

  @Test
  void testRollbackUpdatedAnnotationsPidRepo() throws Exception {
    // Given
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    var updates = givenUpdatedAnnotations();
    var previousVersion = updates.stream().map(UpdatedAnnotation::hashedCurrentAnnotation).toList();

    // When
    rollbackService.rollbackUpdatedAnnotations(updates, false, true);

    // Then
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(repository).should().createAnnotationRecordsHashed(previousVersion, false);
    then(elasticRepository).shouldHaveNoInteractions();
  }

  @Test
  void testRollbackUpdatedAnnotationsPidRepoElastic() throws Exception {
    // Given
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    var updates = givenUpdatedAnnotations();
    var previousVersion = updates.stream().map(UpdatedAnnotation::hashedCurrentAnnotation).toList();

    // When
    rollbackService.rollbackUpdatedAnnotations(updates, true, true);

    // Then
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(repository).should().createAnnotationRecordsHashed(previousVersion, false);
    then(elasticRepository).should().indexAnnotations(previousVersion.stream().map(
        HashedAnnotation::annotation).toList());
  }

  @Test
  void testRollbackUpdatedAnnotationsPidRepoElasticFailed() throws Exception {
    // Given
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    var updates = givenUpdatedAnnotations();
    var previousVersion = updates.stream().map(UpdatedAnnotation::hashedCurrentAnnotation).toList();
    doThrow(IOException.class).when(elasticRepository).indexAnnotations(any());

    // When
    rollbackService.rollbackUpdatedAnnotations(updates, true, true);

    // Then
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(repository).should().createAnnotationRecordsHashed(previousVersion, false);
    then(elasticRepository).should().indexAnnotations(previousVersion.stream().map(
        HashedAnnotation::annotation).toList());
  }

  @Test
  void testRollbackUpdatedAnnotationsPidRepoFailed() throws Exception {
    // Given
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    var updates = givenUpdatedAnnotations();
    doThrow(DataAccessException.class).when(repository).createAnnotationRecordsHashed(anyList(),
        eq(false));

    // When
    rollbackService.rollbackUpdatedAnnotations(updates, false, true);

    // Then
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(elasticRepository).shouldHaveNoInteractions();
  }

  @Test
  void testRollbackUpdatedAnnotationPidDoesNotNeedUpdate() {
    // Given
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(false);

    // When
    rollbackService.rollbackUpdatedAnnotation(givenAnnotationProcessed(), givenAnnotationProcessedAlt(), false, false);

    // Then
    then(handleComponent).shouldHaveNoInteractions();
    then(repository).shouldHaveNoInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
  }

  @Test
  void testRollbackUpdatedAnnotationPid() throws Exception {
    // Given
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    rollbackService.rollbackUpdatedAnnotation(givenAnnotationProcessed(), givenAnnotationProcessedAlt(), false, false);

    // Then
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(repository).shouldHaveNoInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
  }

  @Test
  void testRollbackUpdatedAnnotationPidRepo() throws Exception {
    // Given
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    rollbackService.rollbackUpdatedAnnotation(givenAnnotationProcessed(), givenAnnotationProcessedAlt(), false, true);

    // Then
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(repository).should().createAnnotationRecord(givenAnnotationProcessed(), false);
    then(elasticRepository).shouldHaveNoInteractions();
  }

  @Test
  void testRollbackUpdatedAnnotationPidRepoFailed() throws Exception {
    // Given
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    doThrow(DataAccessException.class).when(repository).createAnnotationRecord(givenAnnotationProcessed(),
        false);

    // When
    rollbackService.rollbackUpdatedAnnotation(givenAnnotationProcessed(), givenAnnotationProcessedAlt(), false, true);

    // Then
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(repository).should().createAnnotationRecord(givenAnnotationProcessed(), false);
    then(elasticRepository).shouldHaveNoInteractions();
  }

  @Test
  void testRollbackUpdatedAnnotationPidRepoElastic() throws Exception {
    // Given
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    rollbackService.rollbackUpdatedAnnotation(givenAnnotationProcessed(), givenAnnotationProcessedAlt(), true, true);

    // Then
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(repository).should().createAnnotationRecord(givenAnnotationProcessed(), false);
    then(elasticRepository).should().indexAnnotation(givenAnnotationProcessed());
  }

  @Test
  void testRollbackUpdatedAnnotationPidRepoElasticFailed() throws Exception {
    // Given
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    doThrow(IOException.class).when(elasticRepository).indexAnnotation(givenAnnotationProcessed());

    // When
    rollbackService.rollbackUpdatedAnnotation(givenAnnotationProcessed(), givenAnnotationProcessedAlt(), true, true);

    // Then
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(repository).should().createAnnotationRecord(givenAnnotationProcessed(), false);
    then(elasticRepository).should().indexAnnotation(givenAnnotationProcessed());
  }



  private Set<UpdatedAnnotation> givenUpdatedAnnotations() {
    return Set.of(new UpdatedAnnotation(givenHashedAnnotation(), givenHashedAnnotationAlt()));

  }

}
