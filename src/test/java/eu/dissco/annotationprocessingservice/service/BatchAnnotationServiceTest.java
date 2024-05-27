package eu.dissco.annotationprocessingservice.service;


import static eu.dissco.annotationprocessingservice.TestUtils.BATCH_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.BATCH_ID_ALT;
import static eu.dissco.annotationprocessingservice.TestUtils.CREATED;
import static eu.dissco.annotationprocessingservice.TestUtils.CREATOR;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.ID_ALT;
import static eu.dissco.annotationprocessingservice.TestUtils.JOB_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.TARGET_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAggregationRating;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationEventBatchEnabled;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRequest;
import static eu.dissco.annotationprocessingservice.TestUtils.givenBaseAnnotationForBatch;
import static eu.dissco.annotationprocessingservice.TestUtils.givenBatchMetadataExtendedLatitudeSearch;
import static eu.dissco.annotationprocessingservice.TestUtils.givenCreator;
import static eu.dissco.annotationprocessingservice.TestUtils.givenElasticDocument;
import static eu.dissco.annotationprocessingservice.TestUtils.givenOaBody;
import static eu.dissco.annotationprocessingservice.TestUtils.givenOaTarget;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;

import eu.dissco.annotationprocessingservice.TestUtils;
import eu.dissco.annotationprocessingservice.component.JsonPathComponent;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.domain.BatchMetadataExtended;
import eu.dissco.annotationprocessingservice.domain.BatchMetadataSearchParam;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.domain.annotation.AnnotationTargetType;
import eu.dissco.annotationprocessingservice.domain.annotation.Motivation;
import eu.dissco.annotationprocessingservice.domain.annotation.Target;
import eu.dissco.annotationprocessingservice.exception.BatchingException;
import eu.dissco.annotationprocessingservice.exception.ConflictException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BatchAnnotationServiceTest {

  @Mock
  private ApplicationProperties applicationProperties;
  @Mock
  private ElasticSearchRepository elasticRepository;
  @Mock
  private KafkaPublisherService kafkaPublisherService;
  @Mock
  private JsonPathComponent jsonPathComponent;
  private BatchAnnotationService batchAnnotationService;

  @BeforeEach
  void setup() {
    batchAnnotationService = new BatchAnnotationService(applicationProperties, elasticRepository,
        kafkaPublisherService, jsonPathComponent);
  }


  @Test
  void testApplyBatchingSinglePage() throws Exception {
    // Given
    var event = givenAnnotationEventBatchEnabled();
    var annotatableIds = List.of("0", "1", "2");
    int pageSize = annotatableIds.size();
    int pageSizePlusOne = pageSize + 1;
    var elasticDocuments = annotatableIds.stream().map(TestUtils::givenElasticDocument).toList();
    var batchAnnotations = annotatableIds.stream().map(id ->
        Annotation.builder()
            .oaBody(givenOaBody())
            .oaMotivation(Motivation.COMMENTING)
            .oaTarget(givenOaTarget(String.valueOf(id)))
            .dcTermsCreated(CREATED)
            .oaCreator(givenCreator(CREATOR))
            .odsAggregateRating(givenAggregationRating())
            .odsBatchId(BATCH_ID)
            .build()
    ).toList();
    var batchEvent = new AnnotationEvent(batchAnnotations, JOB_ID, null, BATCH_ID);
    givenJsonPathResponse(annotatableIds);
    given(applicationProperties.getBatchPageSize()).willReturn(pageSize);
    given(elasticRepository.searchByBatchMetadataExtended(
        givenBatchMetadataExtendedLatitudeSearch(), AnnotationTargetType.DIGITAL_SPECIMEN, 1,
        pageSizePlusOne)).willReturn(elasticDocuments);

    // When
    batchAnnotationService.applyBatchAnnotations(event);

    // Then
    then(kafkaPublisherService).should(times(1)).publishBatchAnnotation(batchEvent);
  }

  @Test
  void testApplyBatchingSinglePageFalsePositive() throws Exception {
    // Given
    var event = givenAnnotationEventBatchEnabled();
    var annotatableIds = List.of("0", "1", "2");
    int pageSize = annotatableIds.size();
    int pageSizePlusOne = pageSize + 1;
    var elasticDocuments = annotatableIds.stream().map(TestUtils::givenElasticDocument).toList();
    given(jsonPathComponent.getAnnotationTargets(any(), any(), any())).willReturn(
        Collections.emptyList());
    given(applicationProperties.getBatchPageSize()).willReturn(pageSize);
    given(elasticRepository.searchByBatchMetadataExtended(
        givenBatchMetadataExtendedLatitudeSearch(), AnnotationTargetType.DIGITAL_SPECIMEN, 1,
        pageSizePlusOne)).willReturn(elasticDocuments);

    // When
    batchAnnotationService.applyBatchAnnotations(event);

    // Then
    then(kafkaPublisherService).shouldHaveNoInteractions();
  }


  @Test
  void testApplyBatchingSinglePageTwoBaseAnnotations() throws Exception {
    // Given
    int placeInBatch = 1;
    var annotationBodyB = givenOaBody("Alt value");
    var baseAnnotationA = givenBaseAnnotationForBatch(placeInBatch, ID, BATCH_ID);
    var baseAnnotationB = givenBaseAnnotationForBatch(placeInBatch, ID_ALT, BATCH_ID_ALT)
        .setOaBody(annotationBodyB);
    var event = new AnnotationEvent(List.of(baseAnnotationA, baseAnnotationB), JOB_ID,
        List.of(givenBatchMetadataExtendedLatitudeSearch()), null);
    var annotatableIds = List.of("0", "1", "2");
    int pageSize = annotatableIds.size();
    int pageSizePlusOne = pageSize + 1;
    var elasticDocuments = annotatableIds.stream().map(TestUtils::givenElasticDocument).toList();
    var batchAnnotationsA = annotatableIds.stream().map(id ->
        Annotation.builder()
            .oaBody(givenOaBody())
            .oaMotivation(Motivation.COMMENTING)
            .oaTarget(givenOaTarget(String.valueOf(id)))
            .dcTermsCreated(CREATED)
            .oaCreator(givenCreator(CREATOR))
            .odsAggregateRating(givenAggregationRating())
            .odsBatchId(BATCH_ID)
            .build()
    ).toList();
    var batchAnnotationsB = annotatableIds.stream().map(id ->
        Annotation.builder()
            .oaBody(annotationBodyB)
            .oaMotivation(Motivation.COMMENTING)
            .oaTarget(givenOaTarget(String.valueOf(id)))
            .dcTermsCreated(CREATED)
            .oaCreator(givenCreator(CREATOR))
            .odsAggregateRating(givenAggregationRating())
            .odsBatchId(BATCH_ID_ALT)
            .build()
    ).toList();

    var batchEventA = new AnnotationEvent(batchAnnotationsA, JOB_ID, null, BATCH_ID);
    var batchEventB = new AnnotationEvent(batchAnnotationsB, JOB_ID, null, BATCH_ID_ALT);

    givenJsonPathResponse(annotatableIds);
    given(applicationProperties.getBatchPageSize()).willReturn(pageSize);
    given(elasticRepository.searchByBatchMetadataExtended(
        givenBatchMetadataExtendedLatitudeSearch(), AnnotationTargetType.DIGITAL_SPECIMEN, 1,
        pageSizePlusOne)).willReturn(elasticDocuments);

    // When
    batchAnnotationService.applyBatchAnnotations(event);

    // Then
    then(kafkaPublisherService).should().publishBatchAnnotation(batchEventA);
    then(kafkaPublisherService).should().publishBatchAnnotation(batchEventB);
  }

  @Test
  void testApplyBatchingTwoBaseAnnotationsTypeMismatch() {
    // Given
    int placeInBatch = 1;
    var baseAnnotationA = givenAnnotationRequest().setPlaceInBatch(placeInBatch);
    var baseAnnotationB = givenAnnotationRequest().setPlaceInBatch(placeInBatch)
        .setOaTarget(givenOaTarget(ID_ALT, AnnotationTargetType.MEDIA_OBJECT));
    var event = new AnnotationEvent(List.of(baseAnnotationA, baseAnnotationB), JOB_ID,
        List.of(givenBatchMetadataExtendedLatitudeSearch()), null);

    // When
    assertThrows(ConflictException.class, () -> batchAnnotationService.applyBatchAnnotations(event
    ));

    // Then
    then(jsonPathComponent).shouldHaveNoInteractions();
    then(kafkaPublisherService).shouldHaveNoInteractions();
  }


  @Test
  void testApplyBatchingMultipleBatchMetadata() throws Exception {
    // Given
    var annotatableIdsA = List.of("0", "1", "2");
    var annotatableIdsB = List.of("3", "4", "5");
    var annotationBodyB = givenOaBody("alt value");
    var annotationTargetB = givenOaTarget(ID_ALT, AnnotationTargetType.MEDIA_OBJECT);
    var batchMetadataA = givenBatchMetadataExtendedLatitudeSearch();
    var batchMetadataB = new BatchMetadataExtended(2,
        List.of(new BatchMetadataSearchParam(
            "digitalSpecimenWrapper.occurrences[*].location.georeference.dwc:decimalLatitude.dwc:value",
            "12")));
    var batchMetadataList = List.of(batchMetadataA, batchMetadataB);
    var baseAnnotationA = givenBaseAnnotationForBatch(1, ID, BATCH_ID);
    var baseAnnotationB = givenBaseAnnotationForBatch(2, ID, BATCH_ID_ALT)
        .setOaBody(annotationBodyB)
        .setOaTarget(annotationTargetB);
    var event = new AnnotationEvent(List.of(baseAnnotationA, baseAnnotationB), JOB_ID,
        batchMetadataList, null);

    int pageSize = annotatableIdsA.size();
    int pageSizePlusOne = pageSize + 1;
    var elasticDocumentsA = annotatableIdsA.stream().map(TestUtils::givenElasticDocument).toList();
    var elasticDocumentsB = annotatableIdsB.stream().map(TestUtils::givenElasticDocument).toList();
    var batchAnnotationsA = annotatableIdsA.stream().map(id ->
        Annotation.builder()
            .oaBody(givenOaBody())
            .oaMotivation(Motivation.COMMENTING)
            .oaTarget(givenOaTarget(String.valueOf(id)))
            .dcTermsCreated(CREATED)
            .oaCreator(givenCreator(CREATOR))
            .odsAggregateRating(givenAggregationRating())
            .odsBatchId(BATCH_ID)
            .build()
    ).toList();

    var batchAnnotationsB = annotatableIdsB.stream().map(id ->
        Annotation.builder()
            .oaBody(annotationBodyB)
            .oaMotivation(Motivation.COMMENTING)
            .oaTarget(givenOaTarget(String.valueOf(id)))
            .dcTermsCreated(CREATED)
            .oaCreator(givenCreator(CREATOR))
            .odsAggregateRating(givenAggregationRating())
            .odsBatchId(BATCH_ID_ALT)
            .build()
    ).toList();
    var batchEventA = new AnnotationEvent(batchAnnotationsA, JOB_ID, null, BATCH_ID);
    var batchEventB = new AnnotationEvent(batchAnnotationsB, JOB_ID, null, BATCH_ID_ALT);
    givenJsonPathResponse(annotatableIdsA);
    givenJsonPathResponse(annotatableIdsB, annotationTargetB);
    given(applicationProperties.getBatchPageSize()).willReturn(pageSize);
    given(elasticRepository.searchByBatchMetadataExtended(
        batchMetadataA, AnnotationTargetType.DIGITAL_SPECIMEN, 1, pageSizePlusOne)).willReturn(
        elasticDocumentsA);
    given(elasticRepository.searchByBatchMetadataExtended(
        batchMetadataB, AnnotationTargetType.MEDIA_OBJECT, 1, pageSizePlusOne)).willReturn(
        elasticDocumentsB);

    // When
    batchAnnotationService.applyBatchAnnotations(event);

    // Then
    then(kafkaPublisherService).should(times(1)).publishBatchAnnotation(batchEventA);
    then(kafkaPublisherService).should(times(1)).publishBatchAnnotation(batchEventB);
  }


  @Test
  void testApplyBatchAnnotationsMissingPlaceInBatch() {
    // Given
    var event = new AnnotationEvent(List.of(givenAnnotationRequest()), JOB_ID,
        List.of(givenBatchMetadataExtendedLatitudeSearch()), null);

    // When
    assertThrows(ConflictException.class,
        () -> batchAnnotationService.applyBatchAnnotations(event));
  }

  @Test
  void testNewMessageErrorCount() throws Exception {
    // Given
    var maxRetries = 3;
    int pageSize = 5;
    var elasticResults = Collections.nCopies(pageSize + 1, givenElasticDocument());
    var event = givenAnnotationEventBatchEnabled();
    given(applicationProperties.getBatchPageSize()).willReturn(5);
    given(applicationProperties.getMaxBatchRetries()).willReturn(maxRetries);
    given(elasticRepository.searchByBatchMetadataExtended(
        any(), any(), anyInt(), anyInt())).willReturn(elasticResults);
    doThrow(BatchingException.class).when(jsonPathComponent)
        .getAnnotationTargets(any(), any(), any());

    // When
    assertThrows(BatchingException.class,
        () -> batchAnnotationService.applyBatchAnnotations(event));

    then(elasticRepository).should(times(maxRetries))
        .searchByBatchMetadataExtended(any(), any(), anyInt(), anyInt());
  }

  @Test
  void testNewMessageBatchEnabledTwoPages() throws Exception {
    // Given
    var event = givenAnnotationEventBatchEnabled();
    int pageSize = 3;
    int pageSizePlusOne = pageSize + 1;
    var elasticPageOne = Collections.nCopies(pageSizePlusOne, givenElasticDocument());
    var elasticPageTwo = List.of(givenElasticDocument());
    given(applicationProperties.getBatchPageSize()).willReturn(pageSize);
    given(elasticRepository.searchByBatchMetadataExtended(
        givenBatchMetadataExtendedLatitudeSearch(), AnnotationTargetType.DIGITAL_SPECIMEN, 1,
        pageSizePlusOne)).willReturn(elasticPageOne);
    given(elasticRepository.searchByBatchMetadataExtended(
        givenBatchMetadataExtendedLatitudeSearch(), AnnotationTargetType.DIGITAL_SPECIMEN, 2,
        pageSizePlusOne)).willReturn(elasticPageTwo);
    given(jsonPathComponent.getAnnotationTargets(any(), any(), any())).willReturn(
        List.of(givenOaTarget(ID)));

    // When
    batchAnnotationService.applyBatchAnnotations(event);

    // Then
    then(kafkaPublisherService).should(times(2)).publishBatchAnnotation(any());
  }

  @Test
  void testApplyBatchAnnotationsNoElasticMatch() throws Exception {
    // Given
    var event = givenAnnotationEventBatchEnabled();
    given(applicationProperties.getBatchPageSize()).willReturn(10);
    given(elasticRepository.searchByBatchMetadataExtended(any(), any(), anyInt(),
        anyInt())).willReturn(
        Collections.emptyList());

    // When
    batchAnnotationService.applyBatchAnnotations(event);

    // Then
    then(kafkaPublisherService).shouldHaveNoInteractions();
    then(jsonPathComponent).shouldHaveNoInteractions();
  }

  private void givenJsonPathResponse(List<String> ids) throws BatchingException {
    givenJsonPathResponse(ids, givenOaTarget(TARGET_ID));
  }

  private void givenJsonPathResponse(List<String> ids, Target target) throws BatchingException {
    for (var id : ids) {
      given(jsonPathComponent.getAnnotationTargets(any(), eq(givenElasticDocument(id)),
          eq(target))).willReturn(List.of(givenOaTarget(id)));
    }
  }
}
