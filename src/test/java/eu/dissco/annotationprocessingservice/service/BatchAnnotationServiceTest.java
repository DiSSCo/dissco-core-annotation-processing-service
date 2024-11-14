package eu.dissco.annotationprocessingservice.service;


import static eu.dissco.annotationprocessingservice.TestUtils.BATCH_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.BATCH_ID_ALT;
import static eu.dissco.annotationprocessingservice.TestUtils.CREATED;
import static eu.dissco.annotationprocessingservice.TestUtils.CREATOR;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.ID_ALT;
import static eu.dissco.annotationprocessingservice.TestUtils.JOB_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.TARGET_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationBatchMetadataLatitudeSearch;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationEventBatchEnabled;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenBaseAnnotationForBatch;
import static eu.dissco.annotationprocessingservice.TestUtils.givenCreator;
import static eu.dissco.annotationprocessingservice.TestUtils.givenElasticDocument;
import static eu.dissco.annotationprocessingservice.TestUtils.givenOaBody;
import static eu.dissco.annotationprocessingservice.TestUtils.givenOaTarget;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;

import eu.dissco.annotationprocessingservice.TestUtils;
import eu.dissco.annotationprocessingservice.component.JsonPathComponent;
import eu.dissco.annotationprocessingservice.domain.AnnotationTargetType;
import eu.dissco.annotationprocessingservice.domain.ProcessedAnnotationBatch;
import eu.dissco.annotationprocessingservice.exception.BatchingException;
import eu.dissco.annotationprocessingservice.exception.ConflictException;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.schema.Annotation;
import eu.dissco.annotationprocessingservice.schema.Annotation.OaMotivation;
import eu.dissco.annotationprocessingservice.schema.AnnotationBatchMetadata;
import eu.dissco.annotationprocessingservice.schema.AnnotationTarget;
import eu.dissco.annotationprocessingservice.schema.SearchParam;
import java.util.Collections;
import java.util.Date;
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
    var elasticDocuments = annotatableIds.stream().map(TestUtils::givenElasticDocument).toList();
    var batchAnnotations = annotatableIds.stream().map(id ->
        new Annotation()
            .withOaHasBody(givenOaBody())
            .withOaMotivation(OaMotivation.OA_COMMENTING)
            .withOaHasTarget(givenOaTarget(id))
            .withDctermsCreated(Date.from(CREATED))
            .withDctermsCreator(givenCreator(CREATOR))
            .withOdsBatchID(BATCH_ID)
    ).toList();
    var batchEvent = new ProcessedAnnotationBatch(batchAnnotations, JOB_ID, null, BATCH_ID);
    givenJsonPathResponse(annotatableIds);
    given(elasticRepository.searchByBatchMetadata(
        givenAnnotationBatchMetadataLatitudeSearch(), AnnotationTargetType.DIGITAL_SPECIMEN,
        null)).willReturn(elasticDocuments);

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
    var elasticDocuments = annotatableIds.stream().map(TestUtils::givenElasticDocument).toList();
    given(jsonPathComponent.getAnnotationTargets(any(), any(), any())).willReturn(
        Collections.emptyList());
    given(elasticRepository.searchByBatchMetadata(
        givenAnnotationBatchMetadataLatitudeSearch(), AnnotationTargetType.DIGITAL_SPECIMEN,
        null)).willReturn(elasticDocuments);
    given(elasticRepository.searchByBatchMetadata(
        eq(givenAnnotationBatchMetadataLatitudeSearch()), eq(AnnotationTargetType.DIGITAL_SPECIMEN),
        anyString())).willReturn(List.of());

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
        .withOaHasBody(annotationBodyB);
    var event = new ProcessedAnnotationBatch(List.of(baseAnnotationA, baseAnnotationB), JOB_ID,
        List.of(givenAnnotationBatchMetadataLatitudeSearch()), null);
    var annotatableIds = List.of("0", "1", "2");
    var elasticDocuments = annotatableIds.stream().map(TestUtils::givenElasticDocument).toList();
    var batchAnnotationsA = annotatableIds.stream().map(id ->
        new Annotation()
            .withOaHasBody(givenOaBody())
            .withOaMotivation(OaMotivation.OA_COMMENTING)
            .withOaHasTarget(givenOaTarget(id))
            .withDctermsCreated(Date.from(CREATED))
            .withDctermsCreator(givenCreator(CREATOR))
            .withOdsBatchID(BATCH_ID)
    ).toList();
    var batchAnnotationsB = annotatableIds.stream().map(id ->
        new Annotation()
            .withOaHasBody(annotationBodyB)
            .withOaMotivation(OaMotivation.OA_COMMENTING)
            .withOaHasTarget(givenOaTarget(id))
            .withDctermsCreated(Date.from(CREATED))
            .withDctermsCreator(givenCreator(CREATOR))
            .withOdsBatchID(BATCH_ID_ALT)
    ).toList();
    var batchEventA = new ProcessedAnnotationBatch(batchAnnotationsA, JOB_ID, null, BATCH_ID);
    var batchEventB = new ProcessedAnnotationBatch(batchAnnotationsB, JOB_ID, null, BATCH_ID_ALT);

    givenJsonPathResponse(annotatableIds);
    given(elasticRepository.searchByBatchMetadata(
        givenAnnotationBatchMetadataLatitudeSearch(), AnnotationTargetType.DIGITAL_SPECIMEN,
        null)).willReturn(elasticDocuments);

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
    var baseAnnotationA = givenAnnotationProcessed().withOdsPlaceInBatch(placeInBatch);
    var baseAnnotationB = givenAnnotationProcessed().withOdsPlaceInBatch(placeInBatch)
        .withOaHasTarget(givenOaTarget(ID_ALT, AnnotationTargetType.MEDIA_OBJECT));
    var event = new ProcessedAnnotationBatch(List.of(baseAnnotationA, baseAnnotationB), JOB_ID,
        List.of(givenAnnotationBatchMetadataLatitudeSearch()), null);

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
    var batchMetadataA = givenAnnotationBatchMetadataLatitudeSearch();
    var batchMetadataB = new AnnotationBatchMetadata(2,
        List.of(new SearchParam(
            "ods:hasEvent[*].ods:Location.ods:GeoReference.dwc:decimalLatitude",
            "62.123")));
    var batchMetadataList = List.of(batchMetadataA, batchMetadataB);
    var baseAnnotationA = givenBaseAnnotationForBatch(1, ID, BATCH_ID);
    var baseAnnotationB = givenBaseAnnotationForBatch(2, ID, BATCH_ID_ALT)
        .withOaHasBody(annotationBodyB)
        .withOaHasTarget(annotationTargetB);
    var event = new ProcessedAnnotationBatch(List.of(baseAnnotationA, baseAnnotationB), JOB_ID,
        batchMetadataList, null);
    var elasticDocumentsA = annotatableIdsA.stream().map(TestUtils::givenElasticDocument).toList();
    var elasticDocumentsB = annotatableIdsB.stream().map(TestUtils::givenElasticDocument).toList();
    var batchAnnotationsA = annotatableIdsA.stream().map(id ->
        new Annotation()
            .withOaHasBody(givenOaBody())
            .withOaMotivation(OaMotivation.OA_COMMENTING)
            .withOaHasTarget(givenOaTarget(id))
            .withDctermsCreated(Date.from(CREATED))
            .withDctermsCreator(givenCreator(CREATOR))
            .withOdsBatchID(BATCH_ID)
    ).toList();
    var batchAnnotationsB = annotatableIdsB.stream().map(id ->
        new Annotation()
            .withOaHasBody(annotationBodyB)
            .withOaMotivation(OaMotivation.OA_COMMENTING)
            .withOaHasTarget(givenOaTarget(id))
            .withDctermsCreated(Date.from(CREATED))
            .withDctermsCreator(givenCreator(CREATOR))
            .withOdsBatchID(BATCH_ID_ALT)
    ).toList();
    var batchEventA = new ProcessedAnnotationBatch(batchAnnotationsA, JOB_ID, null, BATCH_ID);
    var batchEventB = new ProcessedAnnotationBatch(batchAnnotationsB, JOB_ID, null, BATCH_ID_ALT);
    givenJsonPathResponse(annotatableIdsA);
    givenJsonPathResponse(annotatableIdsB, annotationTargetB);
    given(elasticRepository.searchByBatchMetadata(
        batchMetadataA, AnnotationTargetType.DIGITAL_SPECIMEN, null)).willReturn(
        elasticDocumentsA);
    given(elasticRepository.searchByBatchMetadata(
        batchMetadataB, AnnotationTargetType.MEDIA_OBJECT, null)).willReturn(
        elasticDocumentsB);
    given(elasticRepository.searchByBatchMetadata(any(), any(), anyString())).willReturn(List.of());

    // When
    batchAnnotationService.applyBatchAnnotations(event);

    // Then
    then(kafkaPublisherService).should(times(1)).publishBatchAnnotation(batchEventA);
    then(kafkaPublisherService).should(times(1)).publishBatchAnnotation(batchEventB);
  }

  @Test
  void testApplyBatchAnnotationsMissingPlaceInBatch() {
    // Given
    var event = new ProcessedAnnotationBatch(List.of(givenAnnotationProcessed()), JOB_ID,
        List.of(givenAnnotationBatchMetadataLatitudeSearch()), null);

    // When
    assertThrows(ConflictException.class,
        () -> batchAnnotationService.applyBatchAnnotations(event));
  }

  @Test
  void testNewMessageErrorCount() throws Exception {
    // Given
    var maxRetries = 3;
    var elasticResults = Collections.nCopies(6, givenElasticDocument());
    var event = givenAnnotationEventBatchEnabled();
    given(applicationProperties.getMaxBatchRetries()).willReturn(maxRetries);
    given(elasticRepository.searchByBatchMetadata(
        any(), any(), eq(null))).willReturn(elasticResults);
    given(elasticRepository.searchByBatchMetadata(any(), any(), anyString())).willReturn(
        elasticResults);
    doThrow(BatchingException.class).when(jsonPathComponent)
        .getAnnotationTargets(any(), any(), any());

    // When
    assertThrows(BatchingException.class,
        () -> batchAnnotationService.applyBatchAnnotations(event));

    then(elasticRepository).should(times(maxRetries))
        .searchByBatchMetadata(any(), any(), any());
  }

  @Test
  void testNewMessageBatchEnabledTwoPages() throws Exception {
    // Given
    var event = givenAnnotationEventBatchEnabled();
    var elasticPageOne = Collections.nCopies(5, givenElasticDocument());
    var elasticPageTwo = List.of(givenElasticDocument());
    given(elasticRepository.searchByBatchMetadata(
        givenAnnotationBatchMetadataLatitudeSearch(), AnnotationTargetType.DIGITAL_SPECIMEN,
        null)).willReturn(elasticPageOne);
    given(elasticRepository.searchByBatchMetadata(
        eq(givenAnnotationBatchMetadataLatitudeSearch()), eq(AnnotationTargetType.DIGITAL_SPECIMEN),
        anyString())).willReturn(elasticPageTwo).willReturn(List.of());
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
    given(elasticRepository.searchByBatchMetadata(any(), any(), eq(null))).willReturn(
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

  private void givenJsonPathResponse(List<String> ids, AnnotationTarget target)
      throws BatchingException {
    for (var id : ids) {
      given(jsonPathComponent.getAnnotationTargets(any(), eq(givenElasticDocument(id)),
          eq(target))).willReturn(List.of(givenOaTarget(id)));
    }
  }
}
