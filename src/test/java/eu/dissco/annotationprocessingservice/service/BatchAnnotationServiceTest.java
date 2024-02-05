package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.CREATED;
import static eu.dissco.annotationprocessingservice.TestUtils.CREATOR;
import static eu.dissco.annotationprocessingservice.TestUtils.HANDLE_PROXY;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.JOB_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.TARGET_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAggregationRating;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationEventBatchEnabled;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRequest;
import static eu.dissco.annotationprocessingservice.TestUtils.givenBatchMetadataLatitudeSearch;
import static eu.dissco.annotationprocessingservice.TestUtils.givenCreator;
import static eu.dissco.annotationprocessingservice.TestUtils.givenElasticDocument;
import static eu.dissco.annotationprocessingservice.TestUtils.givenOaBody;
import static eu.dissco.annotationprocessingservice.TestUtils.givenOaTarget;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.times;

import co.elastic.clients.elasticsearch.core.BulkResponse;
import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.annotationprocessingservice.TestUtils;
import eu.dissco.annotationprocessingservice.component.JsonPathComponent;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.domain.BatchMetadata;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.domain.annotation.AnnotationTargetType;
import eu.dissco.annotationprocessingservice.domain.annotation.Motivation;
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
  @Mock
  private BulkResponse bulkResponse;
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
        new Annotation()
            .withOaBody(givenOaBody())
            .withOdsJobId(HANDLE_PROXY + JOB_ID)
            .withOaMotivation(Motivation.COMMENTING)
            .withOaTarget(givenOaTarget(String.valueOf(id)))
            .withDcTermsCreated(CREATED)
            .withOaCreator(givenCreator(CREATOR))
            .withOdsAggregateRating(givenAggregationRating())
    ).toList();
    var batchEvent = new AnnotationEvent(batchAnnotations, JOB_ID, null, true);
    givenJsonPathResponse(annotatableIds);
    given(applicationProperties.getBatchPageSize()).willReturn(pageSize);
    given(elasticRepository.searchByBatchMetadata(AnnotationTargetType.DIGITAL_SPECIMEN,
        givenBatchMetadataLatitudeSearch(), 1, pageSizePlusOne)).willReturn(elasticDocuments);

    // When
    batchAnnotationService.applyBatchAnnotations(event);

    // Then
    then(kafkaPublisherService).should(times(1)).publishBatchAnnotation(batchEvent);
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
    given(elasticRepository.searchByBatchMetadata(AnnotationTargetType.DIGITAL_SPECIMEN,
        givenBatchMetadataLatitudeSearch(), 1, pageSizePlusOne)).willReturn(elasticPageOne);
    given(elasticRepository.searchByBatchMetadata(AnnotationTargetType.DIGITAL_SPECIMEN,
        givenBatchMetadataLatitudeSearch(), 2, pageSizePlusOne)).willReturn(elasticPageTwo);
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
    given(elasticRepository.searchByBatchMetadata(any(), any(), anyInt(), anyInt())).willReturn(Collections.emptyList());

    // When
    batchAnnotationService.applyBatchAnnotations(event);

    // Then
    then(kafkaPublisherService).shouldHaveNoInteractions();
    then(jsonPathComponent).shouldHaveNoInteractions();
  }

  private void givenJsonPathResponse(List<String> ids) throws Exception {
    for (var id : ids) {
      given(jsonPathComponent.getAnnotationTargets(any(), eq(givenElasticDocument(id)),
          eq(givenOaTarget(TARGET_ID)))).willReturn(List.of(givenOaTarget(id)));
    }
  }
}