package eu.dissco.annotationprocessingservice.service;

import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.domain.annotation.Generator;
import eu.dissco.annotationprocessingservice.properties.ApplicationProperties;
import eu.dissco.annotationprocessingservice.repository.AnnotationRepository;
import eu.dissco.annotationprocessingservice.repository.ElasticSearchRepository;
import eu.dissco.annotationprocessingservice.web.HandleComponent;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public abstract class AbstractProcessingService {

  protected final AnnotationRepository repository;
  protected final ElasticSearchRepository elasticRepository;
  protected final KafkaPublisherService kafkaService;
  protected final FdoRecordService fdoRecordService;
  protected final HandleComponent handleComponent;
  protected final ApplicationProperties applicationProperties;

  protected void enrichNewAnnotation(Annotation annotation, String id) {
    annotation
        .withOdsId(id)
        .withOdsVersion(1)
        .withAsGenerator(createGenerator())
        .withOaGenerated(Instant.now());
  }

  private Generator createGenerator() {
    return new Generator()
        .withOdsId(applicationProperties.getProcessorHandle())
        .withFoafName("Annotation Processing Service")
        .withOdsType("tool/Software");
  }

  protected void enrichUpdateAnnotation(Annotation annotation, Annotation currentAnnotation) {
    annotation
        .withOdsId(currentAnnotation.getOdsId())
        .withOdsVersion(currentAnnotation.getOdsVersion() + 1)
        .withOaGenerated(currentAnnotation.getOaGenerated())
        .withAsGenerator(currentAnnotation.getAsGenerator())
        .withOaCreator(currentAnnotation.getOaCreator())
        .withDcTermsCreated(currentAnnotation.getDcTermsCreated());
  }

}
