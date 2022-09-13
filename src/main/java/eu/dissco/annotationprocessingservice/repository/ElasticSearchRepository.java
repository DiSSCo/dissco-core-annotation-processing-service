package eu.dissco.annotationprocessingservice.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import eu.dissco.annotationprocessingservice.domain.AnnotationRecord;
import eu.dissco.annotationprocessingservice.properties.ElasticSearchProperties;
import java.io.IOException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class ElasticSearchRepository {

  private final ElasticsearchClient client;
  private final ElasticSearchProperties properties;

  public IndexResponse indexAnnotation(AnnotationRecord annotationRecord) {
    try {
      return client.index(
          idx -> idx.index(properties.getIndexName()).id(annotationRecord.id())
              .document(annotationRecord));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public DeleteResponse archiveAnnotation(String id) {
    try {
      return client.delete(
          req -> req.index(properties.getIndexName()).id(id)
      );
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
