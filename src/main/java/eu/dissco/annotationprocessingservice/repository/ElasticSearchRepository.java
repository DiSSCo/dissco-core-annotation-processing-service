package eu.dissco.annotationprocessingservice.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.properties.ElasticSearchProperties;
import java.io.IOException;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class ElasticSearchRepository {

  private final ElasticsearchClient client;
  private final ElasticSearchProperties properties;

  public IndexResponse indexAnnotation(Annotation annotation) throws IOException {
    return client.index(
        idx -> idx.index(properties.getIndexName()).id(annotation.getOdsId())
            .document(annotation));
  }

  public BulkResponse indexAnnotations(List<Annotation> annotations) throws IOException {
    var bulkRequest = new BulkRequest.Builder();
    for (var annotation : annotations) {
      bulkRequest.operations(op ->
          op.index(idx -> idx
              .index(properties.getIndexName())
              .id(annotation.getOdsId())
              .document(annotation))
      );
    }
    return client.bulk(bulkRequest.build());
  }


  public DeleteResponse archiveAnnotation(String id) throws IOException {
    return client.delete(
        req -> req.index(properties.getIndexName()).id(id)
    );
  }

  public BulkResponse archiveAnnotations(List<String> idList) throws IOException {
    var bulkRequest = new BulkRequest.Builder();
    for (var id : idList) {
      bulkRequest.operations(op ->
          op.delete(req -> req
              .index(properties.getIndexName())
              .id(id)
          ));
    }
    return client.bulk(bulkRequest.build());
  }
}
