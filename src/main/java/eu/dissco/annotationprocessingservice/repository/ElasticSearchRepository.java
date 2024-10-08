package eu.dissco.annotationprocessingservice.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.dissco.annotationprocessingservice.domain.AnnotationTargetType;
import eu.dissco.annotationprocessingservice.properties.ElasticSearchProperties;
import eu.dissco.annotationprocessingservice.schema.Annotation;
import eu.dissco.annotationprocessingservice.schema.AnnotationBatchMetadata;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class ElasticSearchRepository {

  private final ElasticsearchClient client;
  private final ElasticSearchProperties properties;

  public IndexResponse indexAnnotation(Annotation annotation) throws IOException {
    return client.index(
        idx -> idx.index(properties.getIndexName())
            .id(annotation.getId())
            .document(annotation));
  }

  public BulkResponse indexAnnotations(List<Annotation> annotations) throws IOException {
    var bulkRequest = new BulkRequest.Builder();
    for (var annotation : annotations) {
      bulkRequest.operations(op ->
          op.index(idx -> idx
              .index(properties.getIndexName())
              .id(annotation.getId())
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

  private List<Query> generateBatchQueryExtended(AnnotationBatchMetadata ame) {
    var qList = new ArrayList<Query>();
    for (var searchParam : ame.getSearchParams()) {
      var key = searchParam.getInputField()
          .replace("'", "")
          .replace("[*]", "")
          .replace("$", "")
          .replace("[", "")
          .replace("]", ".")
          + "keyword";
      var val = searchParam.getInputValue();
      if (val.isEmpty()) {
        qList.add(
            new Query.Builder().bool(b -> b.mustNot(q -> q.exists(e -> e.field(key)))).build());
      } else {
        qList.add(
            new Query.Builder().term(t -> t.field(key).value(val).caseInsensitive(true)).build());
      }
    }
    return qList;
  }

  public List<JsonNode> searchByBatchMetadataExtended(AnnotationBatchMetadata batchMetadata,
      AnnotationTargetType targetType, int pageNumber, int pageSize) throws IOException {
    var query = generateBatchQueryExtended(batchMetadata);
    var index =
        targetType == AnnotationTargetType.DIGITAL_SPECIMEN ? properties.getDigitalSpecimenIndex()
            : properties.getDigitalMediaObjectIndex();
    var searchRequest = new SearchRequest.Builder()
        .index(index)
        .query(
            q -> q.bool(b -> b.must(query)))
        .trackTotalHits(t -> t.enabled(Boolean.TRUE))
        .from(getOffset(pageNumber, pageSize))
        .size(pageSize).build();
    var searchResult = client.search(searchRequest, ObjectNode.class);

    return searchResult.hits().hits().stream()
        .map(Hit::source)
        .filter(Objects::nonNull)
        .map(JsonNode.class::cast)
        .toList();
  }

  private int getOffset(int pageNumber, int pageSize) {
    int offset = 0;
    if (pageNumber > 1) {
      offset = offset + (pageSize * (pageNumber - 1));
    }
    return offset;
  }

}
