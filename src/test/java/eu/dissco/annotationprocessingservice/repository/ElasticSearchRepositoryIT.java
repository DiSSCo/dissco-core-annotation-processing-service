package eu.dissco.annotationprocessingservice.repository;


import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.ID_ALT;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.TARGET_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationBatchMetadataTwoParam;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenElasticDocument;
import static eu.dissco.annotationprocessingservice.TestUtils.givenSearchParamCountry;
import static org.assertj.core.api.Assertions.assertThat;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest5_client.Rest5ClientTransport;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.annotationprocessingservice.domain.AnnotationTargetType;
import eu.dissco.annotationprocessingservice.properties.ElasticSearchProperties;
import eu.dissco.annotationprocessingservice.schema.AnnotationBatchMetadata;
import eu.dissco.annotationprocessingservice.schema.SearchParam;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.message.BasicHeader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class ElasticSearchRepositoryIT {

  private static final DockerImageName ELASTIC_IMAGE = DockerImageName.parse(
      "docker.elastic.co/elasticsearch/elasticsearch").withTag("9.2.0");
  private static final String ANNOTATION_INDEX = "annotations";
  private static final String DIGITAL_SPECIMEN_INDEX = "digital-specimen";
  private static final String MEDIA_INDEX = "digital-media";
  private static final String ELASTICSEARCH_USERNAME = "elastic";
  private static final String ELASTICSEARCH_PASSWORD = "s3cret";
  private static final ElasticsearchContainer container = new ElasticsearchContainer(
      ELASTIC_IMAGE).withPassword(ELASTICSEARCH_PASSWORD);
  private static ElasticsearchClient client;
  private static Rest5Client restClient;
  private ElasticSearchRepository repository;

  @BeforeAll
  static void initContainer() {
    // Create the elasticsearch container.
    container.start();

    var creds = Base64.getEncoder()
        .encodeToString((ELASTICSEARCH_USERNAME + ":" + ELASTICSEARCH_PASSWORD).getBytes());

    restClient = Rest5Client.builder(
            new HttpHost("https", "localhost", container.getMappedPort(9200)))
        .setDefaultHeaders(new Header[]{new BasicHeader("Authorization", "Basic " + creds)})
        .setSSLContext(container.createSslContextFromCa()).build();

    ElasticsearchTransport transport = new Rest5ClientTransport(restClient,
        new JacksonJsonpMapper(MAPPER));

    client = new ElasticsearchClient(transport);
  }

  @AfterAll
  static void closeResources() throws Exception {
    restClient.close();
  }

  @BeforeEach
  void initRepository() {
    var properties = new ElasticSearchProperties();
    properties.setIndexName(ANNOTATION_INDEX);
    repository = new ElasticSearchRepository(client, properties);
  }

  @AfterEach
  void clearIndex() throws IOException {
    if (client.indices().exists(re -> re.index(DIGITAL_SPECIMEN_INDEX)).value()) {
      client.indices().delete(b -> b.index(DIGITAL_SPECIMEN_INDEX));
    }
    if (client.indices().exists(re -> re.index(ANNOTATION_INDEX)).value()) {
      client.indices().delete(b -> b.index(ANNOTATION_INDEX));
    }
    if (client.indices().exists(re -> re.index(MEDIA_INDEX)).value()) {
      client.indices().delete(b -> b.index(MEDIA_INDEX));
    }
  }

  @Test
  void testIndexAnnotation() throws IOException {
    // Given
    var annotation = givenAnnotationProcessed();

    // When
    var result = repository.indexAnnotation(annotation);

    // Then
    assertThat(result.result().jsonValue()).isEqualTo("created");
  }

  @Test
  void testIndexAnnotations() throws IOException {
    // Given
    var annotations = List.of(givenAnnotationProcessed(),
        givenAnnotationProcessed().withId("alt"));

    // When
    var result = repository.indexAnnotations(annotations);

    // Then
    assertThat(result.errors()).isFalse();
    assertThat(result.items().get(0).id()).isEqualTo(ID);
    assertThat(result.items().get(0).result()).isEqualTo("created");
  }

  @Test
  void testArchiveAnnotation() throws IOException {
    // Given
    var annotation = givenAnnotationProcessed();
    repository.indexAnnotation(annotation);

    // When
    var result = repository.archiveAnnotation(ID);

    // Then
    assertThat(result.result().jsonValue()).isEqualTo("deleted");
  }

  @Test
  void testArchiveAnnotations() throws IOException {
    // Given
    var annotation = givenAnnotationProcessed();
    repository.indexAnnotations(List.of(annotation));

    // When
    var result = repository.archiveAnnotations(List.of(ID));

    // Then
    assertThat(result.errors()).isFalse();
    assertThat(result.items().get(0).id()).isEqualTo(ID);
    assertThat(result.items().get(0).result()).isEqualTo("deleted");
  }

  @Test
  void searchByBatchMetadataExtended() throws Exception {
    // Given
    var targetDocument = givenElasticDocument("Netherlands", TARGET_ID);
    var altDocument = givenElasticDocument("OtherCountry", ID_ALT);
    postDocuments(List.of(targetDocument, altDocument), DIGITAL_SPECIMEN_INDEX);
    var batchMetadata = givenAnnotationBatchMetadataTwoParam();

    // When
    var result = repository.searchByBatchMetadataExtended(batchMetadata,
        AnnotationTargetType.DIGITAL_SPECIMEN, 1, 10);

    // Then
    assertThat(result).isEqualTo(List.of(targetDocument));
  }

  @Test
  void searchByBatchMetadataMustNotExist() throws Exception {
    // Given
    var targetDocument = givenElasticDocument("Netherlands", TARGET_ID);
    var altDocument = givenElasticDocument("OtherCountry", ID_ALT);
    postDocuments(List.of(targetDocument, altDocument), DIGITAL_SPECIMEN_INDEX);
    var batchMetadata = new AnnotationBatchMetadata(1, List.of(
        givenSearchParamCountry(),
        new SearchParam(
            "digitalSpecimenWrapper.thisFieldIsNotPresent",
            ""
        )));

    // When
    var result = repository.searchByBatchMetadataExtended(batchMetadata,
        AnnotationTargetType.DIGITAL_SPECIMEN, 1, 10);

    // Then
    assertThat(result).isEqualTo(List.of(targetDocument));
  }

  public void postDocuments(List<JsonNode> docs, String index) throws IOException {
    var bulkRequest = new BulkRequest.Builder();
    for (var doc : docs) {
      bulkRequest.operations(op -> op.index(
          idx -> idx.index(index).id(doc.get("@id").asText())
              .document(doc)));
    }
    client.bulk(bulkRequest.build());
    client.indices().refresh(b -> b.index(index));
  }

}

