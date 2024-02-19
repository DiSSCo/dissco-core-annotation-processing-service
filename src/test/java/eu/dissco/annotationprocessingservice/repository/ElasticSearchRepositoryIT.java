package eu.dissco.annotationprocessingservice.repository;


import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.ID_ALT;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.TARGET_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenBatchMetadataCountrySearch;
import static eu.dissco.annotationprocessingservice.TestUtils.givenElasticDocument;
import static org.assertj.core.api.Assertions.assertThat;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.annotationprocessingservice.domain.BatchMetadata;
import eu.dissco.annotationprocessingservice.domain.annotation.AnnotationTargetType;
import eu.dissco.annotationprocessingservice.properties.ElasticSearchProperties;
import java.io.IOException;
import java.util.List;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
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
      "docker.elastic.co/elasticsearch/elasticsearch").withTag("8.6.1");
  private static final String ANNOTATION_INDEX = "annotations";
  private static final String DIGITAL_SPECIMEN_INDEX = "digital-specimen";
  private static final String MEDIA_INDEX = "digital-media-object";
  private static final String ELASTICSEARCH_USERNAME = "elastic";
  private static final String ELASTICSEARCH_PASSWORD = "s3cret";
  private static final ElasticsearchContainer container = new ElasticsearchContainer(
      ELASTIC_IMAGE).withPassword(ELASTICSEARCH_PASSWORD);
  private static ElasticsearchClient client;
  private static RestClient restClient;
  private ElasticSearchRepository repository;

  @BeforeAll
  static void initContainer() {
    // Create the elasticsearch container.
    container.start();

    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,
        new UsernamePasswordCredentials(ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD));

    HttpHost host = new HttpHost("localhost",
        container.getMappedPort(9200), "https");
    final RestClientBuilder builder = RestClient.builder(host);

    builder.setHttpClientConfigCallback(clientBuilder -> {
      clientBuilder.setSSLContext(container.createSslContextFromCa());
      clientBuilder.setDefaultCredentialsProvider(credentialsProvider);
      return clientBuilder;
    });
    restClient = builder.build();

    ElasticsearchTransport transport = new RestClientTransport(restClient,
        new JacksonJsonpMapper(MAPPER));

    client = new ElasticsearchClient(transport);
  }

  @AfterAll
  public static void closeResources() throws Exception {
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
        givenAnnotationProcessed().withOdsId("alt"));

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
  void searchByBatchMetadata() throws Exception {
    // Given
    var targetDocument = givenElasticDocument("Netherlands", TARGET_ID);
    var altDocument = givenElasticDocument("OtherCountry", ID_ALT);
    postDocuments(List.of(targetDocument, altDocument), DIGITAL_SPECIMEN_INDEX);
    var batchMetadata = givenBatchMetadataCountrySearch();

    // When
    var result = repository.searchByBatchMetadata(batchMetadata,
        AnnotationTargetType.DIGITAL_SPECIMEN, 1, 10);

    // Then
    assertThat(result).isEqualTo(List.of(targetDocument));
  }

  @Test
  void testByBatchMetadataSpecimen() throws Exception {
    // Given
    var targetDocument = givenElasticDocument("Netherlands", TARGET_ID);
    var altDocument = givenElasticDocument("Netherlands kingdom", ID_ALT);
    postDocuments(List.of(targetDocument, altDocument), DIGITAL_SPECIMEN_INDEX);
    var batchMetadata = givenBatchMetadataCountrySearch();

    // When
    var result = repository.searchByBatchMetadata(batchMetadata,
        AnnotationTargetType.DIGITAL_SPECIMEN, 1, 10);

    // Then
    assertThat(result).isEqualTo(List.of(targetDocument));
  }

  @Test
  void testByBatchMetadataMedia() throws Exception {
    // Given
    var targetDocument = givenElasticDocument("Netherlands", TARGET_ID);
    var altDocument = givenElasticDocument("Netherlands k", ID_ALT);
    postDocuments(List.of(targetDocument, altDocument), MEDIA_INDEX);
    var batchMetadata = givenBatchMetadataCountrySearch();

    // When
    var result = repository.searchByBatchMetadata(batchMetadata, AnnotationTargetType.MEDIA_OBJECT,
        1, 10);

    // Then
    assertThat(result).isEqualTo(List.of(targetDocument));
  }

  @Test
  void testByBatchNoResults() throws Exception {
    // Given
    postDocuments(List.of(givenElasticDocument("OtherCountry", ID_ALT)), DIGITAL_SPECIMEN_INDEX);

    // When
    var result = repository.searchByBatchMetadata(givenBatchMetadataCountrySearch(),
        AnnotationTargetType.DIGITAL_SPECIMEN, 1, 10);

    // Then
    assertThat(result).isEmpty();
  }


  public void postDocuments(List<JsonNode> docs, String index) throws IOException {
    var bulkRequest = new BulkRequest.Builder();
    for (var doc : docs) {
      bulkRequest.operations(op -> op.index(
          idx -> idx.index(index).id(doc.get("id").asText())
              .document(doc)));
    }
    client.bulk(bulkRequest.build());
    client.indices().refresh(b -> b.index(index));
  }

  private JsonNode givenElasticOnlyOneOccurrence() throws Exception {
    return MAPPER.readTree("""
        {
          "id": "20.5000.1025/AAA-BBB-CCC",
          "digitalSpecimenWrapper": {
            "fieldNum": 1,
            "other": [
              "a",
              "10"
            ],
            "occurrences": [
              {
                "dwc:occurrenceRemarks": "Correct",
                "annotateTarget": "this",
                "hello":"hello",
                "location": {
                  "dwc:country": "netherlands",
                  "georeference": {
                    "dwc:decimalLatitude": {
                      "dwc:value": 11
                    },
                    "dwc:decimalLongitude": "10",
                    "dwc": [
                      "1"
                    ]
                  },
                  "locality": "known"
                }
              }
            ]
          }
        }
        """);
  }

}

