package eu.dissco.annotationprocessingservice.domain.annotation;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
@Builder
public class Annotation {

  @JsonProperty("ods:id")
  private String odsId;
  @JsonProperty("ods:jobId")
  String odsJobId;
  @JsonProperty("ods:version")
  private Integer odsVersion;
  @JsonProperty("rdf:type")
  @Builder.Default
  private String rdfType = "Annotation";
  @JsonProperty("oa:motivation")
  private Motivation oaMotivation;
  @JsonProperty("oa:motivatedBy")
  private String oaMotivatedBy;
  @JsonProperty("oa:target")
  private Target oaTarget;
  @JsonProperty("oa:body")
  private Body oaBody;
  @JsonProperty("oa:creator")
  private Creator oaCreator;
  @JsonProperty("dcterms:created")
  private Instant dcTermsCreated;
  @JsonProperty("ods:deletedOn")
  private Instant odsDeletedOn;
  @JsonProperty("as:generator")
  private Generator asGenerator;
  @JsonProperty("oa:generated")
  private Instant oaGenerated;
  @JsonProperty("schema.org:aggregateRating")
  private AggregateRating odsAggregateRating;
  @JsonProperty("placeInBatch")
  private Integer placeInBatch;

}
