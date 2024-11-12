package eu.dissco.annotationprocessingservice.domain;

import lombok.Getter;

@Getter
public enum AgentRoleType {

  COLLECTOR("collector"),
  DATA_TRANSLATOR("data-translator"),
  CREATOR("creator"),
  IDENTIFIER("identifier"),
  GEOREFERENCER("georeferencer"),
  RIGHTS_OWNER("rights-owner"),
  PROCESSING_SERVICE("processing-service"),
  SOURCE_SYSTEM("source-system"),
  TOMBSTONER("tombstoner");

  private final String name;

  AgentRoleType(String name) {
    this.name = name;
  }
}
