package eu.dissco.annotationprocessingservice.utils;

import static eu.dissco.annotationprocessingservice.schema.Identifier.DctermsType.DOI;
import static eu.dissco.annotationprocessingservice.schema.Identifier.OdsGupriLevel.GLOBALLY_UNIQUE_STABLE_PERSISTENT_RESOLVABLE_FDO_COMPLIANT;
import static eu.dissco.annotationprocessingservice.schema.Identifier.OdsIdentifierStatus.PREFERRED;

import eu.dissco.annotationprocessingservice.domain.AgentRoleType;
import eu.dissco.annotationprocessingservice.schema.Agent;
import eu.dissco.annotationprocessingservice.schema.Agent.Type;
import eu.dissco.annotationprocessingservice.schema.Identifier;
import eu.dissco.annotationprocessingservice.schema.OdsHasRole;
import java.util.List;

public class AgentUtils {

  private AgentUtils() {
  }

  public static Agent createAgent(String name, String pid, AgentRoleType role, String idTitle,
      Type agentType) {
    var agent = new Agent()
        .withType(agentType)
        .withId(pid)
        .withSchemaName(name)
        .withSchemaIdentifier(pid)
        .withOdsHasRoles(List.of(new OdsHasRole().withType("schema:Role")
            .withSchemaRoleName(role.getName())));
    if (pid != null) {
      var identifier = new Identifier()
          .withType("ods:Identifier")
          .withId(pid)
          .withDctermsIdentifier(pid)
          .withOdsIsPartOfLabel(false)
          .withOdsIdentifierStatus(PREFERRED)
          .withOdsGupriLevel(
              GLOBALLY_UNIQUE_STABLE_PERSISTENT_RESOLVABLE_FDO_COMPLIANT);
      if (DOI.value().equals(idTitle)) {
        identifier.withDctermsType(DOI);
        identifier.withDctermsTitle("DOI");
      }
      agent.setOdsHasIdentifiers(List.of(identifier));
    }
    return agent;
  }
}
