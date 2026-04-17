package eu.dissco.annotationprocessingservice.client;

import eu.dissco.annotationprocessingservice.exception.PidException;
import java.util.List;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.service.annotation.DeleteExchange;
import org.springframework.web.service.annotation.PatchExchange;
import org.springframework.web.service.annotation.PostExchange;
import org.springframework.web.service.annotation.PutExchange;
import tools.jackson.databind.JsonNode;

public interface HandleClient {

  @PostExchange("batch")
  JsonNode postHandles(@RequestBody List<JsonNode> requestBody) throws PidException;

  @PatchExchange
  void updateHandles(@RequestBody List<JsonNode> requestBody) throws PidException;

  @DeleteExchange("rollback/create")
  void rollbackHandleCreation(@RequestBody List<String> handles) throws PidException;

  @DeleteExchange("rollback/update")
  void rollbackHandleUpdate(@RequestBody List<JsonNode> handles) throws PidException;

  @PutExchange("{pid}")
  void archiveHandle(@PathVariable String pid, @RequestBody JsonNode requestBody) throws PidException;

}
