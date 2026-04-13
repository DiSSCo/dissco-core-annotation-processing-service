package eu.dissco.annotationprocessingservice.web;

import client.HandleClient;
import eu.dissco.annotationprocessingservice.exception.PidException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

@Component
@RequiredArgsConstructor
@Slf4j
public class HandleComponent {

  private final HandleClient handleClient;

  private static final String UNEXPECTED_LOG = "Unexpected response from handle API: {}";
  private static final String UNEXPECTED_ERR = "Unexpected response from handle API.";

  public String postHandle(List<JsonNode> request) throws PidException {
    var responseJson = handleClient.postHandles(request);
    return getHandleName(responseJson);
  }

  public Map<UUID, String> postHandlesHashed(List<JsonNode> request) throws PidException {
    var responseJson = handleClient.postHandles(request);
    return getHandleMapHash(responseJson);
  }

  public Map<String, String> postHandlesTargetPid(List<JsonNode> request)
      throws PidException {
    var responseJson = handleClient.postHandles(request);
    return getHandleMapTargetPid(responseJson);
  }

  public void updateHandles(List<JsonNode> request) throws PidException {
    handleClient.updateHandles(request);
  }

  public void rollbackHandleCreation(List<String> request) throws PidException {
    handleClient.rollbackHandleCreation(request);
  }

  public void rollbackHandleUpdate(List<JsonNode> request) throws PidException {
    handleClient.rollbackHandleUpdate(request);
  }

  public void archiveHandle(JsonNode request, String handle) throws PidException {
    handleClient.archiveHandle(handle, request);
  }

  private String getHandleName(JsonNode jsonResponse) throws PidException {
    try {
      var dataNodeArray = jsonResponse.get("data");
      if (!dataNodeArray.isArray()) {
        throw new PidException(
            "UNEXPECTED_MSG + \" Response: {}\", jsonResponse.toPrettyString()");
      }
      if (dataNodeArray.size() != 1) {
        log.error(UNEXPECTED_LOG, jsonResponse);
        throw new PidException(UNEXPECTED_ERR);
      }
      return dataNodeArray.get(0).get("id").asString();
    } catch (NullPointerException _) {
      log.error(UNEXPECTED_LOG, jsonResponse);
      throw new PidException(UNEXPECTED_ERR);
    }
  }

  private Map<UUID, String> getHandleMapHash(JsonNode jsonResponse) throws PidException {
    try {
      var handleNames = new HashMap<UUID, String>();
      var dataNodeArray = jsonResponse.get("data");
      if (!dataNodeArray.isArray()) {
        throw new PidException(UNEXPECTED_ERR);
      }
      for (var dataNode : dataNodeArray) {
        handleNames.put(
            UUID.fromString(dataNode.get("attributes").get("annotationHash").asString()),
            dataNode.get("id").asString());
      }
      return handleNames;
    } catch (NullPointerException _) {
      log.error(UNEXPECTED_LOG, jsonResponse);
      throw new PidException(UNEXPECTED_ERR);
    }
  }

  private Map<String, String> getHandleMapTargetPid(JsonNode jsonResponse)
      throws PidException {
    try {
      var handleNames = new HashMap<String, String>();
      var dataNodeArray = jsonResponse.get("data");
      if (!dataNodeArray.isArray()) {
        throw new PidException(
            "Unexpected response from handle API. Response: " + jsonResponse);
      }
      for (var dataNode : dataNodeArray) {
        handleNames.put(
            dataNode.get("attributes").get("targetPid").asString(),
            dataNode.get("id").asString());
      }
      return handleNames;
    } catch (NullPointerException _) {
      log.error(UNEXPECTED_LOG, jsonResponse);
      throw new PidException(UNEXPECTED_ERR);
    }
  }

}