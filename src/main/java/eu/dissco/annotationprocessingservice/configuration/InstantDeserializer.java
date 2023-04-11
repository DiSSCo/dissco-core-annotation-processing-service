package eu.dissco.annotationprocessingservice.configuration;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InstantDeserializer extends JsonDeserializer<Instant> {

  private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(
      "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").withZone(
      ZoneOffset.UTC);

  @Override
  public Instant deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) {
    try {
      return Instant.from(formatter.parse(jsonParser.getText()));
    } catch (IOException e) {
      log.error("An error has occurred deserializing a date. More information: {}", e.getMessage());
      return null;
    }
  }
}
