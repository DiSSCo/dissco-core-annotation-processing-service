package eu.dissco.annotationprocessingservice.configuration;

import static eu.dissco.annotationprocessingservice.configuration.ApplicationConfiguration.FORMATTER;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DateDeserializer extends JsonDeserializer<Date> {

  @Override
  public Date deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) {
    try {
      return Date.from(Instant.from(FORMATTER.parse(jsonParser.getText())));
    } catch (IOException e) {
      log.error("An error has occurred deserializing a date. More information: {}", e.getMessage());
      return null;
    }
  }
}
