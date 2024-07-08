package eu.dissco.annotationprocessingservice.configuration;

import static eu.dissco.annotationprocessingservice.configuration.ApplicationConfiguration.FORMATTER;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.util.Date;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DateSerializer extends JsonSerializer<Date> {

  @Override
  public void serialize(Date value, JsonGenerator jsonGenerator,
      SerializerProvider serializerProvider) {
    try {
      jsonGenerator.writeString(FORMATTER.format(value.toInstant()));
    } catch (IOException e) {
      log.error("An error has occurred serializing a date. More information: {}", e.getMessage());
    }
  }
}
