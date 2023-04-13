package eu.dissco.annotationprocessingservice.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ApplicationConfiguration {

  public static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern(
      "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").withZone(ZoneOffset.UTC);

  @Bean
  public ObjectMapper objectMapper() {
    var mapper = new ObjectMapper().findAndRegisterModules();
    SimpleModule dateModule = new SimpleModule();
    dateModule.addSerializer(Instant.class, new InstantSerializer());
    dateModule.addDeserializer(Instant.class, new InstantDeserializer());
    mapper.registerModule(dateModule);
    return mapper;
  }

  @Bean
  public Random random() {
    return new Random();
  }

  @Bean
  public DocumentBuilder documentBuilder() throws ParserConfigurationException {
    var docFactory = DocumentBuilderFactory.newInstance();
    return docFactory.newDocumentBuilder();
  }

}
