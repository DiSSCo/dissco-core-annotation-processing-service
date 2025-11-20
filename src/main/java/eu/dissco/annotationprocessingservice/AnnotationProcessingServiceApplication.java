package eu.dissco.annotationprocessingservice;

import io.github.dissco.annotationlogic.configuration.AnnotationLogicLibraryConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableCaching
@EnableScheduling
@Import({AnnotationLogicLibraryConfiguration.class})
@ConfigurationPropertiesScan
public class AnnotationProcessingServiceApplication {

  public static void main(String[] args) {
    SpringApplication.run(AnnotationProcessingServiceApplication.class, args);
  }

}
