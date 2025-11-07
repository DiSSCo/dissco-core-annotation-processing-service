package eu.dissco.annotationprocessingservice.configuration;

import com.jayway.jsonpath.Option;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class JsonPathComponentConfiguration {

  @Primary
  @Bean("localJsonPath")
  public com.jayway.jsonpath.Configuration localJsonPathConfiguration() {
    return com.jayway.jsonpath.Configuration.builder()
        .options(Option.AS_PATH_LIST, Option.SUPPRESS_EXCEPTIONS, Option.ALWAYS_RETURN_LIST)
        .build();
  }

}
