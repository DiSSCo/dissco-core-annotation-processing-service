package eu.dissco.annotationprocessingservice.properties;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
@ConfigurationProperties("application")
public class ApplicationProperties {

  @NotBlank
  private String processorHandle;

  @NotNull
  private int batchPageSize = 300;

  @NotNull
  private int maxBatchRetries = 10;

  @NotBlank
  private String handleProxy = "https://hdl.handle.net/";

  @NotBlank
  private String createUpdateTombstoneEventType = "https://hdl.handle.net/TEST/123-123-123";

}
