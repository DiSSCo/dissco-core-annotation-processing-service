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
  private String processorHandle = "https://doi.org/10.5281/zenodo.7533428";

  @NotBlank
  private String processorName = "DiSSCo Annotation Processing Service";

  @NotNull
  private int batchPageSize = 300;

  @NotNull
  private int maxBatchRetries = 10;

  @NotBlank
  private String createUpdateTombstoneEventType = "https://doi.org/21.T11148/d7570227982f70256af3";

  @NotNull
  private boolean validateAnnotations = false;

}
