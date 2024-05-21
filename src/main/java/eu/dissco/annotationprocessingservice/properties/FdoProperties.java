package eu.dissco.annotationprocessingservice.properties;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
@ConfigurationProperties("fdo")
public class FdoProperties {
  @NotBlank
  private String type = "https://hdl.handle.net/21.T11148/cf458ca9ee1d44a5608f";

  @NotBlank
  private String issuedForAgent = "https://ror.org/0566bfb96";

}
