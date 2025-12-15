package eu.dissco.annotationprocessingservice.properties;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
@ConfigurationProperties(prefix = "rabbitmq")
public class RabbitMqProperties {

  @Positive
  private int batchSize = 500;
  @NotNull
  private Provenance provenance = new Provenance();
  @NotNull
  private AutoAcceptedAnnotation autoAcceptedAnnotation = new AutoAcceptedAnnotation();
  @NotNull
  private MasAnnotation masAnnotation = new MasAnnotation();

  @Data
  @Validated
  public static class Provenance {

    @NotBlank
    private String exchangeName = "provenance-exchange";

    @NotNull
    private String routingKeyName = "provenance";
  }

  @Data
  @Validated
  public static class AutoAcceptedAnnotation {

    @NotBlank
    private String dlqExchangeName = "auto-accepted-annotation-exchange-dlq";

    @NotNull
    private String dlqRoutingKeyName = "auto-accepted-annotation-dlq";
  }

  @Data
  @Validated
  public static class MasAnnotation {

    @NotBlank
    private String exchangeName = "mas-annotation-exchange";

    @NotNull
    private String routingKeyName = "mas-annotation";
  }

}
