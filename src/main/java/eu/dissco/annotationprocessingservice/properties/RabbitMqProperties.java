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
  private CreateUpdateTombstone createUpdateTombstone = new CreateUpdateTombstone();
  @NotNull
  private AutoAcceptedAnnotation autoAcceptedAnnotation = new AutoAcceptedAnnotation();
  @NotNull
  private MasAnnotation masAnnotation = new MasAnnotation();

  @Data
  @Validated
  public static class CreateUpdateTombstone {

    @NotBlank
    private String exchangeName = "create-update-tombstone-exchange";

    @NotNull
    private String routingKeyName = "create-update-tombstone";
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
