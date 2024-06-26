package eu.dissco.annotationprocessingservice.component;

import static eu.dissco.annotationprocessingservice.TestUtils.CREATED;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.JOB_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRequest;
import static eu.dissco.annotationprocessingservice.TestUtils.givenGenerator;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion.VersionFlag;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.env.Environment;

@ExtendWith(MockitoExtension.class)
class SchemaValidatorComponentTest {

  private SchemaValidatorComponent schemaValidator;
  @Mock
  private Environment env;

  @BeforeEach
  void setup() throws IOException {
    var factory = JsonSchemaFactory.getInstance(VersionFlag.V202012);
    try (InputStream inputStream = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("json-schema/annotation_request.json")) {
      var schema = factory.getSchema(inputStream);
      schemaValidator = new SchemaValidatorComponent(schema, MAPPER, env);
    }
  }

  @Test
  void testValidateProcessResults() {
    // Given
    var event = new AnnotationEvent(List.of(givenAnnotationRequest()), JOB_ID, null, null);

    // Then
    assertDoesNotThrow(() -> schemaValidator.validateEvent(event));
  }

  @ParameterizedTest
  @MethodSource("invalidAnnotations")
  void testInvalidAnnotations(Annotation annotationRequest) {
    // Then
    assertThrows(AnnotationValidationException.class,
        () -> schemaValidator.validateAnnotationRequest(annotationRequest,
            true));

  }

  @Test
  void testUpdateMissingId() {
    // Given
    var annotationRequest = givenAnnotationRequest();

    // Then
    assertThrows(AnnotationValidationException.class,
        () -> schemaValidator.validateAnnotationRequest(annotationRequest,
            false));
  }

  @Test
  void testCreateMissingCreated() {
    // Given
    var annotationRequest = givenAnnotationRequest().setDcTermsCreated(null);

    // Then
    assertThrows(AnnotationValidationException.class,
        () -> schemaValidator.validateAnnotationRequest(annotationRequest,
            false));
  }

  @ParameterizedTest
  @MethodSource("validAnnotations")
  void testValidAnnotation(Annotation annotationRequest, Boolean isNew) {

    // Then
    assertDoesNotThrow(() ->
        schemaValidator.validateAnnotationRequest(annotationRequest, isNew));
  }

  private static Stream<Arguments> validAnnotations() {
    return Stream.of(
        Arguments.of(givenAnnotationRequest(), true),
        Arguments.of(givenAnnotationRequest().setOdsId(ID), false)
    );
  }

  private static Stream<Arguments> invalidAnnotations() {
    return Stream.of(
        Arguments.of(givenAnnotationRequest().setOaGenerated(CREATED)),
        Arguments.of(givenAnnotationRequest().setAsGenerator(givenGenerator())),
        Arguments.of(givenAnnotationRequest().setOdsId(ID)),
        Arguments.of(givenAnnotationRequest().setOaCreator(null)),
        Arguments.of(givenAnnotationRequest().setDcTermsCreated(null)),
        Arguments.of(givenAnnotationRequest().setRdfType(null)),
        Arguments.of(givenAnnotationRequest().setOaBody(null)),
        Arguments.of(givenAnnotationRequest().setOaTarget(null)),
        Arguments.of(givenAnnotationRequest().setOaMotivation(null))
    );
  }


}
