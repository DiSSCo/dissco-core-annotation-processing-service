package eu.dissco.annotationprocessingservice.component;

import static eu.dissco.annotationprocessingservice.TestUtils.CREATED;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.JOB_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationProcessed;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRequest;
import static eu.dissco.annotationprocessingservice.TestUtils.givenGenerator;
import static eu.dissco.annotationprocessingservice.TestUtils.givenHashedAnnotation;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion.VersionFlag;
import eu.dissco.annotationprocessingservice.domain.AnnotationEvent;
import eu.dissco.annotationprocessingservice.domain.HashedAnnotation;
import eu.dissco.annotationprocessingservice.domain.ProcessResult;
import eu.dissco.annotationprocessingservice.domain.UpdatedAnnotation;
import eu.dissco.annotationprocessingservice.domain.annotation.Annotation;
import eu.dissco.annotationprocessingservice.exception.AnnotationValidationException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SchemaValidatorComponentTest {

  private SchemaValidatorComponent schemaValidator;

  @BeforeEach
  void setup() throws IOException {
    var factory = JsonSchemaFactory.getInstance(VersionFlag.V202012);
    try (InputStream inputStream = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("json-schema/annotation_request.json")) {
      var schema = factory.getSchema(inputStream);
      schemaValidator = new SchemaValidatorComponent(schema, MAPPER);
    }
  }

  @Test
  void testValidateProcessResults() {
    // Given
    var event = new AnnotationEvent(List.of(givenAnnotationRequest()), JOB_ID);

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
        Arguments.of(givenAnnotationRequest().withOdsId(ID), false)
    );
  }

  private static Stream<Arguments> invalidAnnotations() {
    return Stream.of(
        Arguments.of(givenAnnotationRequest().withOaGenerated(CREATED)),
        Arguments.of(givenAnnotationRequest().withAsGenerator(givenGenerator())),
        Arguments.of(givenAnnotationRequest().withOdsId(ID)),
        Arguments.of(givenAnnotationRequest().withOaCreator(null)),
        Arguments.of(givenAnnotationRequest().withDcTermsCreated(null)),
        Arguments.of(givenAnnotationRequest().withRdfType(null)),
        Arguments.of(givenAnnotationRequest().withOaBody(null)),
        Arguments.of(givenAnnotationRequest().withOaTarget(null)),
        Arguments.of(givenAnnotationRequest().withOaMotivation(null))
    );
  }


}
