package eu.dissco.annotationprocessingservice.service;


import static eu.dissco.annotationprocessingservice.TestUtils.ANNOTATION_JSONB;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.ID_ALT;
import static eu.dissco.annotationprocessingservice.TestUtils.JOB_ID;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationEvent;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;

import eu.dissco.annotationprocessingservice.Profiles;
import eu.dissco.annotationprocessingservice.database.jooq.enums.ErrorCode;
import eu.dissco.annotationprocessingservice.domain.MasJobRecord;
import eu.dissco.annotationprocessingservice.exception.FailedProcessingException;
import eu.dissco.annotationprocessingservice.exception.UnsupportedOperationException;
import eu.dissco.annotationprocessingservice.repository.MasJobRecordRepository;
import eu.dissco.annotationprocessingservice.schema.AnnotationProcessingEvent;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.env.Environment;

@ExtendWith(MockitoExtension.class)
class MasJobRecordServiceTest {

  @Mock
  private MasJobRecordRepository repository;
  @Mock
  private Environment environment;
  private MasJobRecordService service;

  @BeforeEach
  void setup() {
    service = new MasJobRecordService(repository, environment, MAPPER);
  }

  @Test
  void testVerifyMasJobIdKafka() {
    // Given
    given(environment.matchesProfiles(Profiles.RABBIT_MQ_MAS, Profiles.RABBIT_MQ_AUTO)).willReturn(
        true);

    // When
    assertDoesNotThrow(() -> service.verifyMasJobId(givenAnnotationEvent()));
  }

  @Test
  void testMarkEmptyMasJobRecordAsComplete() {
    // When
    service.markEmptyMasJobRecordAsComplete(JOB_ID, false);

    // Then
    then(repository).should().markMasJobRecordAsComplete(JOB_ID, MAPPER.createObjectNode());
  }

  @Test
  void testVerifyMasJobIdWeb() {
    // Given
    var annotationEvent = givenAnnotationEvent();
    given(environment.matchesProfiles(Profiles.RABBIT_MQ_MAS, Profiles.RABBIT_MQ_AUTO)).willReturn(
        false);

    // When
    assertThrows(UnsupportedOperationException.class,
        () -> service.verifyMasJobId(annotationEvent));
  }

  @Test
  void testVerifyMasJobIdKafkaFails() {
    // Given
    given(environment.matchesProfiles(Profiles.RABBIT_MQ_MAS, Profiles.RABBIT_MQ_AUTO)).willReturn(
        true);

    // When
    assertThrows(FailedProcessingException.class,
        () -> service.verifyMasJobId(
            new AnnotationProcessingEvent(null, List.of(givenAnnotationRequest()), null,
                null)));
  }

  @Test
  void testMarkMasJobRecordAsComplete() throws Exception {
    // Given
    var expectedNode = MAPPER.readTree(ANNOTATION_JSONB);

    // When
    service.markMasJobRecordAsComplete(JOB_ID, List.of(ID), false);

    // Then
    then(repository).should().markMasJobRecordAsComplete(JOB_ID, expectedNode);
  }

  @Test
  void testMarkMasJobRecordAsFailed() {
    // When
    service.markMasJobRecordAsFailed(JOB_ID, false, ErrorCode.DISSCO_EXCEPTION, null);

    // Then
    then(repository).should().markMasJobRecordAsFailed(JOB_ID, ErrorCode.DISSCO_EXCEPTION, null);
  }

  @Test
  void testMarkMasJobRecordAsCompletedBatchResult() {
    // Given
    // When
    service.markMasJobRecordAsComplete(JOB_ID, List.of(ID_ALT), true);

    // Then
    then(repository).shouldHaveNoInteractions();
  }

  @Test
  void testMarkMasJobRecordAsFailedBatchResult() {
    // When
    service.markMasJobRecordAsFailed(JOB_ID, true, ErrorCode.DISSCO_EXCEPTION, null);

    // Then
    then(repository).shouldHaveNoInteractions();
  }

  @Test
  void testMarkEmptyMasJobRecordAsCompletedBatchResult() {
    // When
    service.markEmptyMasJobRecordAsComplete(JOB_ID, true);

    // Then
    then(repository).shouldHaveNoInteractions();
  }

  @Test
  void testGetMasJobRecord() {
    // Given
    var expected = new MasJobRecord(JOB_ID, true, null);
    given(repository.getMasJobRecord(JOB_ID)).willReturn(expected);

    // When
    var result = repository.getMasJobRecord(JOB_ID);

    // Then
    assertThat(result).isEqualTo(expected);
  }

}
