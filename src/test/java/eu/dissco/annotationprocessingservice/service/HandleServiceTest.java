package eu.dissco.annotationprocessingservice.service;

import static eu.dissco.annotationprocessingservice.TestUtils.CREATED;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.MAPPER;
import static eu.dissco.annotationprocessingservice.TestUtils.givenAnnotationRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mockStatic;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.annotationprocessingservice.domain.HandleAttribute;
import eu.dissco.annotationprocessingservice.repository.HandleRepository;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Random;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class HandleServiceTest {

  @Mock
  private Random random;
  @Mock
  private HandleRepository repository;
  private MockedStatic<Instant> mockedStatic;

  private HandleService service;

  @BeforeEach
  void setup() throws ParserConfigurationException {
    var docFactory = DocumentBuilderFactory.newInstance();
    var transfactory = TransformerFactory.newInstance();
    service = new HandleService(random, MAPPER, docFactory.newDocumentBuilder(), repository,
        transfactory);
    Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
    Instant instant = Instant.now(clock);
    mockedStatic = mockStatic(Instant.class);
    mockedStatic.when(Instant::now).thenReturn(instant);
  }

  @AfterEach
  void destroy() {
    mockedStatic.close();
  }

  @Test
  void testCreateNewHandle() throws TransformerException, JsonProcessingException {
    // Given
    given(random.nextInt(33)).willReturn(21);
    var expected = "20.5000.1025/YYY-YYY-YYY";

    // When
    var result = service.createNewHandle(givenAnnotationRecord().annotation());

    // Then
    then(repository).should().createHandle(eq(expected), eq(CREATED), anyList());
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testUpdateHandle() throws JsonProcessingException {
    // Given

    // When
    service.updateHandle(ID, givenAnnotationRecord().annotation());

    // Then
    then(repository).should().updateHandleAttributes(eq(ID), eq(CREATED), anyList(), eq(true));
  }

  @Test
  void testRollbackHandleCreation() throws JsonProcessingException {
    // Given

    // When
    service.rollbackHandleCreation(givenAnnotationRecord());

    // Then
    then(repository).should().rollbackHandleCreation(ID);
  }

  @Test
  void testDeleteVersion() throws JsonProcessingException {
    // Given

    // When
    service.deleteVersion(givenAnnotationRecord());

    // Then
    then(repository).should().updateHandleAttributes(eq(ID), eq(CREATED), anyList(), eq(false));
  }

  @Test
  void testArchiveRecord() {
    // Given

    // When
    service.archiveRecord(ID, "XXX-XX");

    // Then
    then(repository).should().archiveAnnotation(eq(ID), eq(CREATED), any(HandleAttribute.class),
        any(HandleAttribute.class));
  }
}
