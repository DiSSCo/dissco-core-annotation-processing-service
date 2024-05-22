package eu.dissco.annotationprocessingservice.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AnnotationBatchRecordRepositoryIT extends BaseRepositoryIT {

  private AnnotationBatchRecordRepository repository;

  @BeforeEach
  void setup() {
    repository = new AnnotationBatchRecordRepository(context);
  }

  @Test
  void testCreateAnnotationBatchRecord(){

  }



}
