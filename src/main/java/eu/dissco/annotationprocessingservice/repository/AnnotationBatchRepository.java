package eu.dissco.annotationprocessingservice.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

@Slf4j
@RequiredArgsConstructor
@Repository
public class AnnotationBatchRepository {

  private final ObjectMapper mapper;
  private final DSLContext context;



}
