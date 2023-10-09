package eu.dissco.annotationprocessingservice.service;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.annotationprocessingservice.repository.MasJobRecordRepository;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class MasJobRecordService {

  private final MasJobRecordRepository repository;

  public void updateMasJobRecord(List<JsonNode> annotationBodies, String jobId){



  }

}
