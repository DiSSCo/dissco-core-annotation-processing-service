package eu.dissco.annotationprocessingservice.domain;

import eu.dissco.annotationprocessingservice.database.jooq.enums.ErrorCode;

public record MasJobRecord(
    String jobId,
    boolean batchingRequested,
    ErrorCode error
) {

}
