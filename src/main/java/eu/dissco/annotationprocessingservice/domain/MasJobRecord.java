package eu.dissco.annotationprocessingservice.domain;

import eu.dissco.annotationprocessingservice.database.jooq.enums.ErrorCode;

public record MasJobRecord(
    boolean batchingRequested,
    ErrorCode error
) {

}
