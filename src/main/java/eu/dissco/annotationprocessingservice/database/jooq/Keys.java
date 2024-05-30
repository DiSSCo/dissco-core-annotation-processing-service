/*
 * This file is generated by jOOQ.
 */
package eu.dissco.annotationprocessingservice.database.jooq;


import eu.dissco.annotationprocessingservice.database.jooq.tables.Annotation;
import eu.dissco.annotationprocessingservice.database.jooq.tables.AnnotationBatchRecord;
import eu.dissco.annotationprocessingservice.database.jooq.tables.MasJobRecord;
import eu.dissco.annotationprocessingservice.database.jooq.tables.records.AnnotationBatchRecordRecord;
import eu.dissco.annotationprocessingservice.database.jooq.tables.records.AnnotationRecord;
import eu.dissco.annotationprocessingservice.database.jooq.tables.records.MasJobRecordRecord;

import org.jooq.ForeignKey;
import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.impl.DSL;
import org.jooq.impl.Internal;


/**
 * A class modelling foreign key relationships and constraints of tables in
 * public.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class Keys {

    // -------------------------------------------------------------------------
    // UNIQUE and PRIMARY KEY definitions
    // -------------------------------------------------------------------------

    public static final UniqueKey<AnnotationRecord> ANNOTATION_PK = Internal.createUniqueKey(Annotation.ANNOTATION, DSL.name("annotation_pk"), new TableField[] { Annotation.ANNOTATION.ID }, true);
    public static final UniqueKey<AnnotationBatchRecordRecord> ANNOTATION_BATCH_PK = Internal.createUniqueKey(AnnotationBatchRecord.ANNOTATION_BATCH_RECORD, DSL.name("annotation_batch_pk"), new TableField[] { AnnotationBatchRecord.ANNOTATION_BATCH_RECORD.BATCH_ID }, true);
    public static final UniqueKey<MasJobRecordRecord> MAS_JOB_RECORD_PK = Internal.createUniqueKey(MasJobRecord.MAS_JOB_RECORD, DSL.name("mas_job_record_pk"), new TableField[] { MasJobRecord.MAS_JOB_RECORD.JOB_ID }, true);

    // -------------------------------------------------------------------------
    // FOREIGN KEY definitions
    // -------------------------------------------------------------------------

    public static final ForeignKey<AnnotationRecord, AnnotationBatchRecordRecord> ANNOTATION__ANNOTATION_BATCH_ID_FK = Internal.createForeignKey(Annotation.ANNOTATION, DSL.name("annotation_batch_id_fk"), new TableField[] { Annotation.ANNOTATION.BATCH_ID }, Keys.ANNOTATION_BATCH_PK, new TableField[] { AnnotationBatchRecord.ANNOTATION_BATCH_RECORD.BATCH_ID }, true);
}
