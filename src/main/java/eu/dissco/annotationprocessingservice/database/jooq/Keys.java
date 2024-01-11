/*
 * This file is generated by jOOQ.
 */
package eu.dissco.annotationprocessingservice.database.jooq;


import eu.dissco.annotationprocessingservice.database.jooq.tables.Annotation;
import eu.dissco.annotationprocessingservice.database.jooq.tables.MasJobRecord;
import eu.dissco.annotationprocessingservice.database.jooq.tables.records.AnnotationRecord;
import eu.dissco.annotationprocessingservice.database.jooq.tables.records.MasJobRecordRecord;

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
    public static final UniqueKey<MasJobRecordRecord> MAS_JOB_RECORD_NEW_PK = Internal.createUniqueKey(MasJobRecord.MAS_JOB_RECORD, DSL.name("mas_job_record_new_pk"), new TableField[] { MasJobRecord.MAS_JOB_RECORD.JOB_ID }, true);
}
