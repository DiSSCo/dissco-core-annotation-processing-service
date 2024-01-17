/*
 * This file is generated by jOOQ.
 */
package eu.dissco.annotationprocessingservice.database.jooq;


import eu.dissco.annotationprocessingservice.database.jooq.tables.AnnotationTmp;
import eu.dissco.annotationprocessingservice.database.jooq.tables.MasJobRecordTmp;
import eu.dissco.annotationprocessingservice.database.jooq.tables.records.AnnotationTmpRecord;
import eu.dissco.annotationprocessingservice.database.jooq.tables.records.MasJobRecordTmpRecord;
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

    public static final UniqueKey<AnnotationTmpRecord> ANNOTATION_TMP_PK = Internal.createUniqueKey(AnnotationTmp.ANNOTATION_TMP, DSL.name("annotation_tmp_pk"), new TableField[] { AnnotationTmp.ANNOTATION_TMP.ID }, true);
    public static final UniqueKey<MasJobRecordTmpRecord> MAS_JOB_RECORD_TMP_PK = Internal.createUniqueKey(MasJobRecordTmp.MAS_JOB_RECORD_TMP, DSL.name("mas_job_record_tmp_pk"), new TableField[] { MasJobRecordTmp.MAS_JOB_RECORD_TMP.JOB_ID }, true);
}
