/*
 * This file is generated by jOOQ.
 */
package eu.dissco.annotationprocessingservice.database.jooq.tables.records;


import eu.dissco.annotationprocessingservice.database.jooq.tables.AnnotationBatchRecord;

import java.time.Instant;
import java.util.UUID;

import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.Record8;
import org.jooq.Row8;
import org.jooq.impl.UpdatableRecordImpl;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class AnnotationBatchRecordRecord extends UpdatableRecordImpl<AnnotationBatchRecordRecord> implements Record8<UUID, String, String, String, Instant, Instant, String, Long> {

    private static final long serialVersionUID = 1L;

    /**
     * Setter for <code>public.annotation_batch_record.batch_id</code>.
     */
    public void setBatchId(UUID value) {
        set(0, value);
    }

    /**
     * Getter for <code>public.annotation_batch_record.batch_id</code>.
     */
    public UUID getBatchId() {
        return (UUID) get(0);
    }

    /**
     * Setter for <code>public.annotation_batch_record.creator_id</code>.
     */
    public void setCreatorId(String value) {
        set(1, value);
    }

    /**
     * Getter for <code>public.annotation_batch_record.creator_id</code>.
     */
    public String getCreatorId() {
        return (String) get(1);
    }

    /**
     * Setter for <code>public.annotation_batch_record.generator_id</code>.
     */
    public void setGeneratorId(String value) {
        set(2, value);
    }

    /**
     * Getter for <code>public.annotation_batch_record.generator_id</code>.
     */
    public String getGeneratorId() {
        return (String) get(2);
    }

    /**
     * Setter for
     * <code>public.annotation_batch_record.parent_annotation_id</code>.
     */
    public void setParentAnnotationId(String value) {
        set(3, value);
    }

    /**
     * Getter for
     * <code>public.annotation_batch_record.parent_annotation_id</code>.
     */
    public String getParentAnnotationId() {
        return (String) get(3);
    }

    /**
     * Setter for <code>public.annotation_batch_record.created_on</code>.
     */
    public void setCreatedOn(Instant value) {
        set(4, value);
    }

    /**
     * Getter for <code>public.annotation_batch_record.created_on</code>.
     */
    public Instant getCreatedOn() {
        return (Instant) get(4);
    }

    /**
     * Setter for <code>public.annotation_batch_record.last_updated</code>.
     */
    public void setLastUpdated(Instant value) {
        set(5, value);
    }

    /**
     * Getter for <code>public.annotation_batch_record.last_updated</code>.
     */
    public Instant getLastUpdated() {
        return (Instant) get(5);
    }

    /**
     * Setter for <code>public.annotation_batch_record.job_id</code>.
     */
    public void setJobId(String value) {
        set(6, value);
    }

    /**
     * Getter for <code>public.annotation_batch_record.job_id</code>.
     */
    public String getJobId() {
        return (String) get(6);
    }

    /**
     * Setter for <code>public.annotation_batch_record.batch_quantity</code>.
     */
    public void setBatchQuantity(Long value) {
        set(7, value);
    }

    /**
     * Getter for <code>public.annotation_batch_record.batch_quantity</code>.
     */
    public Long getBatchQuantity() {
        return (Long) get(7);
    }

    // -------------------------------------------------------------------------
    // Primary key information
    // -------------------------------------------------------------------------

    @Override
    public Record1<UUID> key() {
        return (Record1) super.key();
    }

    // -------------------------------------------------------------------------
    // Record8 type implementation
    // -------------------------------------------------------------------------

    @Override
    public Row8<UUID, String, String, String, Instant, Instant, String, Long> fieldsRow() {
        return (Row8) super.fieldsRow();
    }

    @Override
    public Row8<UUID, String, String, String, Instant, Instant, String, Long> valuesRow() {
        return (Row8) super.valuesRow();
    }

    @Override
    public Field<UUID> field1() {
        return AnnotationBatchRecord.ANNOTATION_BATCH_RECORD.BATCH_ID;
    }

    @Override
    public Field<String> field2() {
        return AnnotationBatchRecord.ANNOTATION_BATCH_RECORD.CREATOR_ID;
    }

    @Override
    public Field<String> field3() {
        return AnnotationBatchRecord.ANNOTATION_BATCH_RECORD.GENERATOR_ID;
    }

    @Override
    public Field<String> field4() {
        return AnnotationBatchRecord.ANNOTATION_BATCH_RECORD.PARENT_ANNOTATION_ID;
    }

    @Override
    public Field<Instant> field5() {
        return AnnotationBatchRecord.ANNOTATION_BATCH_RECORD.CREATED_ON;
    }

    @Override
    public Field<Instant> field6() {
        return AnnotationBatchRecord.ANNOTATION_BATCH_RECORD.LAST_UPDATED;
    }

    @Override
    public Field<String> field7() {
        return AnnotationBatchRecord.ANNOTATION_BATCH_RECORD.JOB_ID;
    }

    @Override
    public Field<Long> field8() {
        return AnnotationBatchRecord.ANNOTATION_BATCH_RECORD.BATCH_QUANTITY;
    }

    @Override
    public UUID component1() {
        return getBatchId();
    }

    @Override
    public String component2() {
        return getCreatorId();
    }

    @Override
    public String component3() {
        return getGeneratorId();
    }

    @Override
    public String component4() {
        return getParentAnnotationId();
    }

    @Override
    public Instant component5() {
        return getCreatedOn();
    }

    @Override
    public Instant component6() {
        return getLastUpdated();
    }

    @Override
    public String component7() {
        return getJobId();
    }

    @Override
    public Long component8() {
        return getBatchQuantity();
    }

    @Override
    public UUID value1() {
        return getBatchId();
    }

    @Override
    public String value2() {
        return getCreatorId();
    }

    @Override
    public String value3() {
        return getGeneratorId();
    }

    @Override
    public String value4() {
        return getParentAnnotationId();
    }

    @Override
    public Instant value5() {
        return getCreatedOn();
    }

    @Override
    public Instant value6() {
        return getLastUpdated();
    }

    @Override
    public String value7() {
        return getJobId();
    }

    @Override
    public Long value8() {
        return getBatchQuantity();
    }

    @Override
    public AnnotationBatchRecordRecord value1(UUID value) {
        setBatchId(value);
        return this;
    }

    @Override
    public AnnotationBatchRecordRecord value2(String value) {
        setCreatorId(value);
        return this;
    }

    @Override
    public AnnotationBatchRecordRecord value3(String value) {
        setGeneratorId(value);
        return this;
    }

    @Override
    public AnnotationBatchRecordRecord value4(String value) {
        setParentAnnotationId(value);
        return this;
    }

    @Override
    public AnnotationBatchRecordRecord value5(Instant value) {
        setCreatedOn(value);
        return this;
    }

    @Override
    public AnnotationBatchRecordRecord value6(Instant value) {
        setLastUpdated(value);
        return this;
    }

    @Override
    public AnnotationBatchRecordRecord value7(String value) {
        setJobId(value);
        return this;
    }

    @Override
    public AnnotationBatchRecordRecord value8(Long value) {
        setBatchQuantity(value);
        return this;
    }

    @Override
    public AnnotationBatchRecordRecord values(UUID value1, String value2, String value3, String value4, Instant value5, Instant value6, String value7, Long value8) {
        value1(value1);
        value2(value2);
        value3(value3);
        value4(value4);
        value5(value5);
        value6(value6);
        value7(value7);
        value8(value8);
        return this;
    }

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /**
     * Create a detached AnnotationBatchRecordRecord
     */
    public AnnotationBatchRecordRecord() {
        super(AnnotationBatchRecord.ANNOTATION_BATCH_RECORD);
    }

    /**
     * Create a detached, initialised AnnotationBatchRecordRecord
     */
    public AnnotationBatchRecordRecord(UUID batchId, String creatorId, String generatorId, String parentAnnotationId, Instant createdOn, Instant lastUpdated, String jobId, Long batchQuantity) {
        super(AnnotationBatchRecord.ANNOTATION_BATCH_RECORD);

        setBatchId(batchId);
        setCreatorId(creatorId);
        setGeneratorId(generatorId);
        setParentAnnotationId(parentAnnotationId);
        setCreatedOn(createdOn);
        setLastUpdated(lastUpdated);
        setJobId(jobId);
        setBatchQuantity(batchQuantity);
        resetChangedOnNotNull();
    }
}
