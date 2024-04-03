/*
 * This file is generated by jOOQ.
 */
package eu.dissco.annotationprocessingservice.database.jooq.tables.records;


import eu.dissco.annotationprocessingservice.database.jooq.enums.MjrJobState;
import eu.dissco.annotationprocessingservice.database.jooq.enums.MjrTargetType;
import eu.dissco.annotationprocessingservice.database.jooq.tables.MasJobRecord;

import java.time.Instant;

import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Record1;
import org.jooq.Record12;
import org.jooq.Row12;
import org.jooq.impl.UpdatableRecordImpl;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class MasJobRecordRecord extends UpdatableRecordImpl<MasJobRecordRecord> implements Record12<String, MjrJobState, String, Instant, Instant, JSONB, String, String, MjrTargetType, Boolean, Object, Instant> {

    private static final long serialVersionUID = 1L;

    /**
     * Setter for <code>public.mas_job_record.job_id</code>.
     */
    public void setJobId(String value) {
        set(0, value);
    }

    /**
     * Getter for <code>public.mas_job_record.job_id</code>.
     */
    public String getJobId() {
        return (String) get(0);
    }

    /**
     * Setter for <code>public.mas_job_record.job_state</code>.
     */
    public void setJobState(MjrJobState value) {
        set(1, value);
    }

    /**
     * Getter for <code>public.mas_job_record.job_state</code>.
     */
    public MjrJobState getJobState() {
        return (MjrJobState) get(1);
    }

    /**
     * Setter for <code>public.mas_job_record.mas_id</code>.
     */
    public void setMasId(String value) {
        set(2, value);
    }

    /**
     * Getter for <code>public.mas_job_record.mas_id</code>.
     */
    public String getMasId() {
        return (String) get(2);
    }

    /**
     * Setter for <code>public.mas_job_record.time_started</code>.
     */
    public void setTimeStarted(Instant value) {
        set(3, value);
    }

    /**
     * Getter for <code>public.mas_job_record.time_started</code>.
     */
    public Instant getTimeStarted() {
        return (Instant) get(3);
    }

    /**
     * Setter for <code>public.mas_job_record.time_completed</code>.
     */
    public void setTimeCompleted(Instant value) {
        set(4, value);
    }

    /**
     * Getter for <code>public.mas_job_record.time_completed</code>.
     */
    public Instant getTimeCompleted() {
        return (Instant) get(4);
    }

    /**
     * Setter for <code>public.mas_job_record.annotations</code>.
     */
    public void setAnnotations(JSONB value) {
        set(5, value);
    }

    /**
     * Getter for <code>public.mas_job_record.annotations</code>.
     */
    public JSONB getAnnotations() {
        return (JSONB) get(5);
    }

    /**
     * Setter for <code>public.mas_job_record.target_id</code>.
     */
    public void setTargetId(String value) {
        set(6, value);
    }

    /**
     * Getter for <code>public.mas_job_record.target_id</code>.
     */
    public String getTargetId() {
        return (String) get(6);
    }

    /**
     * Setter for <code>public.mas_job_record.user_id</code>.
     */
    public void setUserId(String value) {
        set(7, value);
    }

    /**
     * Getter for <code>public.mas_job_record.user_id</code>.
     */
    public String getUserId() {
        return (String) get(7);
    }

    /**
     * Setter for <code>public.mas_job_record.target_type</code>.
     */
    public void setTargetType(MjrTargetType value) {
        set(8, value);
    }

    /**
     * Getter for <code>public.mas_job_record.target_type</code>.
     */
    public MjrTargetType getTargetType() {
        return (MjrTargetType) get(8);
    }

    /**
     * Setter for <code>public.mas_job_record.batching_requested</code>.
     */
    public void setBatchingRequested(Boolean value) {
        set(9, value);
    }

    /**
     * Getter for <code>public.mas_job_record.batching_requested</code>.
     */
    public Boolean getBatchingRequested() {
        return (Boolean) get(9);
    }

    /**
     * Setter for <code>public.mas_job_record.error</code>.
     */
    public void setError(Object value) {
        set(10, value);
    }

    /**
     * Getter for <code>public.mas_job_record.error</code>.
     */
    public Object getError() {
        return get(10);
    }

    /**
     * Setter for <code>public.mas_job_record.time_to_live</code>.
     */
    public void setTimeToLive(Instant value) {
        set(11, value);
    }

    /**
     * Getter for <code>public.mas_job_record.time_to_live</code>.
     */
    public Instant getTimeToLive() {
        return (Instant) get(11);
    }

    // -------------------------------------------------------------------------
    // Primary key information
    // -------------------------------------------------------------------------

    @Override
    public Record1<String> key() {
        return (Record1) super.key();
    }

    // -------------------------------------------------------------------------
    // Record12 type implementation
    // -------------------------------------------------------------------------

    @Override
    public Row12<String, MjrJobState, String, Instant, Instant, JSONB, String, String, MjrTargetType, Boolean, Object, Instant> fieldsRow() {
        return (Row12) super.fieldsRow();
    }

    @Override
    public Row12<String, MjrJobState, String, Instant, Instant, JSONB, String, String, MjrTargetType, Boolean, Object, Instant> valuesRow() {
        return (Row12) super.valuesRow();
    }

    @Override
    public Field<String> field1() {
        return MasJobRecord.MAS_JOB_RECORD.JOB_ID;
    }

    @Override
    public Field<MjrJobState> field2() {
        return MasJobRecord.MAS_JOB_RECORD.JOB_STATE;
    }

    @Override
    public Field<String> field3() {
        return MasJobRecord.MAS_JOB_RECORD.MAS_ID;
    }

    @Override
    public Field<Instant> field4() {
        return MasJobRecord.MAS_JOB_RECORD.TIME_STARTED;
    }

    @Override
    public Field<Instant> field5() {
        return MasJobRecord.MAS_JOB_RECORD.TIME_COMPLETED;
    }

    @Override
    public Field<JSONB> field6() {
        return MasJobRecord.MAS_JOB_RECORD.ANNOTATIONS;
    }

    @Override
    public Field<String> field7() {
        return MasJobRecord.MAS_JOB_RECORD.TARGET_ID;
    }

    @Override
    public Field<String> field8() {
        return MasJobRecord.MAS_JOB_RECORD.USER_ID;
    }

    @Override
    public Field<MjrTargetType> field9() {
        return MasJobRecord.MAS_JOB_RECORD.TARGET_TYPE;
    }

    @Override
    public Field<Boolean> field10() {
        return MasJobRecord.MAS_JOB_RECORD.BATCHING_REQUESTED;
    }

    @Override
    public Field<Object> field11() {
        return MasJobRecord.MAS_JOB_RECORD.ERROR;
    }

    @Override
    public Field<Instant> field12() {
        return MasJobRecord.MAS_JOB_RECORD.TIME_TO_LIVE;
    }

    @Override
    public String component1() {
        return getJobId();
    }

    @Override
    public MjrJobState component2() {
        return getJobState();
    }

    @Override
    public String component3() {
        return getMasId();
    }

    @Override
    public Instant component4() {
        return getTimeStarted();
    }

    @Override
    public Instant component5() {
        return getTimeCompleted();
    }

    @Override
    public JSONB component6() {
        return getAnnotations();
    }

    @Override
    public String component7() {
        return getTargetId();
    }

    @Override
    public String component8() {
        return getUserId();
    }

    @Override
    public MjrTargetType component9() {
        return getTargetType();
    }

    @Override
    public Boolean component10() {
        return getBatchingRequested();
    }

    @Override
    public Object component11() {
        return getError();
    }

    @Override
    public Instant component12() {
        return getTimeToLive();
    }

    @Override
    public String value1() {
        return getJobId();
    }

    @Override
    public MjrJobState value2() {
        return getJobState();
    }

    @Override
    public String value3() {
        return getMasId();
    }

    @Override
    public Instant value4() {
        return getTimeStarted();
    }

    @Override
    public Instant value5() {
        return getTimeCompleted();
    }

    @Override
    public JSONB value6() {
        return getAnnotations();
    }

    @Override
    public String value7() {
        return getTargetId();
    }

    @Override
    public String value8() {
        return getUserId();
    }

    @Override
    public MjrTargetType value9() {
        return getTargetType();
    }

    @Override
    public Boolean value10() {
        return getBatchingRequested();
    }

    @Override
    public Object value11() {
        return getError();
    }

    @Override
    public Instant value12() {
        return getTimeToLive();
    }

    @Override
    public MasJobRecordRecord value1(String value) {
        setJobId(value);
        return this;
    }

    @Override
    public MasJobRecordRecord value2(MjrJobState value) {
        setJobState(value);
        return this;
    }

    @Override
    public MasJobRecordRecord value3(String value) {
        setMasId(value);
        return this;
    }

    @Override
    public MasJobRecordRecord value4(Instant value) {
        setTimeStarted(value);
        return this;
    }

    @Override
    public MasJobRecordRecord value5(Instant value) {
        setTimeCompleted(value);
        return this;
    }

    @Override
    public MasJobRecordRecord value6(JSONB value) {
        setAnnotations(value);
        return this;
    }

    @Override
    public MasJobRecordRecord value7(String value) {
        setTargetId(value);
        return this;
    }

    @Override
    public MasJobRecordRecord value8(String value) {
        setUserId(value);
        return this;
    }

    @Override
    public MasJobRecordRecord value9(MjrTargetType value) {
        setTargetType(value);
        return this;
    }

    @Override
    public MasJobRecordRecord value10(Boolean value) {
        setBatchingRequested(value);
        return this;
    }

    @Override
    public MasJobRecordRecord value11(Object value) {
        setError(value);
        return this;
    }

    @Override
    public MasJobRecordRecord value12(Instant value) {
        setTimeToLive(value);
        return this;
    }

    @Override
    public MasJobRecordRecord values(String value1, MjrJobState value2, String value3, Instant value4, Instant value5, JSONB value6, String value7, String value8, MjrTargetType value9, Boolean value10, Object value11, Instant value12) {
        value1(value1);
        value2(value2);
        value3(value3);
        value4(value4);
        value5(value5);
        value6(value6);
        value7(value7);
        value8(value8);
        value9(value9);
        value10(value10);
        value11(value11);
        value12(value12);
        return this;
    }

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /**
     * Create a detached MasJobRecordRecord
     */
    public MasJobRecordRecord() {
        super(MasJobRecord.MAS_JOB_RECORD);
    }

    /**
     * Create a detached, initialised MasJobRecordRecord
     */
    public MasJobRecordRecord(String jobId, MjrJobState jobState, String masId, Instant timeStarted, Instant timeCompleted, JSONB annotations, String targetId, String userId, MjrTargetType targetType, Boolean batchingRequested, Object error, Instant timeToLive) {
        super(MasJobRecord.MAS_JOB_RECORD);

        setJobId(jobId);
        setJobState(jobState);
        setMasId(masId);
        setTimeStarted(timeStarted);
        setTimeCompleted(timeCompleted);
        setAnnotations(annotations);
        setTargetId(targetId);
        setUserId(userId);
        setTargetType(targetType);
        setBatchingRequested(batchingRequested);
        setError(error);
        setTimeToLive(timeToLive);
        resetChangedOnNotNull();
    }
}
