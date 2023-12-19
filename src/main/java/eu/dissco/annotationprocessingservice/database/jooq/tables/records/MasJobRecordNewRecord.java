/*
 * This file is generated by jOOQ.
 */
package eu.dissco.annotationprocessingservice.database.jooq.tables.records;


import eu.dissco.annotationprocessingservice.database.jooq.tables.MasJobRecordNew;
import eu.dissco.annotationprocessingservice.domain.MasJobState;

import java.time.Instant;

import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Record1;
import org.jooq.Record8;
import org.jooq.Row8;
import org.jooq.impl.UpdatableRecordImpl;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class MasJobRecordNewRecord extends UpdatableRecordImpl<MasJobRecordNewRecord> implements Record8<String, MasJobState, String, Instant, Instant, JSONB, String, String> {

    private static final long serialVersionUID = 1L;

    /**
     * Setter for <code>public.mas_job_record_new.job_id</code>.
     */
    public void setJobId(String value) {
        set(0, value);
    }

    /**
     * Getter for <code>public.mas_job_record_new.job_id</code>.
     */
    public String getJobId() {
        return (String) get(0);
    }

    /**
     * Setter for <code>public.mas_job_record_new.job_state</code>.
     */
    public void setJobState(MasJobState value) {
        set(1, value);
    }

    /**
     * Getter for <code>public.mas_job_record_new.job_state</code>.
     */
    public MasJobState getJobState() {
        return (MasJobState) get(1);
    }

    /**
     * Setter for <code>public.mas_job_record_new.mas_id</code>.
     */
    public void setMasId(String value) {
        set(2, value);
    }

    /**
     * Getter for <code>public.mas_job_record_new.mas_id</code>.
     */
    public String getMasId() {
        return (String) get(2);
    }

    /**
     * Setter for <code>public.mas_job_record_new.time_started</code>.
     */
    public void setTimeStarted(Instant value) {
        set(3, value);
    }

    /**
     * Getter for <code>public.mas_job_record_new.time_started</code>.
     */
    public Instant getTimeStarted() {
        return (Instant) get(3);
    }

    /**
     * Setter for <code>public.mas_job_record_new.time_completed</code>.
     */
    public void setTimeCompleted(Instant value) {
        set(4, value);
    }

    /**
     * Getter for <code>public.mas_job_record_new.time_completed</code>.
     */
    public Instant getTimeCompleted() {
        return (Instant) get(4);
    }

    /**
     * Setter for <code>public.mas_job_record_new.annotations</code>.
     */
    public void setAnnotations(JSONB value) {
        set(5, value);
    }

    /**
     * Getter for <code>public.mas_job_record_new.annotations</code>.
     */
    public JSONB getAnnotations() {
        return (JSONB) get(5);
    }

    /**
     * Setter for <code>public.mas_job_record_new.target_id</code>.
     */
    public void setTargetId(String value) {
        set(6, value);
    }

    /**
     * Getter for <code>public.mas_job_record_new.target_id</code>.
     */
    public String getTargetId() {
        return (String) get(6);
    }

    /**
     * Setter for <code>public.mas_job_record_new.user_id</code>. User
     * scheduling MAS
     */
    public void setUserId(String value) {
        set(7, value);
    }

    /**
     * Getter for <code>public.mas_job_record_new.user_id</code>. User
     * scheduling MAS
     */
    public String getUserId() {
        return (String) get(7);
    }

    // -------------------------------------------------------------------------
    // Primary key information
    // -------------------------------------------------------------------------

    @Override
    public Record1<String> key() {
        return (Record1) super.key();
    }

    // -------------------------------------------------------------------------
    // Record8 type implementation
    // -------------------------------------------------------------------------

    @Override
    public Row8<String, MasJobState, String, Instant, Instant, JSONB, String, String> fieldsRow() {
        return (Row8) super.fieldsRow();
    }

    @Override
    public Row8<String, MasJobState, String, Instant, Instant, JSONB, String, String> valuesRow() {
        return (Row8) super.valuesRow();
    }

    @Override
    public Field<String> field1() {
        return MasJobRecordNew.MAS_JOB_RECORD_NEW.JOB_ID;
    }

    @Override
    public Field<MasJobState> field2() {
        return MasJobRecordNew.MAS_JOB_RECORD_NEW.JOB_STATE;
    }

    @Override
    public Field<String> field3() {
        return MasJobRecordNew.MAS_JOB_RECORD_NEW.MAS_ID;
    }

    @Override
    public Field<Instant> field4() {
        return MasJobRecordNew.MAS_JOB_RECORD_NEW.TIME_STARTED;
    }

    @Override
    public Field<Instant> field5() {
        return MasJobRecordNew.MAS_JOB_RECORD_NEW.TIME_COMPLETED;
    }

    @Override
    public Field<JSONB> field6() {
        return MasJobRecordNew.MAS_JOB_RECORD_NEW.ANNOTATIONS;
    }

    @Override
    public Field<String> field7() {
        return MasJobRecordNew.MAS_JOB_RECORD_NEW.TARGET_ID;
    }

    @Override
    public Field<String> field8() {
        return MasJobRecordNew.MAS_JOB_RECORD_NEW.USER_ID;
    }

    @Override
    public String component1() {
        return getJobId();
    }

    @Override
    public MasJobState component2() {
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
    public String value1() {
        return getJobId();
    }

    @Override
    public MasJobState value2() {
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
    public MasJobRecordNewRecord value1(String value) {
        setJobId(value);
        return this;
    }

    @Override
    public MasJobRecordNewRecord value2(MasJobState value) {
        setJobState(value);
        return this;
    }

    @Override
    public MasJobRecordNewRecord value3(String value) {
        setMasId(value);
        return this;
    }

    @Override
    public MasJobRecordNewRecord value4(Instant value) {
        setTimeStarted(value);
        return this;
    }

    @Override
    public MasJobRecordNewRecord value5(Instant value) {
        setTimeCompleted(value);
        return this;
    }

    @Override
    public MasJobRecordNewRecord value6(JSONB value) {
        setAnnotations(value);
        return this;
    }

    @Override
    public MasJobRecordNewRecord value7(String value) {
        setTargetId(value);
        return this;
    }

    @Override
    public MasJobRecordNewRecord value8(String value) {
        setUserId(value);
        return this;
    }

    @Override
    public MasJobRecordNewRecord values(String value1, MasJobState value2, String value3, Instant value4, Instant value5, JSONB value6, String value7, String value8) {
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
     * Create a detached MasJobRecordNewRecord
     */
    public MasJobRecordNewRecord() {
        super(MasJobRecordNew.MAS_JOB_RECORD_NEW);
    }

    /**
     * Create a detached, initialised MasJobRecordNewRecord
     */
    public MasJobRecordNewRecord(String jobId, MasJobState jobState, String masId, Instant timeStarted, Instant timeCompleted, JSONB annotations, String targetId, String userId) {
        super(MasJobRecordNew.MAS_JOB_RECORD_NEW);

        setJobId(jobId);
        setJobState(jobState);
        setMasId(masId);
        setTimeStarted(timeStarted);
        setTimeCompleted(timeCompleted);
        setAnnotations(annotations);
        setTargetId(targetId);
        setUserId(userId);
        resetChangedOnNotNull();
    }
}
