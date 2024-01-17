/*
 * This file is generated by jOOQ.
 */
package eu.dissco.annotationprocessingservice.database.jooq.tables.records;


import eu.dissco.annotationprocessingservice.database.jooq.tables.AnnotationTmp;

import java.time.Instant;
import java.util.UUID;

import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Record1;
import org.jooq.Record18;
import org.jooq.Row18;
import org.jooq.impl.UpdatableRecordImpl;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class AnnotationTmpRecord extends UpdatableRecordImpl<AnnotationTmpRecord> implements Record18<String, Integer, String, String, String, String, JSONB, JSONB, String, JSONB, Instant, JSONB, Instant, Instant, JSONB, Instant, UUID, String> {

    private static final long serialVersionUID = 1L;

    /**
     * Setter for <code>public.annotation_tmp.id</code>.
     */
    public void setId(String value) {
        set(0, value);
    }

    /**
     * Getter for <code>public.annotation_tmp.id</code>.
     */
    public String getId() {
        return (String) get(0);
    }

    /**
     * Setter for <code>public.annotation_tmp.version</code>.
     */
    public void setVersion(Integer value) {
        set(1, value);
    }

    /**
     * Getter for <code>public.annotation_tmp.version</code>.
     */
    public Integer getVersion() {
        return (Integer) get(1);
    }

    /**
     * Setter for <code>public.annotation_tmp.type</code>.
     */
    public void setType(String value) {
        set(2, value);
    }

    /**
     * Getter for <code>public.annotation_tmp.type</code>.
     */
    public String getType() {
        return (String) get(2);
    }

    /**
     * Setter for <code>public.annotation_tmp.motivation</code>.
     */
    public void setMotivation(String value) {
        set(3, value);
    }

    /**
     * Getter for <code>public.annotation_tmp.motivation</code>.
     */
    public String getMotivation() {
        return (String) get(3);
    }

    /**
     * Setter for <code>public.annotation_tmp.motivated_by</code>.
     */
    public void setMotivatedBy(String value) {
        set(4, value);
    }

    /**
     * Getter for <code>public.annotation_tmp.motivated_by</code>.
     */
    public String getMotivatedBy() {
        return (String) get(4);
    }

    /**
     * Setter for <code>public.annotation_tmp.target_id</code>.
     */
    public void setTargetId(String value) {
        set(5, value);
    }

    /**
     * Getter for <code>public.annotation_tmp.target_id</code>.
     */
    public String getTargetId() {
        return (String) get(5);
    }

    /**
     * Setter for <code>public.annotation_tmp.target</code>.
     */
    public void setTarget(JSONB value) {
        set(6, value);
    }

    /**
     * Getter for <code>public.annotation_tmp.target</code>.
     */
    public JSONB getTarget() {
        return (JSONB) get(6);
    }

    /**
     * Setter for <code>public.annotation_tmp.body</code>.
     */
    public void setBody(JSONB value) {
        set(7, value);
    }

    /**
     * Getter for <code>public.annotation_tmp.body</code>.
     */
    public JSONB getBody() {
        return (JSONB) get(7);
    }

    /**
     * Setter for <code>public.annotation_tmp.creator_id</code>.
     */
    public void setCreatorId(String value) {
        set(8, value);
    }

    /**
     * Getter for <code>public.annotation_tmp.creator_id</code>.
     */
    public String getCreatorId() {
        return (String) get(8);
    }

    /**
     * Setter for <code>public.annotation_tmp.creator</code>.
     */
    public void setCreator(JSONB value) {
        set(9, value);
    }

    /**
     * Getter for <code>public.annotation_tmp.creator</code>.
     */
    public JSONB getCreator() {
        return (JSONB) get(9);
    }

    /**
     * Setter for <code>public.annotation_tmp.created</code>.
     */
    public void setCreated(Instant value) {
        set(10, value);
    }

    /**
     * Getter for <code>public.annotation_tmp.created</code>.
     */
    public Instant getCreated() {
        return (Instant) get(10);
    }

    /**
     * Setter for <code>public.annotation_tmp.generator</code>.
     */
    public void setGenerator(JSONB value) {
        set(11, value);
    }

    /**
     * Getter for <code>public.annotation_tmp.generator</code>.
     */
    public JSONB getGenerator() {
        return (JSONB) get(11);
    }

    /**
     * Setter for <code>public.annotation_tmp.generated</code>.
     */
    public void setGenerated(Instant value) {
        set(12, value);
    }

    /**
     * Getter for <code>public.annotation_tmp.generated</code>.
     */
    public Instant getGenerated() {
        return (Instant) get(12);
    }

    /**
     * Setter for <code>public.annotation_tmp.last_checked</code>.
     */
    public void setLastChecked(Instant value) {
        set(13, value);
    }

    /**
     * Getter for <code>public.annotation_tmp.last_checked</code>.
     */
    public Instant getLastChecked() {
        return (Instant) get(13);
    }

    /**
     * Setter for <code>public.annotation_tmp.aggregate_rating</code>.
     */
    public void setAggregateRating(JSONB value) {
        set(14, value);
    }

    /**
     * Getter for <code>public.annotation_tmp.aggregate_rating</code>.
     */
    public JSONB getAggregateRating() {
        return (JSONB) get(14);
    }

    /**
     * Setter for <code>public.annotation_tmp.deleted_on</code>.
     */
    public void setDeletedOn(Instant value) {
        set(15, value);
    }

    /**
     * Getter for <code>public.annotation_tmp.deleted_on</code>.
     */
    public Instant getDeletedOn() {
        return (Instant) get(15);
    }

    /**
     * Setter for <code>public.annotation_tmp.annotation_hash</code>.
     */
    public void setAnnotationHash(UUID value) {
        set(16, value);
    }

    /**
     * Getter for <code>public.annotation_tmp.annotation_hash</code>.
     */
    public UUID getAnnotationHash() {
        return (UUID) get(16);
    }

    /**
     * Setter for <code>public.annotation_tmp.mjr_job_id</code>.
     */
    public void setMjrJobId(String value) {
        set(17, value);
    }

    /**
     * Getter for <code>public.annotation_tmp.mjr_job_id</code>.
     */
    public String getMjrJobId() {
        return (String) get(17);
    }

    // -------------------------------------------------------------------------
    // Primary key information
    // -------------------------------------------------------------------------

    @Override
    public Record1<String> key() {
        return (Record1) super.key();
    }

    // -------------------------------------------------------------------------
    // Record18 type implementation
    // -------------------------------------------------------------------------

    @Override
    public Row18<String, Integer, String, String, String, String, JSONB, JSONB, String, JSONB, Instant, JSONB, Instant, Instant, JSONB, Instant, UUID, String> fieldsRow() {
        return (Row18) super.fieldsRow();
    }

    @Override
    public Row18<String, Integer, String, String, String, String, JSONB, JSONB, String, JSONB, Instant, JSONB, Instant, Instant, JSONB, Instant, UUID, String> valuesRow() {
        return (Row18) super.valuesRow();
    }

    @Override
    public Field<String> field1() {
        return AnnotationTmp.ANNOTATION_TMP.ID;
    }

    @Override
    public Field<Integer> field2() {
        return AnnotationTmp.ANNOTATION_TMP.VERSION;
    }

    @Override
    public Field<String> field3() {
        return AnnotationTmp.ANNOTATION_TMP.TYPE;
    }

    @Override
    public Field<String> field4() {
        return AnnotationTmp.ANNOTATION_TMP.MOTIVATION;
    }

    @Override
    public Field<String> field5() {
        return AnnotationTmp.ANNOTATION_TMP.MOTIVATED_BY;
    }

    @Override
    public Field<String> field6() {
        return AnnotationTmp.ANNOTATION_TMP.TARGET_ID;
    }

    @Override
    public Field<JSONB> field7() {
        return AnnotationTmp.ANNOTATION_TMP.TARGET;
    }

    @Override
    public Field<JSONB> field8() {
        return AnnotationTmp.ANNOTATION_TMP.BODY;
    }

    @Override
    public Field<String> field9() {
        return AnnotationTmp.ANNOTATION_TMP.CREATOR_ID;
    }

    @Override
    public Field<JSONB> field10() {
        return AnnotationTmp.ANNOTATION_TMP.CREATOR;
    }

    @Override
    public Field<Instant> field11() {
        return AnnotationTmp.ANNOTATION_TMP.CREATED;
    }

    @Override
    public Field<JSONB> field12() {
        return AnnotationTmp.ANNOTATION_TMP.GENERATOR;
    }

    @Override
    public Field<Instant> field13() {
        return AnnotationTmp.ANNOTATION_TMP.GENERATED;
    }

    @Override
    public Field<Instant> field14() {
        return AnnotationTmp.ANNOTATION_TMP.LAST_CHECKED;
    }

    @Override
    public Field<JSONB> field15() {
        return AnnotationTmp.ANNOTATION_TMP.AGGREGATE_RATING;
    }

    @Override
    public Field<Instant> field16() {
        return AnnotationTmp.ANNOTATION_TMP.DELETED_ON;
    }

    @Override
    public Field<UUID> field17() {
        return AnnotationTmp.ANNOTATION_TMP.ANNOTATION_HASH;
    }

    @Override
    public Field<String> field18() {
        return AnnotationTmp.ANNOTATION_TMP.MJR_JOB_ID;
    }

    @Override
    public String component1() {
        return getId();
    }

    @Override
    public Integer component2() {
        return getVersion();
    }

    @Override
    public String component3() {
        return getType();
    }

    @Override
    public String component4() {
        return getMotivation();
    }

    @Override
    public String component5() {
        return getMotivatedBy();
    }

    @Override
    public String component6() {
        return getTargetId();
    }

    @Override
    public JSONB component7() {
        return getTarget();
    }

    @Override
    public JSONB component8() {
        return getBody();
    }

    @Override
    public String component9() {
        return getCreatorId();
    }

    @Override
    public JSONB component10() {
        return getCreator();
    }

    @Override
    public Instant component11() {
        return getCreated();
    }

    @Override
    public JSONB component12() {
        return getGenerator();
    }

    @Override
    public Instant component13() {
        return getGenerated();
    }

    @Override
    public Instant component14() {
        return getLastChecked();
    }

    @Override
    public JSONB component15() {
        return getAggregateRating();
    }

    @Override
    public Instant component16() {
        return getDeletedOn();
    }

    @Override
    public UUID component17() {
        return getAnnotationHash();
    }

    @Override
    public String component18() {
        return getMjrJobId();
    }

    @Override
    public String value1() {
        return getId();
    }

    @Override
    public Integer value2() {
        return getVersion();
    }

    @Override
    public String value3() {
        return getType();
    }

    @Override
    public String value4() {
        return getMotivation();
    }

    @Override
    public String value5() {
        return getMotivatedBy();
    }

    @Override
    public String value6() {
        return getTargetId();
    }

    @Override
    public JSONB value7() {
        return getTarget();
    }

    @Override
    public JSONB value8() {
        return getBody();
    }

    @Override
    public String value9() {
        return getCreatorId();
    }

    @Override
    public JSONB value10() {
        return getCreator();
    }

    @Override
    public Instant value11() {
        return getCreated();
    }

    @Override
    public JSONB value12() {
        return getGenerator();
    }

    @Override
    public Instant value13() {
        return getGenerated();
    }

    @Override
    public Instant value14() {
        return getLastChecked();
    }

    @Override
    public JSONB value15() {
        return getAggregateRating();
    }

    @Override
    public Instant value16() {
        return getDeletedOn();
    }

    @Override
    public UUID value17() {
        return getAnnotationHash();
    }

    @Override
    public String value18() {
        return getMjrJobId();
    }

    @Override
    public AnnotationTmpRecord value1(String value) {
        setId(value);
        return this;
    }

    @Override
    public AnnotationTmpRecord value2(Integer value) {
        setVersion(value);
        return this;
    }

    @Override
    public AnnotationTmpRecord value3(String value) {
        setType(value);
        return this;
    }

    @Override
    public AnnotationTmpRecord value4(String value) {
        setMotivation(value);
        return this;
    }

    @Override
    public AnnotationTmpRecord value5(String value) {
        setMotivatedBy(value);
        return this;
    }

    @Override
    public AnnotationTmpRecord value6(String value) {
        setTargetId(value);
        return this;
    }

    @Override
    public AnnotationTmpRecord value7(JSONB value) {
        setTarget(value);
        return this;
    }

    @Override
    public AnnotationTmpRecord value8(JSONB value) {
        setBody(value);
        return this;
    }

    @Override
    public AnnotationTmpRecord value9(String value) {
        setCreatorId(value);
        return this;
    }

    @Override
    public AnnotationTmpRecord value10(JSONB value) {
        setCreator(value);
        return this;
    }

    @Override
    public AnnotationTmpRecord value11(Instant value) {
        setCreated(value);
        return this;
    }

    @Override
    public AnnotationTmpRecord value12(JSONB value) {
        setGenerator(value);
        return this;
    }

    @Override
    public AnnotationTmpRecord value13(Instant value) {
        setGenerated(value);
        return this;
    }

    @Override
    public AnnotationTmpRecord value14(Instant value) {
        setLastChecked(value);
        return this;
    }

    @Override
    public AnnotationTmpRecord value15(JSONB value) {
        setAggregateRating(value);
        return this;
    }

    @Override
    public AnnotationTmpRecord value16(Instant value) {
        setDeletedOn(value);
        return this;
    }

    @Override
    public AnnotationTmpRecord value17(UUID value) {
        setAnnotationHash(value);
        return this;
    }

    @Override
    public AnnotationTmpRecord value18(String value) {
        setMjrJobId(value);
        return this;
    }

    @Override
    public AnnotationTmpRecord values(String value1, Integer value2, String value3, String value4, String value5, String value6, JSONB value7, JSONB value8, String value9, JSONB value10, Instant value11, JSONB value12, Instant value13, Instant value14, JSONB value15, Instant value16, UUID value17, String value18) {
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
        value13(value13);
        value14(value14);
        value15(value15);
        value16(value16);
        value17(value17);
        value18(value18);
        return this;
    }

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /**
     * Create a detached AnnotationTmpRecord
     */
    public AnnotationTmpRecord() {
        super(AnnotationTmp.ANNOTATION_TMP);
    }

    /**
     * Create a detached, initialised AnnotationTmpRecord
     */
    public AnnotationTmpRecord(String id, Integer version, String type, String motivation, String motivatedBy, String targetId, JSONB target, JSONB body, String creatorId, JSONB creator, Instant created, JSONB generator, Instant generated, Instant lastChecked, JSONB aggregateRating, Instant deletedOn, UUID annotationHash, String mjrJobId) {
        super(AnnotationTmp.ANNOTATION_TMP);

        setId(id);
        setVersion(version);
        setType(type);
        setMotivation(motivation);
        setMotivatedBy(motivatedBy);
        setTargetId(targetId);
        setTarget(target);
        setBody(body);
        setCreatorId(creatorId);
        setCreator(creator);
        setCreated(created);
        setGenerator(generator);
        setGenerated(generated);
        setLastChecked(lastChecked);
        setAggregateRating(aggregateRating);
        setDeletedOn(deletedOn);
        setAnnotationHash(annotationHash);
        setMjrJobId(mjrJobId);
        resetChangedOnNotNull();
    }
}
