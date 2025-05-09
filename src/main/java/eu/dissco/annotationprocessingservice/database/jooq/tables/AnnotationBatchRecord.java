/*
 * This file is generated by jOOQ.
 */
package eu.dissco.annotationprocessingservice.database.jooq.tables;


import eu.dissco.annotationprocessingservice.database.jooq.Keys;
import eu.dissco.annotationprocessingservice.database.jooq.Public;
import eu.dissco.annotationprocessingservice.database.jooq.tables.records.AnnotationBatchRecordRecord;

import java.time.Instant;
import java.util.Collection;
import java.util.UUID;

import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Name;
import org.jooq.PlainSQL;
import org.jooq.QueryPart;
import org.jooq.SQL;
import org.jooq.Schema;
import org.jooq.Select;
import org.jooq.Stringly;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.TableOptions;
import org.jooq.UniqueKey;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.TableImpl;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class AnnotationBatchRecord extends TableImpl<AnnotationBatchRecordRecord> {

    private static final long serialVersionUID = 1L;

    /**
     * The reference instance of <code>public.annotation_batch_record</code>
     */
    public static final AnnotationBatchRecord ANNOTATION_BATCH_RECORD = new AnnotationBatchRecord();

    /**
     * The class holding records for this type
     */
    @Override
    public Class<AnnotationBatchRecordRecord> getRecordType() {
        return AnnotationBatchRecordRecord.class;
    }

    /**
     * The column <code>public.annotation_batch_record.id</code>.
     */
    public final TableField<AnnotationBatchRecordRecord, UUID> ID = createField(DSL.name("id"), SQLDataType.UUID.nullable(false), this, "");

    /**
     * The column <code>public.annotation_batch_record.creator</code>.
     */
    public final TableField<AnnotationBatchRecordRecord, String> CREATOR = createField(DSL.name("creator"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>public.annotation_batch_record.created</code>.
     */
    public final TableField<AnnotationBatchRecordRecord, Instant> CREATED = createField(DSL.name("created"), SQLDataType.INSTANT.nullable(false), this, "");

    /**
     * The column <code>public.annotation_batch_record.last_updated</code>.
     */
    public final TableField<AnnotationBatchRecordRecord, Instant> LAST_UPDATED = createField(DSL.name("last_updated"), SQLDataType.INSTANT, this, "");

    /**
     * The column <code>public.annotation_batch_record.job_id</code>.
     */
    public final TableField<AnnotationBatchRecordRecord, String> JOB_ID = createField(DSL.name("job_id"), SQLDataType.CLOB, this, "");

    /**
     * The column <code>public.annotation_batch_record.batch_quantity</code>.
     */
    public final TableField<AnnotationBatchRecordRecord, Long> BATCH_QUANTITY = createField(DSL.name("batch_quantity"), SQLDataType.BIGINT, this, "");

    /**
     * The column
     * <code>public.annotation_batch_record.parent_annotation_id</code>.
     */
    public final TableField<AnnotationBatchRecordRecord, String> PARENT_ANNOTATION_ID = createField(DSL.name("parent_annotation_id"), SQLDataType.CLOB, this, "");

    private AnnotationBatchRecord(Name alias, Table<AnnotationBatchRecordRecord> aliased) {
        this(alias, aliased, (Field<?>[]) null, null);
    }

    private AnnotationBatchRecord(Name alias, Table<AnnotationBatchRecordRecord> aliased, Field<?>[] parameters, Condition where) {
        super(alias, null, aliased, parameters, DSL.comment(""), TableOptions.table(), where);
    }

    /**
     * Create an aliased <code>public.annotation_batch_record</code> table
     * reference
     */
    public AnnotationBatchRecord(String alias) {
        this(DSL.name(alias), ANNOTATION_BATCH_RECORD);
    }

    /**
     * Create an aliased <code>public.annotation_batch_record</code> table
     * reference
     */
    public AnnotationBatchRecord(Name alias) {
        this(alias, ANNOTATION_BATCH_RECORD);
    }

    /**
     * Create a <code>public.annotation_batch_record</code> table reference
     */
    public AnnotationBatchRecord() {
        this(DSL.name("annotation_batch_record"), null);
    }

    @Override
    public Schema getSchema() {
        return aliased() ? null : Public.PUBLIC;
    }

    @Override
    public UniqueKey<AnnotationBatchRecordRecord> getPrimaryKey() {
        return Keys.ANNOTATION_BATCH_PK;
    }

    @Override
    public AnnotationBatchRecord as(String alias) {
        return new AnnotationBatchRecord(DSL.name(alias), this);
    }

    @Override
    public AnnotationBatchRecord as(Name alias) {
        return new AnnotationBatchRecord(alias, this);
    }

    @Override
    public AnnotationBatchRecord as(Table<?> alias) {
        return new AnnotationBatchRecord(alias.getQualifiedName(), this);
    }

    /**
     * Rename this table
     */
    @Override
    public AnnotationBatchRecord rename(String name) {
        return new AnnotationBatchRecord(DSL.name(name), null);
    }

    /**
     * Rename this table
     */
    @Override
    public AnnotationBatchRecord rename(Name name) {
        return new AnnotationBatchRecord(name, null);
    }

    /**
     * Rename this table
     */
    @Override
    public AnnotationBatchRecord rename(Table<?> name) {
        return new AnnotationBatchRecord(name.getQualifiedName(), null);
    }

    /**
     * Create an inline derived table from this table
     */
    @Override
    public AnnotationBatchRecord where(Condition condition) {
        return new AnnotationBatchRecord(getQualifiedName(), aliased() ? this : null, null, condition);
    }

    /**
     * Create an inline derived table from this table
     */
    @Override
    public AnnotationBatchRecord where(Collection<? extends Condition> conditions) {
        return where(DSL.and(conditions));
    }

    /**
     * Create an inline derived table from this table
     */
    @Override
    public AnnotationBatchRecord where(Condition... conditions) {
        return where(DSL.and(conditions));
    }

    /**
     * Create an inline derived table from this table
     */
    @Override
    public AnnotationBatchRecord where(Field<Boolean> condition) {
        return where(DSL.condition(condition));
    }

    /**
     * Create an inline derived table from this table
     */
    @Override
    @PlainSQL
    public AnnotationBatchRecord where(SQL condition) {
        return where(DSL.condition(condition));
    }

    /**
     * Create an inline derived table from this table
     */
    @Override
    @PlainSQL
    public AnnotationBatchRecord where(@Stringly.SQL String condition) {
        return where(DSL.condition(condition));
    }

    /**
     * Create an inline derived table from this table
     */
    @Override
    @PlainSQL
    public AnnotationBatchRecord where(@Stringly.SQL String condition, Object... binds) {
        return where(DSL.condition(condition, binds));
    }

    /**
     * Create an inline derived table from this table
     */
    @Override
    @PlainSQL
    public AnnotationBatchRecord where(@Stringly.SQL String condition, QueryPart... parts) {
        return where(DSL.condition(condition, parts));
    }

    /**
     * Create an inline derived table from this table
     */
    @Override
    public AnnotationBatchRecord whereExists(Select<?> select) {
        return where(DSL.exists(select));
    }

    /**
     * Create an inline derived table from this table
     */
    @Override
    public AnnotationBatchRecord whereNotExists(Select<?> select) {
        return where(DSL.notExists(select));
    }
}
