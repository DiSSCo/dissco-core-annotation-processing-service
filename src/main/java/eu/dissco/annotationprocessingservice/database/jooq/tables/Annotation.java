/*
 * This file is generated by jOOQ.
 */
package eu.dissco.annotationprocessingservice.database.jooq.tables;


import eu.dissco.annotationprocessingservice.database.jooq.Indexes;
import eu.dissco.annotationprocessingservice.database.jooq.Keys;
import eu.dissco.annotationprocessingservice.database.jooq.Public;
import eu.dissco.annotationprocessingservice.database.jooq.tables.records.AnnotationRecord;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.jooq.Field;
import org.jooq.ForeignKey;
import org.jooq.Function16;
import org.jooq.Index;
import org.jooq.JSONB;
import org.jooq.Name;
import org.jooq.Record;
import org.jooq.Records;
import org.jooq.Row16;
import org.jooq.Schema;
import org.jooq.SelectField;
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
public class Annotation extends TableImpl<AnnotationRecord> {

    private static final long serialVersionUID = 1L;

    /**
     * The reference instance of <code>public.annotation</code>
     */
    public static final Annotation ANNOTATION = new Annotation();

    /**
     * The class holding records for this type
     */
    @Override
    public Class<AnnotationRecord> getRecordType() {
        return AnnotationRecord.class;
    }

    /**
     * The column <code>public.annotation.id</code>.
     */
    public final TableField<AnnotationRecord, String> ID = createField(DSL.name("id"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>public.annotation.version</code>.
     */
    public final TableField<AnnotationRecord, Integer> VERSION = createField(DSL.name("version"), SQLDataType.INTEGER.nullable(false), this, "");

    /**
     * The column <code>public.annotation.type</code>.
     */
    public final TableField<AnnotationRecord, String> TYPE = createField(DSL.name("type"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>public.annotation.motivation</code>.
     */
    public final TableField<AnnotationRecord, String> MOTIVATION = createField(DSL.name("motivation"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>public.annotation.motivated_by</code>.
     */
    public final TableField<AnnotationRecord, String> MOTIVATED_BY = createField(DSL.name("motivated_by"), SQLDataType.CLOB, this, "");

    /**
     * The column <code>public.annotation.target_id</code>.
     */
    public final TableField<AnnotationRecord, String> TARGET_ID = createField(DSL.name("target_id"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>public.annotation.target</code>.
     */
    public final TableField<AnnotationRecord, JSONB> TARGET = createField(DSL.name("target"), SQLDataType.JSONB.nullable(false), this, "");

    /**
     * The column <code>public.annotation.body</code>.
     */
    public final TableField<AnnotationRecord, JSONB> BODY = createField(DSL.name("body"), SQLDataType.JSONB.nullable(false), this, "");

    /**
     * The column <code>public.annotation.creator_id</code>.
     */
    public final TableField<AnnotationRecord, String> CREATOR_ID = createField(DSL.name("creator_id"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>public.annotation.creator</code>.
     */
    public final TableField<AnnotationRecord, JSONB> CREATOR = createField(DSL.name("creator"), SQLDataType.JSONB.nullable(false), this, "");

    /**
     * The column <code>public.annotation.created</code>.
     */
    public final TableField<AnnotationRecord, Instant> CREATED = createField(DSL.name("created"), SQLDataType.INSTANT.nullable(false), this, "");

    /**
     * The column <code>public.annotation.generator</code>.
     */
    public final TableField<AnnotationRecord, JSONB> GENERATOR = createField(DSL.name("generator"), SQLDataType.JSONB.nullable(false), this, "");

    /**
     * The column <code>public.annotation.generated</code>.
     */
    public final TableField<AnnotationRecord, Instant> GENERATED = createField(DSL.name("generated"), SQLDataType.INSTANT.nullable(false), this, "");

    /**
     * The column <code>public.annotation.last_checked</code>.
     */
    public final TableField<AnnotationRecord, Instant> LAST_CHECKED = createField(DSL.name("last_checked"), SQLDataType.INSTANT.nullable(false), this, "");

    /**
     * The column <code>public.annotation.aggregate_rating</code>.
     */
    public final TableField<AnnotationRecord, JSONB> AGGREGATE_RATING = createField(DSL.name("aggregate_rating"), SQLDataType.JSONB, this, "");

    /**
     * The column <code>public.annotation.deleted_on</code>.
     */
    public final TableField<AnnotationRecord, Instant> DELETED_ON = createField(DSL.name("deleted_on"), SQLDataType.INSTANT, this, "");

    private Annotation(Name alias, Table<AnnotationRecord> aliased) {
        this(alias, aliased, null);
    }

    private Annotation(Name alias, Table<AnnotationRecord> aliased, Field<?>[] parameters) {
        super(alias, null, aliased, parameters, DSL.comment(""), TableOptions.table());
    }

    /**
     * Create an aliased <code>public.annotation</code> table reference
     */
    public Annotation(String alias) {
        this(DSL.name(alias), ANNOTATION);
    }

    /**
     * Create an aliased <code>public.annotation</code> table reference
     */
    public Annotation(Name alias) {
        this(alias, ANNOTATION);
    }

    /**
     * Create a <code>public.annotation</code> table reference
     */
    public Annotation() {
        this(DSL.name("annotation"), null);
    }

    public <O extends Record> Annotation(Table<O> child, ForeignKey<O, AnnotationRecord> key) {
        super(child, key, ANNOTATION);
    }

    @Override
    public Schema getSchema() {
        return aliased() ? null : Public.PUBLIC;
    }

    @Override
    public List<Index> getIndexes() {
        return Arrays.asList(Indexes.ANNOTATION_ID_CREATOR_ID_INDEX, Indexes.ANNOTATION_ID_TARGET_ID_INDEX);
    }

    @Override
    public UniqueKey<AnnotationRecord> getPrimaryKey() {
        return Keys.ANNOTATION_PK;
    }

    @Override
    public Annotation as(String alias) {
        return new Annotation(DSL.name(alias), this);
    }

    @Override
    public Annotation as(Name alias) {
        return new Annotation(alias, this);
    }

    @Override
    public Annotation as(Table<?> alias) {
        return new Annotation(alias.getQualifiedName(), this);
    }

    /**
     * Rename this table
     */
    @Override
    public Annotation rename(String name) {
        return new Annotation(DSL.name(name), null);
    }

    /**
     * Rename this table
     */
    @Override
    public Annotation rename(Name name) {
        return new Annotation(name, null);
    }

    /**
     * Rename this table
     */
    @Override
    public Annotation rename(Table<?> name) {
        return new Annotation(name.getQualifiedName(), null);
    }

    // -------------------------------------------------------------------------
    // Row16 type methods
    // -------------------------------------------------------------------------

    @Override
    public Row16<String, Integer, String, String, String, String, JSONB, JSONB, String, JSONB, Instant, JSONB, Instant, Instant, JSONB, Instant> fieldsRow() {
        return (Row16) super.fieldsRow();
    }

    /**
     * Convenience mapping calling {@link SelectField#convertFrom(Function)}.
     */
    public <U> SelectField<U> mapping(Function16<? super String, ? super Integer, ? super String, ? super String, ? super String, ? super String, ? super JSONB, ? super JSONB, ? super String, ? super JSONB, ? super Instant, ? super JSONB, ? super Instant, ? super Instant, ? super JSONB, ? super Instant, ? extends U> from) {
        return convertFrom(Records.mapping(from));
    }

    /**
     * Convenience mapping calling {@link SelectField#convertFrom(Class,
     * Function)}.
     */
    public <U> SelectField<U> mapping(Class<U> toType, Function16<? super String, ? super Integer, ? super String, ? super String, ? super String, ? super String, ? super JSONB, ? super JSONB, ? super String, ? super JSONB, ? super Instant, ? super JSONB, ? super Instant, ? super Instant, ? super JSONB, ? super Instant, ? extends U> from) {
        return convertFrom(toType, Records.mapping(from));
    }
}
