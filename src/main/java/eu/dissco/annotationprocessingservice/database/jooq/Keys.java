/*
 * This file is generated by jOOQ.
 */
package eu.dissco.annotationprocessingservice.database.jooq;


import eu.dissco.annotationprocessingservice.database.jooq.tables.Handles;
import eu.dissco.annotationprocessingservice.database.jooq.tables.NewAnnotation;
import eu.dissco.annotationprocessingservice.database.jooq.tables.records.HandlesRecord;
import eu.dissco.annotationprocessingservice.database.jooq.tables.records.NewAnnotationRecord;

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

    public static final UniqueKey<HandlesRecord> HANDLES_PKEY = Internal.createUniqueKey(Handles.HANDLES, DSL.name("handles_pkey"), new TableField[] { Handles.HANDLES.HANDLE, Handles.HANDLES.IDX }, true);
    public static final UniqueKey<NewAnnotationRecord> NEW_ANNOTATION_PKEY = Internal.createUniqueKey(NewAnnotation.NEW_ANNOTATION, DSL.name("new_annotation_pkey"), new TableField[] { NewAnnotation.NEW_ANNOTATION.ID, NewAnnotation.NEW_ANNOTATION.VERSION }, true);
}
