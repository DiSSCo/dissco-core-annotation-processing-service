/*
 * This file is generated by jOOQ.
 */
package eu.dissco.annotationprocessingservice.database.jooq;


import eu.dissco.annotationprocessingservice.database.jooq.tables.AnnotationBatchRecord;
import eu.dissco.annotationprocessingservice.database.jooq.tables.MasJobRecord;
import eu.dissco.annotationprocessingservice.database.jooq.tables.NewAnnotation;
import eu.dissco.annotationprocessingservice.database.jooq.tables.SourceSystem;
import java.util.Arrays;
import java.util.List;
import org.jooq.Catalog;
import org.jooq.Table;
import org.jooq.impl.SchemaImpl;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class Public extends SchemaImpl {

    private static final long serialVersionUID = 1L;

    /**
     * The reference instance of <code>public</code>
     */
    public static final Public PUBLIC = new Public();

    /**
     * The table <code>public.annotation_batch_record</code>.
     */
    public final AnnotationBatchRecord ANNOTATION_BATCH_RECORD = AnnotationBatchRecord.ANNOTATION_BATCH_RECORD;

    /**
     * The table <code>public.mas_job_record</code>.
     */
    public final MasJobRecord MAS_JOB_RECORD = MasJobRecord.MAS_JOB_RECORD;

    /**
     * The table <code>public.new_annotation</code>.
     */
    public final NewAnnotation NEW_ANNOTATION = NewAnnotation.NEW_ANNOTATION;

    /**
     * The table <code>public.source_system</code>.
     */
    public final SourceSystem SOURCE_SYSTEM = SourceSystem.SOURCE_SYSTEM;

    /**
     * No further instances allowed
     */
    private Public() {
        super("public", null);
    }


    @Override
    public Catalog getCatalog() {
        return DefaultCatalog.DEFAULT_CATALOG;
    }

    @Override
    public final List<Table<?>> getTables() {
        return Arrays.asList(
            AnnotationBatchRecord.ANNOTATION_BATCH_RECORD,
            MasJobRecord.MAS_JOB_RECORD,
            NewAnnotation.NEW_ANNOTATION,
            SourceSystem.SOURCE_SYSTEM
        );
    }
}
