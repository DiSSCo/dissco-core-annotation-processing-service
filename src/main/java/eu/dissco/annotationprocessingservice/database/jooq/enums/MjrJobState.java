/*
 * This file is generated by jOOQ.
 */
package eu.dissco.annotationprocessingservice.database.jooq.enums;


import eu.dissco.annotationprocessingservice.database.jooq.Public;

import org.jooq.Catalog;
import org.jooq.EnumType;
import org.jooq.Schema;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public enum MjrJobState implements EnumType {

    SCHEDULED("SCHEDULED"),

    RUNNING("RUNNING"),

    FAILED("FAILED"),

    COMPLETED("COMPLETED");

    private final String literal;

    private MjrJobState(String literal) {
        this.literal = literal;
    }

    @Override
    public Catalog getCatalog() {
        return getSchema().getCatalog();
    }

    @Override
    public Schema getSchema() {
        return Public.PUBLIC;
    }

    @Override
    public String getName() {
        return "mjr_job_state";
    }

    @Override
    public String getLiteral() {
        return literal;
    }

    /**
     * Lookup a value of this EnumType by its literal
     */
    public static MjrJobState lookupLiteral(String literal) {
        return EnumType.lookupLiteral(MjrJobState.class, literal);
    }
}
