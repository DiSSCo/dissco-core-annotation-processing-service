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
public enum ErrorCode implements EnumType {

    TIMEOUT("TIMEOUT"),

    DISSCO_EXCEPTION("DISSCO_EXCEPTION"),

    MAS_EXCEPTION("MAS_EXCEPTION");

    private final String literal;

    private ErrorCode(String literal) {
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
        return "error_code";
    }

    @Override
    public String getLiteral() {
        return literal;
    }

    /**
     * Lookup a value of this EnumType by its literal. Returns
     * <code>null</code>, if no such value could be found, see {@link
     * EnumType#lookupLiteral(Class, String)}.
     */
    public static ErrorCode lookupLiteral(String literal) {
        return EnumType.lookupLiteral(ErrorCode.class, literal);
    }
}
