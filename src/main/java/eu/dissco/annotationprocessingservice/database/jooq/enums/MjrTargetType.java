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
public enum MjrTargetType implements EnumType {

    DIGITAL_SPECIMEN("DIGITAL_SPECIMEN"),

    MEDIA_OBJECT("MEDIA_OBJECT");

    private final String literal;

    private MjrTargetType(String literal) {
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
        return "mjr_target_type";
    }

    @Override
    public String getLiteral() {
        return literal;
    }

    /**
     * Lookup a value of this EnumType by its literal
     */
    public static MjrTargetType lookupLiteral(String literal) {
        return EnumType.lookupLiteral(MjrTargetType.class, literal);
    }
}
