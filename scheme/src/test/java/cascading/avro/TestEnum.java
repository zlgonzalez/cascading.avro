package cascading.avro;

/**
 * Enum used for testing must be top level, as otherwise name looks like <enclosing class>$TestEnum, and '$' isn't
 * valid as an Avro name.
 *
 */
public enum TestEnum {
        ONE,
        TWO

}
