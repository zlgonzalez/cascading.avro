package cascading.avro;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.hadoop.io.BytesWritable;

import cascading.tuple.Fields;

class AvroHelper implements Serializable {

    private static final long serialVersionUID = -7610828460324010725L;

    /**
     * Helper class used to save an Enum name in a type that Avro requires for
     * serialization.
     */
    static class CascadingEnumSymbol implements GenericEnumSymbol {

        private final String name;

        private CascadingEnumSymbol(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.avro.generic.GenericContainer#getSchema()
         */
        @Override
        public Schema getSchema() {
            return null;
        }
    }

    private static final String RECORD_NAME = "CascadingAvroSchema";

    public static Schema getSchema(String recordName, Fields fields,
            Class<?>[] types) {
        validateFields(fields, types);
        return generateSchema(recordName, fields, types, 0);
    }

    public static Schema getSchema(Fields fields, Class<?>[] types) {
        validateFields(fields, types);
        return generateSchema(RECORD_NAME, fields, types, 0);
    }

    private static Schema generateSchema(String recordName,
            Fields schemeFields, Class<?>[] types, int depth) {
        // Create a 'record' that is made up of fields.
        // Since we support arrays and maps that means we can have nested
        // records

        List<Schema.Field> fields = new ArrayList<Schema.Field>();
        for (int typeIndex = 0, fieldIndex = 0; typeIndex < types.length; typeIndex++, fieldIndex++) {
            String fieldName = schemeFields.get(fieldIndex).toString();
            // at most 2, since we only allow primitive
            // types for arrays and maps
            final Class<?> type = types[typeIndex];
            final boolean b = type == List.class || type == Map.class;
            Class<?>[] subTypes = new Class[] { type,
                    b ? types[++typeIndex] : type };
            final Schema schema = createAvroSchema(recordName, schemeFields,
                    subTypes, depth + 1);
            final Schema nullSchema = Schema.create(Schema.Type.NULL);
            final List<Schema> schemas = new LinkedList<Schema>();
            schemas.add(nullSchema);
            schemas.add(schema);
            fields.add(new Schema.Field(fieldName, Schema.createUnion(schemas),
                    "", null));
        }

        // Avro doesn't like anonymous records - so create a named one.
        if (depth > 0) {
            recordName = recordName + depth;
        }
        Schema schema = Schema.createRecord(recordName, "auto generated", "",
                false);
        schema.setFields(fields);
        return schema;
    }

    @SuppressWarnings({ "static-access" })
    private static Schema createAvroSchema(String recordName, Fields fields,
            Class<?>[] types, int depth) {
        Schema.Type avroType = toSchemaType(types[0]);

        if (avroType == Schema.Type.ARRAY) {
            final Fields offsetSelector = fields.offsetSelector(
                    types.length - 1, 1);
            Class<?> arrayTypes[] = { types[1] };
            final Schema schema = createAvroSchema(recordName, offsetSelector,
                    arrayTypes, depth + 1);
            final Schema createArray = Schema.createArray(schema);
            return createArray;
        } else if (avroType == Schema.Type.MAP) {
            final Fields offsetSelector = fields.offsetSelector(
                    types.length - 1, 1);
            Class<?> mapTypes[] = { types[1] };
            return Schema.createMap(createAvroSchema(recordName,
                    offsetSelector, mapTypes, depth + 1));
        } else if (avroType == Schema.Type.RECORD) {
            final Fields offsetSelector = fields.offsetSelector(
                    types.length - 1, 1);
            return generateSchema(recordName, offsetSelector, types, depth + 1);
        } else if (avroType == Schema.Type.ENUM) {
            Class<?> clazz = types[0];
            Object[] names = clazz.getEnumConstants();
            List<String> enumNames = new ArrayList<String>(names.length);
            for (Object name : names) {
                enumNames.add(name.toString());
            }
            return Schema.createEnum(types[0].getName(), null, null, enumNames);
        } else {
            return Schema.create(avroType);
        }
    }

    private static void validateFields(Fields schemeFields,
            Class<?>[] schemeTypes) {
        if (schemeFields.size() == 0) {
            throw new IllegalArgumentException(
                    "There must be at least one field");
        }

        if (getSchemeTypesSize(schemeTypes) != schemeFields.size()) {
            throw new IllegalArgumentException(
                    "You must have a schemeType for every field");
        }

        for (int i = 0; i < schemeTypes.length; i++) {
            final Class<?> type = schemeTypes[i];
            final boolean b = type == List.class || type == Map.class;
            if (b && !isValidArrayType(schemeTypes[++i])) {
                throw new IllegalArgumentException(
                        "Only primitive types are allowed for an Array");
            }
        }
    }

    private static boolean isValidArrayType(Class<?> arrayType) {
        // only primitive types are allowed for arrays
        return arrayType == Boolean.class || arrayType == Integer.class
                || arrayType == Long.class || arrayType == Float.class
                || arrayType == Double.class || arrayType == String.class
                || arrayType == BytesWritable.class;
    }

    private static int getSchemeTypesSize(Class<?>[] schemeTypes) {
        int len = 0;
        for (int i = 0; i < schemeTypes.length; i++, len++) {
            final Class<?> type = schemeTypes[i];
            final boolean b = type == List.class || type == Map.class;
            if (b) {
                i++;
            }
        }
        return len;
    }

    private static final HashMap<Class<?>, Schema.Type> SCHEMA_TYPES = createTypeMap();

    public static Schema.Type toSchemaType(Class<?> type) {
        if (SCHEMA_TYPES.containsKey(type)) {
            return SCHEMA_TYPES.get(type);
        } else if (type.isEnum()) {
            return Schema.Type.ENUM;
        } else {
            throw new UnsupportedOperationException("The class type " + type
                    + " is currently unsupported");
        }
    }

    private static HashMap<Class<?>, Schema.Type> createTypeMap() {
        HashMap<Class<?>, Schema.Type> typeMap = new HashMap<Class<?>, Schema.Type>();
        typeMap.put(Integer.class, Schema.Type.INT);
        typeMap.put(Long.class, Schema.Type.LONG);
        typeMap.put(Boolean.class, Schema.Type.BOOLEAN);
        typeMap.put(Double.class, Schema.Type.DOUBLE);
        typeMap.put(Float.class, Schema.Type.FLOAT);
        typeMap.put(String.class, Schema.Type.STRING);
        typeMap.put(BytesWritable.class, Schema.Type.BYTES);

        // Note : Cascading field type for Array and Map is really a Tuple
        typeMap.put(List.class, Schema.Type.ARRAY);
        typeMap.put(Map.class, Schema.Type.MAP);

        // TODO - the following Avro Schema.Types are not handled as yet
        // FIXED
        // RECORD
        // UNION

        return typeMap;
    }
}