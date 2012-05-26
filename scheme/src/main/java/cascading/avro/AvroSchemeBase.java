/*
 * Copyright (c) 2012 MaxPoint Interactive, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cascading.avro;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericFixed;
import org.apache.hadoop.io.BytesWritable;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Class AvroAdapterBase
 */

public abstract class AvroSchemeBase<Config, Input, Output, SourceContext, SinkContext>
        extends Scheme<Config, Input, Output, SourceContext, SinkContext> {
    private static final long serialVersionUID = -4209069134414384317L;
    public static final EnumSet<Schema.Type> ALLOWED_TYPES = EnumSet.of(
            Schema.Type.BOOLEAN, Schema.Type.BYTES, Schema.Type.DOUBLE,
            Schema.Type.FIXED, Schema.Type.FLOAT, Schema.Type.INT,
            Schema.Type.LONG, Schema.Type.NULL, Schema.Type.STRING,
            Schema.Type.UNION, Schema.Type.ARRAY, Schema.Type.MAP);

    /**
     * @param dataSchema
     */
    public AvroSchemeBase(Schema dataSchema) {
        this.dataSchema = dataSchema;
        final LinkedHashMap<String, FieldType> schemaFields = parseSchema(
                dataSchema, ALLOWED_TYPES);

        final Fields fields = fields(schemaFields);
        setSinkFields(fields);
        setSourceFields(fields);

        final Collection<FieldType> types = schemaFields.values();
        fieldTypes = types.toArray(new FieldType[types.size()]);

    }

    public AvroSchemeBase() {
    }

    /**
     * Extracts serialization info from Avro schema
     */
    protected static LinkedHashMap<String, FieldType> parseSchema(
            Schema avroSchema, Set<Schema.Type> allowedTypes) {
        if (avroSchema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException(
                    "Base schema must be of type RECORD, found "
                            + avroSchema.getType());
        }

        final LinkedHashMap<String, FieldType> fields = new LinkedHashMap<String, FieldType>();
        final List<Schema.Field> schemaFields = avroSchema.getFields();
        for (Schema.Field field : schemaFields) {
            final Schema.Type type = field.schema().getType();
            if (!allowedTypes.contains(type)) {
                throw new IllegalArgumentException(
                        "Don't know how to handle schema with " + field.name()
                                + " of type " + type);
            }
            fields.put(field.name(), typeInfo(field));
        }
        return fields;
    }

    private static FieldType typeInfo(Schema.Field field) {
        final Schema schema = field.schema();
        final Schema.Type type = schema.getType();
        // special case [type, null] unions
        if (type == Schema.Type.UNION) {
            return new FieldType(resolveUnion(schema), true, field.pos(),
                    schema);
        } else {
            return new FieldType(type, type == Schema.Type.NULL, field.pos(),
                    schema);
        }
    }

    protected static Schema.Type resolveUnion(Schema schema) {
        final List<Schema> components = schema.getTypes();
        if (components.size() == 2) {
            final Schema s0 = components.get(0), s1 = components.get(1);
            if (s0.getType() == Schema.Type.NULL) {
                return s1.getType();
            } else if (s1.getType() == Schema.Type.NULL) {
                return s0.getType();
            }
        }
        throw new IllegalArgumentException("Can't parse " + schema);
    }

    protected static Class<?>[] inferClasses(Collection<FieldType> types) {
        Class<?>[] result = new Class<?>[types.size()];
        int ix = 0;
        for (FieldType typeInfo : types) {
            result[ix++] = inferClass(typeInfo);
        }
        return result;
    }

    protected static Class<?> inferClass(FieldType typeInfo) {
        switch (typeInfo.type) {
        case BOOLEAN:
            return Boolean.class;
        case BYTES:
            return BytesWritable.class;
        case DOUBLE:
            return Double.class;
        case FIXED:
            return BytesWritable.class;
        case FLOAT:
            return Float.class;
        case INT:
            return Integer.class;
        case LONG:
            return Long.class;
        case NULL:
            return Object.class;
        case STRING:
            return String.class;
        case MAP:
            return Map.class;
        case ARRAY:
            return List.class;
        }
        throw new IllegalArgumentException("Can't resolve " + typeInfo.type
                + " to java class");
    }

    public final static Fields fields(
            LinkedHashMap<String, FieldType> schemaFields) {
        final Set<String> names = schemaFields.keySet();
        return new Fields(names.toArray(new String[names.size()]));
    }

    static Schema readSchema(ObjectInputStream in) throws IOException {
        return Schema.parse(in.readUTF());
    }

    protected final static class FieldType implements Serializable {
        private static final long serialVersionUID = -8008959529050821547L;
        public boolean isNullable;
        public Schema.Type type;
        public Schema schema;
        public int pos;

        private FieldType(Schema.Type type, boolean nullable, int pos,
                Schema schema) {
            this.type = type;
            isNullable = nullable;
            this.pos = pos;
            this.schema = schema;
        }

        private void writeObject(java.io.ObjectOutputStream out)
                throws IOException {
            out.writeBoolean(isNullable);
            out.writeObject(type);
            out.writeInt(pos);
            out.writeUTF(schema.toString());
        }

        private void readObject(java.io.ObjectInputStream in)
                throws IOException, ClassNotFoundException {
            isNullable = in.readBoolean();
            type = (Schema.Type) in.readObject();
            pos = in.readInt();
            schema = readSchema(in);
        }
    }

    protected Schema dataSchema;
    protected FieldType[] fieldTypes;

    protected final Object fromAvro(FieldType typeInfo, Object val) {
        if (val == null) {
            return null;
        }
        switch (typeInfo.type) {
        case STRING:
            return val.toString();
        case FIXED:
            return new BytesWritable(((GenericFixed) val).bytes());
        case BYTES:
            return bytesWritable((ByteBuffer) val);
        }
        return val;
    }

    private final BytesWritable bytesWritable(ByteBuffer val) {
        final byte[] data = new byte[val.remaining()];
        val.get(data);
        return new BytesWritable(data);
    }

    protected final Object toAvro(Comparable<?> field, FieldType typeInfo,
            Object val) throws IOException {
        if (val == null) {
            if (typeInfo.isNullable) {
                return null;
            } else {
                throw new NullPointerException("Field " + field
                        + " is not nullable");
            }
        }

        switch (typeInfo.type) {
        case STRING:
            return val.toString();
        case FIXED:
            return new GenericData.Fixed(typeInfo.schema,
                    ((BytesWritable) val).getBytes());
        case BYTES:
            return ByteBuffer.wrap(((BytesWritable) val).getBytes());
        case LONG:
            return ((Number) val).longValue();
        case INT:
            return ((Number) val).intValue();
        case DOUBLE:
            return ((Number) val).doubleValue();
        case FLOAT:
            return ((Number) val).floatValue();

        }
        return val;
    }

    private final void writeObject(java.io.ObjectOutputStream out)
            throws IOException {
        out.writeObject(this.fieldTypes);
        out.writeUTF(this.dataSchema.toString());
    }

    private final void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        this.fieldTypes = (FieldType[]) in.readObject();
        this.dataSchema = AvroSchemeBase.readSchema(in);
    }

    @Override
    public void sinkConfInit(FlowProcess<Config> flowProcess,
            Tap<Config, Input, Output> tap, Config conf) {
    }

    /**
     * @param call
     * @param record
     * @return
     */
    protected boolean read(SourceCall<?, ?> call, final Record record) {
        final Tuple result = Tuple.size(getSourceFields().size());
        for (int i = 0; i < fieldTypes.length; i++) {
            final Object val = fromAvro(fieldTypes[i],
                    record.get(fieldTypes[i].pos));
            result.set(i, val);
        }
        TupleEntry incomingEntry = call.getIncomingEntry();
        incomingEntry.setTuple(result);
        return true;
    }

    /**
     * @param call
     * @return
     * @throws IOException
     */
    protected Record write(SinkCall<?, ?> call) throws IOException {
        Record record = new Record(dataSchema);
        TupleEntry tupleEntry = call.getOutgoingEntry();
        final Fields sinkFields = getSinkFields();
        for (int i = 0; i < fieldTypes.length; i++) {
            final Comparable<?> field = sinkFields.get(i);
            final Object val = tupleEntry.getObject(field);
            record.put(fieldTypes[i].pos, toAvro(field, fieldTypes[i], val));
        }
        return record;
    }

}
