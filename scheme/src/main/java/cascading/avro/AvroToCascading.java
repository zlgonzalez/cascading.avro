/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.avro;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.io.BytesWritable;

import cascading.tuple.Tuple;

public class AvroToCascading {

    public static Object[] parseRecord(IndexedRecord record, Schema readerSchema) {

        Object[] result = new Object[readerSchema.getFields().size()];
        Schema writerSchema = record.getSchema();
        List<Field> schemaFields = readerSchema.getFields();
        for (int i = 0; i < schemaFields.size(); i++) {
            Field field = schemaFields.get(i);

            if (writerSchema.getField(field.name()) == null) {
         		result[i] = null;
            } else {
                Object obj = record.get(i);
                result[i] = fromAvro(obj, field.schema());
            }
        }
        return result;
    }
    
    protected static Object fromAvro(Object obj, Schema schema) {
    	if (obj == null) {
    		return null;
    	}
        switch (schema.getType()) {

            case UNION:
                return fromAvroUnion(obj, schema);

            case ARRAY:
                return fromAvroArray(obj, schema);

            case STRING:
            case ENUM:
                return obj.toString();

            case FIXED:
                return fromAvroFixed(obj, schema);
            case BYTES:
                return fromAvroBytes((ByteBuffer) obj);

            case RECORD:
                Object[] objs = parseRecord((IndexedRecord) obj, schema);
                Tuple result = new Tuple();
                result.addAll(objs);
                return result;

            case MAP:
                return fromAvroMap(obj, schema);

            case NULL:
            case BOOLEAN:
            case DOUBLE:
            case FLOAT:
            case INT:
            case LONG:
                return obj;

            default:
                throw new RuntimeException("Can't convert from type " + schema.getType().toString());

        }
    }

    protected static Object fromAvroFixed(Object obj, Schema schema) {
        Fixed fixed = (Fixed) obj;
        return new BytesWritable(fixed.bytes());
    }
    
    @SuppressWarnings("unchecked")
    protected static Object fromAvroMap(Object obj, Schema schema) {

        Map<String, Object> convertedMap = new HashMap<String, Object>();
        // CharSequence because the str can be configured as either Utf8 or String.
        for (Map.Entry<CharSequence, Object> e : ((Map<CharSequence, Object>) obj).entrySet()) {
            convertedMap.put(e.getKey().toString(), fromAvro(e.getValue(), schema.getValueType()));
        }
        return convertedMap;
    }

    protected static BytesWritable fromAvroBytes(ByteBuffer val) {
        BytesWritable result = new BytesWritable(val.array());
        return result;
    }

    @SuppressWarnings("rawtypes")
	protected static Object fromAvroArray(Object obj, Schema schema) {
        List<Object> array = new ArrayList<Object>();
        for (Object element : (GenericData.Array) obj) {
            array.add(fromAvro(element, schema.getElementType()));
        }
        return array;
    }

    protected static Object fromAvroUnion(Object obj, Schema schema) {
        List<Schema> types = schema.getTypes();
        if (types.size() < 1) {
            throw new AvroRuntimeException("Union has no types");
        }
        if (types.size() == 1) {
            return fromAvro(obj, types.get(0));
        } else if (types.size() > 2) {
            throw new AvroRuntimeException("Unions may only consist of a concrete type and null in cascading.avro");
        } else if (!types.get(0).getType().equals(Type.NULL) && !types.get(1).getType().equals(Type.NULL)) {
            throw new AvroRuntimeException("Unions may only consist of a concrete type and null in cascading.avro");
        } else {
            Integer concreteIndex = (types.get(0).getType() == Type.NULL) ? 1 : 0;
            return fromAvro(obj, types.get(concreteIndex));
        }
    }

}
