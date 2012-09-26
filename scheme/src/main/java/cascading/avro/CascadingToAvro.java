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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.BytesWritable;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class CascadingToAvro {

	public static Object[] parseTupleEntry(TupleEntry tuple, Schema writerSchema) {
		if (!(writerSchema.getFields().size() == tuple.size())) {
			throw new RuntimeException("Arity mismatch between incoming tuple and schema");
		}
		Object [] result = new Object[writerSchema.getFields().size()];
		
		Fields fields = tuple.getFields();
		List<Field> schemaFields = writerSchema.getFields();
		for (int i = 0; i < schemaFields.size(); i++) {
			Field field = schemaFields.get(i);
			
//			if (!fields.contains(new Fields(field.name()))) {
//				System.out.println(fields);
//				throw new RuntimeException("Tuple doesn't contain field: "+ field.name());
//			}
			Object obj = tuple.getTuple().getObject(i);
			result[i] = toAvro(obj, field.schema());
			
		}
		return result;
	}

	protected static Object toAvro(Object obj, Schema schema) {
		switch(schema.getType()) {
		
		case ARRAY: 
			return toAvroArray(obj, schema);
		
		case STRING:
		case ENUM:
			return obj.toString();

		case FIXED:
			return toAvroFixed(obj, schema);
		case BYTES:
			return toAvroBytes(obj);
			
		case RECORD: 
			Object [] objs = parseTupleEntry((TupleEntry) obj, schema);
			Record record = new Record(schema);
			for(int i=0; i < objs.length; i++ ) record.put(i,  objs[i]);
			return record;
			
		case MAP:
			return toAvroMap(obj, schema);
			
		case UNION:	
			return toAvroUnion(obj, schema);
					
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
	


	protected static Object toAvroFixed(Object obj, Schema schema) {
		BytesWritable bytes = (BytesWritable) obj;
		Fixed fixed = new Fixed(schema, bytes.getBytes());
		return fixed;
	}

	@SuppressWarnings("unchecked")
	protected static Object toAvroMap(Object obj, Schema schema) {
	
        Map<String, Object> convertedMap = new HashMap<String, Object>();
        for (Map.Entry<String, Object> e : ((Map<String, Object>) obj).entrySet()) {
            convertedMap.put(e.getKey(), toAvro(e.getValue(), schema.getValueType()));
        }
        return convertedMap;
    }

	protected static Object toAvroBytes(Object obj) {
        BytesWritable inBytes = (BytesWritable) obj;
        ByteBuffer buffer = ByteBuffer.wrap(inBytes.getBytes());
        return buffer;
    }

	protected static Object toAvroArray(Object obj, Schema schema) {
		if (obj instanceof Iterable) {
			Schema elementSchema = schema.getElementType();
			List<Object> array = new ArrayList<Object>();
			for( Object element : (Iterable<Object>) obj) {
				array.add(toAvro(element, elementSchema));
			}
			return new GenericData.Array(schema, array);
		}
		else throw new RuntimeException("Can't convert from non-iterable to array");
	}

	protected static Object toAvroUnion(Object obj, Schema schema) {
		if (obj == null) return obj;
		
		List<Schema> types = schema.getTypes();
		if (types.size() < 1) {
			throw new AvroRuntimeException("Union in writer schema has no types");
		}
		if (types.size() == 1) {
			return toAvro(obj, types.get(0));
		}
		else if (types.size() > 2) {
			throw new AvroRuntimeException("Unions may only consist of a concrete type and null in cascading.avro");
		}
		else if (!types.get(0).getType().equals(Type.NULL) && !types.get(1).getType().equals(Type.NULL)){
			throw new AvroRuntimeException("Unions may only consist of a concrete type and null in cascading.avro");
		}
		else {
			Integer concreteIndex = (types.get(0).getType() == Type.NULL) ? 1 : 0;
			return toAvro(obj, types.get(concreteIndex));
		}
	}
	
	protected static Schema generateAvroSchemaFromTupleEntry(TupleEntry tupleEntry, String recordName, boolean isNullable) {
		Fields tupleFields = tupleEntry.getFields();
		int numFields = tupleFields.size();
		List<Field> avroFields = new ArrayList<Field>();
		for( Comparable fieldName : tupleFields) {
			if (!(fieldName instanceof String)) throw new RuntimeException("Can't generate schema from non-string named fields");
			Schema fieldSchema = generateAvroSchemaFromElement(tupleEntry.getObject(fieldName), (String) fieldName, isNullable);
			avroFields.add(new Field((String) fieldName, fieldSchema, null, null));
		}
		
		Schema outputSchema = Schema.createRecord(recordName, "auto-generated by cascading.avro", null, false);
		outputSchema.setFields(avroFields);
		
		return outputSchema;
	}
	
	protected static Schema generateAvroSchemaFromElement(Object element, String name, boolean isNullable) {
		if (element == null) throw new RuntimeException("Can't infer schema from null valued element");
		else if (isNullable) return generateUnionSchema(element, name);
		else if (element instanceof TupleEntry) return generateAvroSchemaFromTupleEntry((TupleEntry) element, name, isNullable);
		else if (element instanceof Map) return generateAvroSchemaFromMap((Map<String, Object>) element, name);
		else if (element instanceof Iterable) return generateAvroSchemaFromIterable((Iterable) element, name);
		else if (element instanceof BytesWritable) return Schema.create(Schema.Type.BYTES);
		else if (element instanceof String) return Schema.create(Schema.Type.STRING);
		else if (element instanceof Double) return Schema.create(Schema.Type.DOUBLE);
		else if (element instanceof Float) return Schema.create(Schema.Type.FLOAT);
		else if (element instanceof Integer) return Schema.create(Schema.Type.INT);
		else if (element instanceof Long) return Schema.create(Schema.Type.LONG);
		else if (element instanceof Boolean) return Schema.create(Schema.Type.BOOLEAN);
		else throw new RuntimeException("Can't create schema from type " + element.getClass());
		
	}

	private static Schema generateAvroSchemaFromIterable(Iterable element, String name) {
		Iterator<Object> iterator = element.iterator();
		if (! iterator.hasNext()) throw new RuntimeException("Can't infer list schema from empty iterable");
		else {
			Schema itemSchema = generateAvroSchemaFromElement(iterator.next(), name + "ArrayElement", false);
			Schema arraySchema = Schema.createArray(itemSchema);
			return arraySchema;
		}
		
	}

	private static Schema generateAvroSchemaFromMap(Map<String, Object> element, String name) {
		if(element.isEmpty()) throw new RuntimeException("Can't infer map schema from empty map");
		else {
			Iterator<Object> iterator = element.values().iterator();
			Schema valueSchema = generateAvroSchemaFromElement(iterator.next(), name + "MapValue", false);
			Schema mapSchema = Schema.createMap(valueSchema);
			return mapSchema;
		}
		
	}

	private static Schema generateUnionSchema(Object element, String name) {
		List<Schema> types = new ArrayList<Schema>();
		types.add(Schema.create(Schema.Type.NULL));
		types.add(generateAvroSchemaFromElement(element, name, false));
		Schema unionSchema = Schema.createUnion(types);
		return unionSchema;
	}
	

}
