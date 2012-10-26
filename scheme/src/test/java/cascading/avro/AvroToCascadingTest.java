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

import static org.junit.Assert.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.BytesWritable;
import org.hamcrest.core.IsNull;
import org.junit.Before;
import org.junit.Test;

public class AvroToCascadingTest {

    Record record = null;

    @Before
    public void setUp() throws Exception {
        Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("test5.avsc"));
        record = new Record(schema);
        record.put(0, false);
        record.put(1, 10);
        record.put(2, 5L);
        record.put(3, 0.6f);
        record.put(4, 1.01);
        record.put(5, "This is my string");
        byte[] buffer_value = { 0, 1, 2, 3, 0, 0, 0 };
        ByteBuffer buffer = ByteBuffer.wrap(buffer_value);
        record.put(6, buffer);
        Fixed fixed = new Fixed(schema.getField("aFixed").schema(), buffer_value);
        record.put(7, fixed);
        Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.INT));
        GenericArray<Integer> array = new GenericData.Array<Integer>(1, arraySchema);
        array.add(0);
        array.add(1);
        record.put(8, array);
        Map<Utf8, Integer> myMap = new HashMap<Utf8, Integer>();
        myMap.put(new Utf8("one"), 1);
        myMap.put(new Utf8("two"), 2);
        record.put(9, myMap);
        record.put(10, 5);
    }

    @Test
    public void testFromArrayPrimitive() {
        Schema fieldSchema = record.getSchema().getField("aList").schema();
        List<Integer> outList = (List<Integer>) AvroToCascading.fromAvro(record.get(8), fieldSchema);

        assertThat(outList.get(0), is(0));
        assertThat(outList.get(1), is(1));
    }

    @Test
    public void testFromArrayNested() {
        Schema innerSchema = Schema.createArray(Schema.create(Schema.Type.INT));
        Schema outerSchema = Schema.createArray(innerSchema);
        GenericArray<GenericArray<Integer>> array = new GenericData.Array<GenericArray<Integer>>(1, outerSchema);
        GenericArray<Integer> innerArray = new GenericData.Array<Integer>(1, innerSchema);
        innerArray.add(0);
        innerArray.add(1);
        array.add(innerArray);

        List<List<Integer>> outList = (List<List<Integer>>) AvroToCascading.fromAvro(array, outerSchema);

        assertThat(outList.get(0), is((List<Integer>) innerArray));
        assertThat(outList.get(0).get(0), is(0));
        assertThat(outList.get(0).get(1), is(1));
    }

    @Test
    public void testFromMapPrimitive() {
        Schema fieldSchema = record.getSchema().getField("aMap").schema();
        Map<String, Integer> outMap = (Map<String, Integer>) AvroToCascading.fromAvro(record.get(9), fieldSchema);

        assertThat(outMap.get("one"), is(1));
        assertThat(outMap.get("two"), is(2));
    }

    @Test
    public void testFromMapNested() {
        Schema innerSchema = Schema.createMap(Schema.create(Schema.Type.LONG));
        Schema outerSchema = Schema.createMap(innerSchema);
        Map<Utf8, Long> innerMap = new HashMap<Utf8, Long>();
        innerMap.put(new Utf8("one"), 1L);
        innerMap.put(new Utf8("two"), 2L);
        Map<Utf8, Map<Utf8, Long>> outerMap = new HashMap<Utf8, Map<Utf8, Long>>();
        outerMap.put(new Utf8("map1"), innerMap);
        Map<String, Map<String, Long>> outMap = (Map<String, Map<String, Long>>) AvroToCascading.fromAvro(outerMap,
                outerSchema);

        assertThat(outMap.get("map1").get("two"), is(2L));
        assertThat(outMap.get("map1").get("one"), is(1L));
    }

    @Test
    public void testFromBytes() {
        Schema fieldSchema = record.getSchema().getField("aBytes").schema();
        byte[] buffer_value = { 0, 1, 2, 3, 0, 0, 0 };
        BytesWritable result = new BytesWritable(buffer_value);

        BytesWritable outBytes = (BytesWritable) AvroToCascading.fromAvro(record.get("aBytes"), fieldSchema);

        assertThat(outBytes, is(result));
    }

    @Test
    public void testFromFixed() {
        Schema fieldSchema = record.getSchema().getField("aFixed").schema();
        byte[] buffer_value = { 0, 1, 2, 3, 0, 0, 0 };
        BytesWritable result = new BytesWritable(buffer_value);

        BytesWritable outBytes = (BytesWritable) AvroToCascading.fromAvro(record.get("aFixed"), fieldSchema);

        assertThat(outBytes, is(result));
    }

    @Test
    public void testFromUnion() {
        Schema fieldSchema = record.getSchema().getField("aUnion").schema();
        Integer outInt = (Integer) AvroToCascading.fromAvro(record.get(10), fieldSchema);

        assertThat(outInt, is(5));
    }

    @Test
    public void testParseRecord() {
        byte[] buffer_value = { 0, 1, 2, 3, 0, 0, 0 };
        BytesWritable bwritable = new BytesWritable(buffer_value);

        Object[] output = AvroToCascading.parseRecord(record, record.getSchema());

        assertThat(output.length, is(11));
        assertThat((Boolean) output[0], is(false));
        assertThat((Integer) output[1], is(10));
        assertThat((Long) output[2], is(5L));
        assertThat((Float) output[3], is(0.6f));
        assertThat((Double) output[4], is(1.01));
        assertThat((String) output[5], is("This is my string"));
        assertThat((BytesWritable) output[6], is(bwritable));
        assertThat((BytesWritable) output[7], is(bwritable));
        List<Integer> outList = (List<Integer>) output[8];
        assertThat(outList.get(0), is(0));
        assertThat(outList.get(1), is(1));
        Map<String, Integer> outMap = (Map<String, Integer>) output[9];
        assertThat(outMap.get("one"), is(1));
        assertThat(outMap.get("two"), is(2));
        assertThat((Integer) output[10], is(5));
    }

}
