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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

/**
 * Class AvroSchemeTest
 */
public class AvroSchemeTest {
    @Rule
    public final TemporaryFolder tempDir = new TemporaryFolder();

    @Test
    public void testRoundTrip() throws Exception {
        final Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream(
                "test1.avsc"));
        final AvroScheme scheme = new AvroScheme(schema);

        final Fields fields = new Fields("aBoolean", "anInt", "aLong",
                "aDouble", "aFloat", "aBytes", "aFixed", "aNull", "aString",
                "aList", "aMap", "aUnion");

        final Hfs lfs = new Lfs(scheme, tempDir.getRoot().toString());
        HadoopFlowProcess writeProcess = new HadoopFlowProcess(new JobConf());
        final TupleEntryCollector collector = lfs.openForWrite(writeProcess);
        List<Integer> aList = new ArrayList<Integer>();
        Map<String, Integer> aMap = new HashMap<String, Integer>();
        aMap.put("one", 1);
        aMap.put("two", 2);

        aList.add(0);
        aList.add(1);
        BytesWritable bytesWritable = new BytesWritable(new byte[] {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16} );
        BytesWritable bytesWritable2 = new BytesWritable(new byte[] { 1,
                2, 3 });
        Tuple tuple = new Tuple(false, 1, 2L, 3.0, 4.0F, bytesWritable2,
                bytesWritable, null, "test-string", aList, aMap, 5);
        write(scheme, collector, new TupleEntry(fields, tuple));
        write(scheme, collector, new TupleEntry(fields, new Tuple(false, 1, 2L,
                3.0, 4.0F, new BytesWritable(new byte[0]), new BytesWritable(
                        new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4,
                                5, 6 }), null, "other string", aList, aMap, null)));
        collector.close();

        HadoopFlowProcess readProcess = new HadoopFlowProcess(new JobConf());
        final TupleEntryIterator iterator = lfs.openForRead(readProcess);
        assertTrue(iterator.hasNext());
        final TupleEntry readEntry1 = iterator.next();

        assertEquals(false, readEntry1.getBoolean(0));
        assertEquals(1, readEntry1.getInteger(1));
        assertEquals(2L, readEntry1.getLong(2));
        assertEquals(3.0, readEntry1.getDouble(3), 0.01);
        assertEquals(4.0F, readEntry1.getFloat(4), 0.01);
        assertEquals(bytesWritable2, readEntry1.getObject(5));
        assertEquals(bytesWritable, readEntry1.getObject(6));
        assertEquals("test-string", readEntry1.getString(8));
        assertEquals("0", ((List) readEntry1.getObject(9)).get(0)
                .toString());
        assertEquals(1,
                ((Map) readEntry1.getObject(10)).get("one"));
        assertTrue(iterator.hasNext());
        final TupleEntry readEntry2 = iterator.next();

        assertNull(readEntry2.get("aUnion"));
    }



    @Test
    public void listOrMapInsideListTest() throws Exception {
        final Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream(
                "test4.avsc"));
        final AvroScheme scheme = new AvroScheme(schema);

        final Fields fields = new Fields("aListOfListOfInt", "aListOfMapToLong");

        final Lfs lfs = new Lfs(scheme, tempDir.getRoot().toString());
        HadoopFlowProcess writeProcess = new HadoopFlowProcess(new JobConf());
        final TupleEntryCollector collector = lfs.openForWrite(writeProcess);

        List<Map<String, Long>> aListOfMapToLong = new ArrayList<Map<String, Long>>();
        Map<String, Long> aMapToLong = new HashMap<String, Long>();
        aMapToLong.put("one", 1L);
        aMapToLong.put("two", 2L);
        aListOfMapToLong.add(aMapToLong);

        List<List<Integer>> aListOfListOfInt = new ArrayList<List<Integer>>();
        List<Integer> aListOfInt = new LinkedList<Integer>();
        aListOfInt.add(0);
        aListOfInt.add(1);
        aListOfListOfInt.add(aListOfInt);

        write(scheme, collector, new TupleEntry(fields, new Tuple(
                aListOfListOfInt, aListOfMapToLong)));
        collector.close();

        HadoopFlowProcess readProcess = new HadoopFlowProcess(new JobConf());
        final TupleEntryIterator iterator = lfs.openForRead(readProcess);
        assertTrue(iterator.hasNext());
        final TupleEntry readEntry1 = iterator.next();

        List<Integer> outListOfInt = (List) ((List) readEntry1
                .getObject("aListOfListOfInt")).get(0);
        Map<Utf8, Long> outMapToLong = (Map) ((List) readEntry1
                .getObject("aListOfMapToLong")).get(0);

        assertEquals(Integer.valueOf(0), outListOfInt.get(0));
        assertEquals(Integer.valueOf(1), outListOfInt.get(1));
        assertEquals(Long.valueOf(1L), outMapToLong.get("one"));
        assertEquals(Long.valueOf(2L), outMapToLong.get("two"));
        assertTrue(!iterator.hasNext());

    }

    @Test
    public void listOrMapInsideMapTest() throws Exception {
        final Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream(
                "test3.avsc"));
        final AvroScheme scheme = new AvroScheme(schema);

        final Fields fields = new Fields("aMapToListOfInt", "aMapToMapToLong");

        final Lfs lfs = new Lfs(scheme, tempDir.getRoot().toString());
        HadoopFlowProcess writeProcess = new HadoopFlowProcess(new JobConf());
        final TupleEntryCollector collector = lfs.openForWrite(writeProcess);

        Map<String, Map<String, Long>> aMapToMapToLong = new HashMap<String, Map<String, Long>>();
        Map<String, Long> aMapToLong = new HashMap<String, Long>();
        aMapToLong.put("one", 1L);
        aMapToLong.put("two", 2L);
        aMapToMapToLong.put("key", aMapToLong);

        Map<String, List<Integer>> aMapToListOfInt = new HashMap<String, List<Integer>>();
        List<Integer> aListOfInt = new LinkedList<Integer>();
        aListOfInt.add(0);
        aListOfInt.add(1);
        aMapToListOfInt.put("key", aListOfInt);

        write(scheme, collector, new TupleEntry(fields, new Tuple(
                aMapToListOfInt, aMapToMapToLong)));
        collector.close();

        HadoopFlowProcess readProcess = new HadoopFlowProcess(new JobConf());
        final TupleEntryIterator iterator = lfs.openForRead(readProcess);
        assertTrue(iterator.hasNext());
        final TupleEntry readEntry1 = iterator.next();

        List<Integer> outListOfInt = (List) ((Map) readEntry1
                .getObject("aMapToListOfInt")).get("key");
        Map<String, Long> outMapToLong = (Map) ((Map) readEntry1
                .getObject("aMapToMapToLong")).get("key");

        assertEquals(Integer.valueOf(0), outListOfInt.get(0));
        assertEquals(Integer.valueOf(1), outListOfInt.get(1));
        assertEquals(Long.valueOf(1L), outMapToLong.get("one"));
        assertEquals(Long.valueOf(2L), outMapToLong.get("two"));
        assertTrue(!iterator.hasNext());

    }

    private void write(AvroScheme scheme, TupleEntryCollector collector,
            TupleEntry te) {
        collector.add(te.selectTuple(scheme.getSinkFields()));
    }

    @Test
    public void testSerialization() throws Exception {
        final Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream(
                "test1.avsc"));
        final AvroScheme expected = new AvroScheme(schema);

        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bytes);
        oos.writeObject(expected);
        oos.close();

        final ObjectInputStream iis = new ObjectInputStream(
                new ByteArrayInputStream(bytes.toByteArray()));
        final AvroScheme actual = (AvroScheme) iis.readObject();

        assertEquals(expected, actual);
    }
}
