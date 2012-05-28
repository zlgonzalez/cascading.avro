/*
 Copyright (c) 2008 Sven Duzont sven.duzont@gmail.com> All rights reserved.

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"),
 to deal in the Software without restriction, including without limitation
 the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is furnished
 to do so, subject to the following conditions: The above copyright notice
 and this permission notice shall be included in all
 copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS",
 WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
 TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
 OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package cascading.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

public class FieldsAvroSchemeTest {

    private String outDir;

    @Rule
    public final TemporaryFolder tempDir = new TemporaryFolder();

    @Before
    public void setup() throws IOException, InterruptedException {
        outDir = tempDir.getRoot().getAbsolutePath();
    }

    @Test
    public void testSchemeChecks() {

        try {
            new AvroScheme(new Fields("a", "b"), new Class[] { String.class,
                    Long.class });
        } catch (Exception e) {
            fail("Exception should not be thrown - this is the valid case");
        }

        try {
            new AvroScheme(new Fields(), new Class[] {});
            fail("Exception should be thrown when scheme field is empty");
        } catch (Exception e) {
        }

        try {
            new AvroScheme(new Fields("a", "b", "c"),
                    new Class[] { Integer.class });
            fail("Exception should be thrown as there are more fields defined than types");
        } catch (Exception e) {
        }

        try {
            new AvroScheme(new Fields("a"), new Class[] { Integer.class,
                    String.class });
            fail("Exception should be thrown as there are more types defined than fields");
        } catch (Exception e) {
        }

        try {
            new AvroScheme(new Fields("array"), new Class[] { List.class,
                    Long.class });
        } catch (Exception e) {
            fail("Exception shouldn't be thrown as array type is valid");
        }

        try {
            new AvroScheme(new Fields("array"), new Class[] { List.class,
                    List.class });
            fail("Exception should be thrown as array type isn't a primitive");
        } catch (Exception e) {
        }

    }

    @SuppressWarnings({ "serial", "rawtypes", "unchecked" })
    @Test
    public void testRoundTrip() throws Exception {

        // Create a scheme that tests each of the supported types

        final Fields testFields = new Fields("anInt", "aLong", "aBoolean",
                "aDouble", "aFloat", "aString", "aBytes", "aList", "aMap");
        final Class<?>[] schemeTypes = { Integer.class, Long.class,
                Boolean.class, Double.class, Float.class, String.class,
                BytesWritable.class, List.class, Long.class, Map.class,
                String.class };
        final String in = outDir + "testRoundTrip/in";
        final String out = outDir + "testRoundTrip/out";
        final String verifyout = outDir + "testRoundTrip/verifyout";

        final int numRecords = 2;

        // Create a sequence file with the appropriate tuples
        Lfs lfsSource = new Lfs(new SequenceFile(testFields), in,
                SinkMode.REPLACE);

        TupleEntryCollector write = lfsSource
                .openForWrite(new HadoopFlowProcess(new JobConf()));
        Tuple t = new Tuple();
        t.add(0);
        t.add(0L);
        t.add(false);
        t.add(0.0d);
        t.add(0.0f);
        t.add("0");
        addToTuple(t, new byte[] { 0 });

        List<Long> arrayOfLongs = new ArrayList<Long>() {

            {
                add(0L);
            }
        };
        Tuple listTuple = createList(arrayOfLongs);

        t.add(listTuple);

        Map<String, String> mapOfStrings = new HashMap<String, String>() {

            {
                put("key0", "value-0");
            }
        };
        Tuple mapTuple = createMap(mapOfStrings);

        t.add(mapTuple);

        // addToTuple(t, TestEnum.ONE);
        write.add(t);

        t = new Tuple();
        t.add(1);
        t.add(1L);
        t.add(true);
        t.add(1.0d);
        t.add(1.0f);
        t.add("1");
        addToTuple(t, new byte[] { 0, 1 });
        t.add(new Tuple(0L, 1L));
        t.add(new Tuple("key0", "value-0", "key1", "value-1"));
        // addToTuple(t, TestEnum.TWO);
        write.add(t);

        write.close();
        // Now read from the results, and write to an Avro file.
        Pipe writePipe = new Pipe("tuples to avro");

        Tap avroSink = new Lfs(new AvroScheme(testFields, schemeTypes), out);
        Flow flow = new HadoopFlowConnector().connect(lfsSource, avroSink,
                writePipe);
        flow.complete();
        flow.cleanup();

        // Now read it back in, and verify that the data/types match up.
        Tap avroSource = new Lfs(new AvroScheme(testFields, schemeTypes), out);
        Pipe readPipe = new Pipe("avro to tuples");
        Tap verifySink = new Hfs(new SequenceFile(testFields), verifyout,
                SinkMode.REPLACE);

        Flow readFlow = new HadoopFlowConnector().connect(avroSource,
                verifySink, readPipe);
        readFlow.complete();
        readFlow.cleanup();

        TupleEntryIterator sinkTuples = verifySink
                .openForRead(new HadoopFlowProcess(new JobConf()));
        assertTrue(sinkTuples.hasNext());
        int i = 0;
        while (sinkTuples.hasNext()) {
            TupleEntry te = sinkTuples.next();

            assertTrue(te.getObject("anInt") instanceof Integer);
            assertTrue(te.getObject("aLong") instanceof Long);
            assertTrue(te.getObject("aBoolean") instanceof Boolean);
            assertTrue(te.getObject("aDouble") instanceof Double);
            assertTrue(te.getObject("aFloat") instanceof Float);
            assertTrue(te.getObject("aString") instanceof String);
            assertTrue(te.getObject("aBytes") instanceof BytesWritable);
            assertTrue(te.getObject("aList") instanceof Tuple);
            assertTrue(te.getObject("aMap") instanceof Tuple);

            assertEquals(i, te.getInteger("anInt"));
            assertEquals(i, te.getLong("aLong"));
            assertEquals(i > 0, te.getBoolean("aBoolean"));
            assertEquals(i, te.getDouble("aDouble"), 0.0001);
            assertEquals(i, te.getFloat("aFloat"), 0.0001);
            assertEquals("" + i, te.getString("aString"));
            // assertEquals(i == 0 ? TestEnum.ONE : TestEnum.TWO,
            // TestEnum.valueOf(te.getString("anEnum")));

            int bytesLength = ((BytesWritable) te.getObject("aBytes"))
                    .getLength();
            byte[] bytes = ((BytesWritable) te.getObject("aBytes")).getBytes();
            assertEquals(i + 1, bytesLength);
            for (int j = 0; j < bytesLength; j++) {
                assertEquals(j, bytes[j]);
            }

            Tuple longArray = (Tuple) te.getObject("aList");
            assertEquals(i + 1, longArray.size());
            for (int j = 0; j < longArray.size(); j++) {
                assertTrue(longArray.getObject(j) instanceof Long);
                assertEquals(j, longArray.getLong(j));
            }

            Tuple stringMap = (Tuple) te.getObject("aMap");
            int numMapEntries = i + 1;
            assertEquals(2 * numMapEntries, stringMap.size());

            // Build a map from the data
            Map<String, String> testMap = new HashMap<String, String>();
            for (int j = 0; j < numMapEntries; j++) {
                assertTrue(stringMap.getObject(j * 2) instanceof String);
                String key = stringMap.getString(j * 2);
                assertTrue(stringMap.getObject(j * 2 + 1) instanceof String);
                String value = stringMap.getString(j * 2 + 1);
                testMap.put(key, value);
            }

            // Now make sure it has everything we're expecting.
            for (int j = 0; j < numMapEntries; j++) {
                assertEquals("value-" + j, testMap.get("key" + j));
            }

            i++;
        }
        sinkTuples.close();

        assertEquals(numRecords, i);

        // Ensure that the Avro file we write out is readable via the standard
        // Avro API
        File avroFile = new File(out + "/part-00000.avro");
        GenericDatumReader<Object> reader2 = new GenericDatumReader<Object>();
        DataFileReader<Object> reader = new DataFileReader<Object>(avroFile,
                reader2);
        i = 0;
        while (reader.hasNext()) {
            reader.next();
            i++;
        }
        reader.close();
        assertEquals(numRecords, i);

    }

    @Test
    public void testInvalidArrayData() {
        final Fields testFields = new Fields("aList");
        final Class<?>[] schemeTypes = { List.class, Long.class };

        final String in = outDir + "testInvalidArrayData/in";
        final String out = outDir + "testInvalidArrayData/out";

        // Create a sequence file with the appropriate tuples
        Lfs lfsSource = new Lfs(new SequenceFile(testFields), in,
                SinkMode.REPLACE);
        TupleEntryCollector write;
        try {
            HadoopFlowProcess process = new HadoopFlowProcess(new JobConf());
            write = lfsSource.openForWrite(process);
            Tuple t = new Tuple();
            t.add(new Tuple(0L, "invalid data type"));
            write.add(t);
            write.close();
            // Now read from the results, and write to an Avro file.
            Pipe writePipe = new Pipe("tuples to avro");

            Tap avroSink = new Lfs(new AvroScheme(testFields, schemeTypes), out);
            Flow flow = new HadoopFlowConnector().connect(lfsSource, avroSink,
                    writePipe);
            flow.complete();
            fail("Exception should be thrown as there is an invalid array element");

        } catch (Exception e) {
            // Ignore.
        }
    }

    @Test
    public void testInvalidMap() {
        final Fields testFields = new Fields("aMap");
        final Class<?>[] schemeTypes = { Map.class, String.class };

        final String in = outDir + "testInvalidMap/in";
        final String out = outDir + "testInvalidMap/out";

        // Create a sequence file with the appropriate tuples
        Lfs lfsSource = new Lfs(new SequenceFile(testFields), in,
                SinkMode.REPLACE);
        TupleEntryCollector write;
        try {
            HadoopFlowProcess process = new HadoopFlowProcess(new JobConf());
            write = lfsSource.openForWrite(process);
            Tuple t = new Tuple();
            // add invalid map data - where only key is present and no value
            t.add(new Tuple("key0", "value-0", "key1"));
            write.add(t);
            write.close();
            // Now read from the results, and write to an Avro file.
            Pipe writePipe = new Pipe("tuples to avro");

            Tap avroSink = new Lfs(new AvroScheme(testFields, schemeTypes), out);
            Flow flow = new HadoopFlowConnector().connect(lfsSource, avroSink,
                    writePipe);
            flow.complete();
            fail("Exception should be thrown as there is an invalid map");

        } catch (Exception e) {
            // Ignore.
        }
    }

    @Test
    public void testInvalidMapData() {
        final Fields testFields = new Fields("aMap");
        final Class<?>[] schemeTypes = { Map.class, String.class };

        final String in = outDir + "testInvalidMapData/in";
        final String out = outDir + "testInvalidMapData/out";

        // Create a sequence file with the appropriate tuples
        Lfs lfsSource = new Lfs(new SequenceFile(testFields), in,
                SinkMode.REPLACE);
        TupleEntryCollector write;
        try {
            HadoopFlowProcess process = new HadoopFlowProcess(new JobConf());
            write = lfsSource.openForWrite(process);
            Tuple t = new Tuple();
            // add invalid map data - key isn't a String
            t.add(new Tuple("key0", "value-0", 1L, "value-2"));
            write.add(t);
            write.close();
            // Now read from the results, and write to an Avro file.
            Pipe writePipe = new Pipe("tuples to avro");

            Tap avroSink = new Lfs(new AvroScheme(testFields, schemeTypes), out);
            Flow flow = new HadoopFlowConnector().connect(lfsSource, avroSink,
                    writePipe);
            flow.complete();
            fail("Exception should be thrown as the key isn't a String");

        } catch (Exception e) {
            // Ignore.
        }
    }

    @Test
    public void testNullable() throws Exception {
        final Fields testFields = new Fields("nullString");
        final Class<?>[] schemeTypes = { String.class };

        final String in = outDir + "testNullable/in";
        final String out = outDir + "testNullable/out";

        // Create a sequence file with the appropriate tuples
        Lfs lfsSource = new Lfs(new SequenceFile(testFields), in,
                SinkMode.REPLACE);

        HadoopFlowProcess process = new HadoopFlowProcess(new JobConf());
        TupleEntryCollector write = lfsSource.openForWrite(process);
        Tuple t = new Tuple();
        String nullString = null;
        t.add(nullString);
        write.add(t);
        write.close();
        // Now read from the results, and write to an Avro file.
        Pipe writePipe = new Pipe("tuples to avro");

        Tap avroSink = new Lfs(new AvroScheme(testFields, schemeTypes), out);
        Flow flow = new HadoopFlowConnector().connect(lfsSource, avroSink,
                writePipe);
        flow.complete();
    }

    @Test
    public void testSetRecordName() {
        final Fields fields = new Fields("a");
        final Class[] types = new Class[] { Long.class };
        String expected = "{\"type\":\"record\",\"name\":\"CascadingAvroSchema\",\"namespace\":\"\",\"doc\":\"auto generated\",\"fields\":[{\"name\":\"a\",\"type\":[\"null\",\"long\"],\"doc\":\"\"}]}";
        String jsonSchema = AvroHelper.getSchema(fields, types).toString();
        assertEquals(expected, jsonSchema);
        String jsonSchemaWithRecordName = AvroHelper.getSchema("FooBar",
                fields, types).toString();
        String expectedWithName = "{\"type\":\"record\",\"name\":\"FooBar\",\"namespace\":\"\",\"doc\":\"auto generated\",\"fields\":[{\"name\":\"a\",\"type\":[\"null\",\"long\"],\"doc\":\"\"}]}";
        assertEquals(expectedWithName, jsonSchemaWithRecordName);
    }

    @Test
    public void testEnumInSchema() throws Exception {
        final Fields fields = new Fields("a");
        final Class[] types = new Class[] { TestEnum.class };
        AvroScheme avroScheme = new AvroScheme(fields, types);
        String jsonSchema = getJsonSchema(fields, types);
        String enumField = String
                .format("{\"type\":\"enum\",\"name\":\"%s\",\"namespace\":\"%s\",\"symbols\":[\"ONE\",\"TWO\"]}",
                        "TestEnum", TestEnum.class.getPackage().getName());
        String expected = String
                .format("{\"type\":\"record\",\"name\":\"CascadingAvroSchema\",\"namespace\":\"\",\"doc\":\"auto generated\",\"fields\":[{\"name\":\"a\",\"type\":[\"null\",%s],\"doc\":\"\"}]}",
                        enumField);
        assertEquals(expected, jsonSchema);
    }

    public static void addToTuple(Tuple t, Enum e) {
        t.add(e.toString());
    }

    /**
     * @param list
     * @return
     */
    private static Tuple createList(List<?> list) {
        Tuple listTuple = new Tuple();
        for (Object item : list) {
            listTuple.add(item);
        }
        return listTuple;
    }

    /**
     * @param map
     * @return
     */
    private static Tuple createMap(Map<String, ?> map) {
        Tuple mapTuple = new Tuple();
        for (String key : map.keySet()) {
            mapTuple.add(key);
            mapTuple.add(map.get(key));
        }
        return mapTuple;
    }

    public static void addToTuple(Tuple t, byte[] bytes) {
        t.add(new BytesWritable(bytes));
    }

    /**
     * @return the JSON representation of the Avro schema that will be generated
     */
    public String getJsonSchema(Fields fields, Class<?>[] types) {
        // Since _schema is set up as transient generate the most current state
        Schema schema = AvroHelper.getSchema(fields, types);
        return schema.toString();
    }

}
