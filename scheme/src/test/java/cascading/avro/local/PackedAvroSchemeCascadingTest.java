package cascading.avro.local;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.Test;

import cascading.avro.aggregator.TopPojoAggregator;
import cascading.avro.pojo.EmbeddedPojo;
import cascading.avro.pojo.TopPojo;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.expression.ExpressionFunction;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

/**
 * Tests to demonstrate using the PackedAvroScheme in a Cascading flow when you
 * want to serialize / deserialize Pojos created via the Avro code generator.
 */
public class PackedAvroSchemeCascadingTest {

    /**
     * Reads from a csv file and writes to Avro format by serializing an pojo
     * with an embedded pojo, which was created by the Avro code generator
     * (using the avro-maven-plugin)
     * 
     * @throws IOException
     */
    @Test
    public void testPackedAvroSchemeSerialization() throws IOException {
        /* ---[ Do Cascading Flow in Local Model ]--- */

        String topPath = "src/test/resources/cascading/data/toppojo.input.txt";
        String embPath = "src/test/resources/cascading/data/embeddedpojo.input.txt";
        String avroOutPath = "target/output/toppojo.avro";

        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, PackedAvroSchemeCascadingTest.class);
        LocalFlowConnector flowConnector = new LocalFlowConnector(properties);

        Schema payerSchema = TopPojo.getClassSchema();

        // 'Id' is the join key between these two inputs
        Fields topFields = new Fields("Id", "State");
        Fields embeddedFields = new Fields("IdFK", "Age", "City", "LastName");

        // input taps
        Tap<?, ?, ?> topTap = new FileTap(new TextDelimited(topFields, false, false, ","), topPath);
        Tap<?, ?, ?> embTap = new FileTap(new TextDelimited(embeddedFields, false, false, ","), embPath);

        Pipe topPipe = new Pipe("top");
        Pipe embPipe = new Pipe("embedded");

        Pipe joinPipe = new CoGroup(topPipe, new Fields("Id"), embPipe, new Fields("IdFK"));
        int numIncomingFields = topFields.size() + embeddedFields.size();
        Fields outputFields = new Fields("toppojo");
        joinPipe = new Every(joinPipe, Fields.ALL, new TopPojoAggregator(numIncomingFields, outputFields),
                Fields.RESULTS);

        // sink tap
        PackedAvroScheme<TopPojo> packedAvroScheme = new PackedAvroScheme<TopPojo>(payerSchema);
        Tap<?, ?, ?> avroOutTap = new FileTap(packedAvroScheme, avroOutPath, SinkMode.REPLACE);

        FlowDef flowDef = FlowDef.flowDef().setName("transform to Avro").addSource(topPipe, topTap)
                .addSource(embPipe, embTap).addTailSink(joinPipe, avroOutTap);

        Flow<?> flow = flowConnector.connect(flowDef);
        flow.complete();

        /* ---[ Validate the output ]--- */

        File avroOutFile = new File(avroOutPath);
        assertTrue(avroOutFile.exists());

        // read in input as TopPojo objects using Avro readers
        DatumReader<TopPojo> ptRdr = new SpecificDatumReader<TopPojo>(TopPojo.class);
        DataFileReader<TopPojo> ptFileRdr = new DataFileReader<TopPojo>(avroOutFile, ptRdr);

        // should be two entries
        // entry 1
        assertTrue(ptFileRdr.hasNext());
        TopPojo first = ptFileRdr.next();
        assertNotNull(first);
        assertEquals(Integer.valueOf(1), first.getId());
        assertEquals("state1", first.getState().toString());
        List<EmbeddedPojo> lsEmb = first.getThings();
        assertEquals(2, lsEmb.size());
        assertEquals("age1-1", lsEmb.get(0).getAge().toString());
        assertEquals("city1-1", lsEmb.get(0).getCity().toString());
        assertEquals("name1-1", lsEmb.get(0).getLastName().toString());
        assertEquals("age1-2", lsEmb.get(1).getAge().toString());
        assertEquals("city1-2", lsEmb.get(1).getCity().toString());
        assertEquals("name1-2", lsEmb.get(1).getLastName().toString());

        // entry 2
        assertTrue(ptFileRdr.hasNext());
        TopPojo second = ptFileRdr.next();
        assertNotNull(second);
        assertEquals(Integer.valueOf(2), second.getId());
        assertEquals("state2", second.getState().toString());
        List<EmbeddedPojo> lsEmb2 = second.getThings();
        assertEquals(1, lsEmb2.size());
        assertEquals("age2", lsEmb2.get(0).getAge().toString());
        assertEquals("city2", lsEmb2.get(0).getCity().toString());
        assertEquals("name2", lsEmb2.get(0).getLastName().toString());

        assertFalse(ptFileRdr.hasNext());
    }

    /**
     * Reads from an Avro file of serialized TopPojo objects, converts them to
     * JSON and writes the JSON out as a text file.
     * 
     * @throws IOException
     */
    @Test
    public void testPackedAvroSchemeDeserialization() throws IOException {
        /* ---[ Do Cascading Flow in Local Model ]--- */

        // this avro file has the same contents as the one generated in the
        // Serialization test method above
        String avroInputPath = "src/test/resources/cascading/data/toppojo.avro";
        String jsonOutPath = "target/output/toppojo.json";

        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, PackedAvroSchemeCascadingTest.class);
        LocalFlowConnector flowConnector = new LocalFlowConnector(properties);

        Schema payerSchema = TopPojo.getClassSchema();

        // input tap (reads Avro formatted files and hydrates into TopPojo Java
        // object
        PackedAvroScheme<TopPojo> packedAvroScheme = new PackedAvroScheme<TopPojo>(payerSchema);
        Tap<?, ?, ?> avroInTap = new FileTap(packedAvroScheme, avroInputPath);

        Fields inputFields = new Fields("toppojo-as-avro");
        Fields outputFields = new Fields("toppojo-as-json");

        Pipe p = new Pipe("TopPojo Avro input");
        p = new Each(p, new ExpressionFunction(inputFields, "$0.toString()", TopPojo.class));

        Tap<?, ?, ?> outTap = new FileTap(new TextLine(outputFields), jsonOutPath);

        FlowDef flowDef = FlowDef.flowDef().setName("transform Avro to JSON").addSource(p, avroInTap)
                .addTailSink(p, outTap);

        Flow<?> flow = flowConnector.connect(flowDef);
        flow.complete();

        /* ---[ Verify Output ]--- */

        File outFile = new File(jsonOutPath);
        assertTrue(outFile.exists());
        BufferedReader br = new BufferedReader(new FileReader(outFile));
        String line1 = br.readLine();
        assertNotNull(line1);
        assertTrue(Pattern.compile("\"Age\"\\s*:\\s*\"age1-1\",\\s*\"City\"\\s*:\\s*\"city1-1\"").matcher(line1).find());
        assertTrue(Pattern.compile("\"Age\"\\s*:\\s*\"age1-2\",\\s*\"City\"\\s*:\\s*\"city1-2\"").matcher(line1).find());
        assertTrue(Pattern.compile("\"Id\":\\s*1,\\s*\"State\"\\s*:\\s*\"state1\"").matcher(line1).find());

        String line2 = br.readLine();
        assertNotNull(line2);
        assertTrue(Pattern.compile("\"Age\"\\s*:\\s*\"age2\",\\s*\"City\"\\s*:\\s*\"city2\"").matcher(line2).find());
        assertTrue(Pattern.compile("\"Id\":\\s*2,\\s*\"State\"\\s*:\\s*\"state2\"").matcher(line2).find());

        String line3 = br.readLine();
        assertNull(line3);

        br.close();
    }
}
