package cascading.avro;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


@SuppressWarnings("unchecked")
public class PackedAvroSchemeTest extends Assert {

  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  @Test
  public void testPackedAvroRecord() throws Exception {
    final Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream(
        "test2.avsc"));
    String in = tempDir.getRoot().toString() + "/recordtest/in";
    String out = tempDir.getRoot().toString() + "/recordtest/out";
    Tap lfsSource = new Lfs(new PackedAvroScheme<GenericData.Record>(schema), in, SinkMode.REPLACE);
    TupleEntryCollector write = lfsSource.openForWrite(new HadoopFlowProcess());
    Tuple tuple = Tuple.size(1);
    GenericData.Record record = new GenericData.Record(schema);
    String innerSchema = "{\"type\":\"record\", \"name\":\"nested\", \"namespace\": \"cascading.avro\", \"fields\": [{\"name\":\"anInnerField1\", \"type\":\"int\"}, {\"name\":\"anInnerField2\", \"type\":\"string\"} ] }";
    GenericData.Record inner = new GenericData.Record(new Schema.Parser().parse(innerSchema));
    inner.put(0, 0);
    inner.put(1,"the string");
    record.put(0, inner);
    record.put(1,"outer string");
    tuple.set(0,record);
    TupleEntry te = new TupleEntry(new Fields("test2"), tuple);
    write.add(te);
    write.close();


    Pipe writePipe = new Pipe("tuples to avro");

    Tap avroSink = new Lfs(new PackedAvroScheme<GenericData.Record>(schema), out);
    Flow flow = new HadoopFlowConnector().connect(lfsSource, avroSink, writePipe);
    flow.complete();

    // Now read it back in, and verify that the data/types match up.
    Tap avroSource = new Lfs(new PackedAvroScheme<GenericData.Record>(schema), out);


    TupleEntryIterator iterator = avroSource.openForRead(new HadoopFlowProcess());

    assertTrue(iterator.hasNext());
    final TupleEntry readEntry1 = iterator.next();

    assertTrue(readEntry1.getObject(0) instanceof GenericData.Record);
    assertEquals(record.get(0), ((GenericData.Record) readEntry1.getObject(0)).get(0));
    assertEquals(new Utf8((String)record.get(1)), ((GenericData.Record) readEntry1.getObject(0)).get(1));


  }
}
