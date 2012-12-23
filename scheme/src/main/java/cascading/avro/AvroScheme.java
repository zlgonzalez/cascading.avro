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

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.CompositeTap;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.*;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;


public class AvroScheme extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {

  protected Schema schema;
  private static final String DEFAULT_RECORD_NAME = "CascadingAvroRecord";


  /**
   * Constructor to read from an Avro source without specifying the schema. If this is used as a source Scheme
   * a runtime error will be thrown.
   */
  public AvroScheme() {
    this(null);
  }

  /**
   * Create a new Cascading 2.0 scheme suitable for reading and writing data using the Avro serialization format.
   * Note that if schema is null, the Avro schema will be inferred from one of the source files (if this scheme
   * is being used as a source) or it will be inferred from the first output TupleEntry written (if this scheme
   * is being used as a sink). In this latter case, every field value in the first output TupleEntry must be
   * non-null, otherwise an exception will be thrown.
   *
   * @param fields Fields object from cascading
   * @param types  array of Class types
   */
  public AvroScheme(Fields fields, Class<?>[] types) {
    this(CascadingToAvro.generateAvroSchemaFromFieldsAndTypes(DEFAULT_RECORD_NAME, fields, types));
  }


  /**
   * Create a new Cascading 2.0 scheme suitable for reading and writing data using the Avro serialization format.
   * If this is used as a source Scheme a runtime error will be thrown.
   *
   * @param schema     Avro schema, or null if this is to be inferred from source file/outgoing TupleEntry data.
   */
  public AvroScheme(Schema schema) {
    this.schema = schema;

    if (schema == null) {
      setSinkFields(Fields.ALL);
      setSourceFields(Fields.UNKNOWN);
    } else {
      Fields cascadingFields = new Fields();
      for (Field avroField : schema.getFields()) {
        cascadingFields = cascadingFields.append(new Fields(avroField.name()));
      }
      setSinkFields(cascadingFields);
      setSourceFields(cascadingFields);
    }
  }

  /**
   * Return the schema which has been set as a string
   *
   * @return String representing the schema
   */
  protected String getJsonSchema() {
    if (schema == null) {
      return "";
    } else {
      return schema.toString();
    }
  }


  /**
   * Sink method to take an outgoing tuple and write it to Avro. If the packUnpack option is set to true then
   * the sink will convert the tuple into an Avro Record before passing it to the output. Otherwise it will just
   * pass the incoming Record through.
   *
   * @param flowProcess The cascading FlowProcess object. Should be passed in by cascading automatically.
   * @param sinkCall    The cascading SinkCall object. Should be passed in by cascading automatically.
   * @throws IOException
   */
  @Override
  public void sink(
      FlowProcess<JobConf> flowProcess,
      SinkCall<Object[], OutputCollector> sinkCall)
      throws IOException {
    TupleEntry tupleEntry = sinkCall.getOutgoingEntry();

      Record record = new Record((Schema) sinkCall.getContext()[0]);
      Object[] objectArray = CascadingToAvro.parseTupleEntry(tupleEntry, (Schema) sinkCall.getContext()[0]);
      for (int i = 0; i < objectArray.length; i++) {
        record.put(i, objectArray[i]);
      }
    //noinspection unchecked
    sinkCall.getOutput().collect(new AvroWrapper<Record>(record), NullWritable.get());

  }

  /**
   * Sink prepare method called by cascading once on each reducer. This method stuffs the schema into a context
   * for easy access by the sink method.
   *
   * @param flowProcess The cascading FlowProcess object. Should be passed in by cascading automatically.
   * @param sinkCall    The cascading SinkCall object. Should be passed in by cascading automatically.
   * @throws IOException
   */
  @Override
  public void sinkPrepare(
      FlowProcess<JobConf> flowProcess,
      SinkCall<Object[], OutputCollector> sinkCall)
      throws IOException {
    sinkCall.setContext(new Object[]{schema});

  }

  /**
   * sinkConfInit is called by cascading to set up the sinks. This happens on the client side before the
   * job is distributed.
   * There is a check for the presence of a schema and an exception is thrown if none has been provided.
   * After the schema check the conf object is given the options that Avro needs.
   *
   * @param flowProcess The cascading FlowProcess object. Should be passed in by cascading automatically.
   * @param tap         The cascading Tap object. Should be passed in by cascading automatically.
   * @param conf        The Hadoop JobConf object. This is passed in by cascading automatically.
   * @throws RuntimeException If no schema is present this halts the entire process.
   */
  @Override
  public void sinkConfInit(
      FlowProcess<JobConf> flowProcess,
      Tap<JobConf, RecordReader, OutputCollector> tap,
      JobConf conf) {
    if (schema == null) {
      throw new RuntimeException("Must provide sink schema");
    }
    // Set the output schema and output format class
    conf.set(AvroJob.OUTPUT_SCHEMA, schema.toString());
    conf.setOutputFormat(AvroOutputFormat.class);

    // add AvroSerialization to io.serializations
    Collection<String> serializations = conf.getStringCollection("io.serializations");
    if (!serializations.contains(AvroSerialization.class.getName())) {
      serializations.add(AvroSerialization.class.getName());
      conf.setStrings("io.serializations",
          serializations.toArray(new String[serializations.size()]));
    }
  }

  /**
   * This method is called by cascading to set up the incoming fields. If a schema isn't present then it will
   * go and peek at the input data to retrieve one. If packUnpack is false then it sets the incoming fields
   * to be Fields.ALL,
   * there should only be one incoming field in this case. Otherwise it get the field names from the schema
   * and sets the incoming fields to be the same.
   *
   * @param flowProcess The cascading FlowProcess object. Should be passed in by cascading automatically.
   * @param tap         The cascading Tap object. Should be passed in by cascading automatically.
   * @return Fields The source cascading fields.
   */
  @Override
  public Fields retrieveSourceFields(FlowProcess<JobConf> flowProcess, Tap tap) {
    if (schema == null) {
      try {
        schema = getSourceSchema(flowProcess, tap);
      } catch (IOException e) {
        throw new RuntimeException("Can't get schema from data source");
      }
    }
    Fields cascadingFields = new Fields();
    if (schema.getType().equals(Schema.Type.NULL)) {
      cascadingFields = Fields.NONE;
    } else {
      for (Field avroField : schema.getFields())
        cascadingFields = cascadingFields.append(new Fields(avroField.name()));
    }
    setSourceFields(cascadingFields);
    return getSourceFields();
  }


  /**
   * Source method to take an incoming Avro record and make it a Tuple. If the packUnpack option is set to true then
   * the source will convert the Avro Record into a Cascading Tuple. Otherwise it will just
   * pass the incoming Record through in the first Field of a tuple.
   *
   * @param flowProcess The cascading FlowProcess object. Should be passed in by cascading automatically.
   * @param sourceCall  The cascading SourceCall object. Should be passed in by cascading automatically.
   * @return boolean true on successful parsing and collection, false on failure.
   * @throws IOException
   */
  @Override
  public boolean source(
      FlowProcess<JobConf> flowProcess,
      SourceCall<Object[], RecordReader> sourceCall)
      throws IOException {

    @SuppressWarnings("unchecked") RecordReader<AvroWrapper<Record>, Writable> input = sourceCall.getInput();
    AvroWrapper<Record> wrapper = input.createKey();
    if (!input.next(wrapper, input.createValue())) {
      return false;
    }
    Record record = wrapper.datum();
    TupleEntry tupleEntry = sourceCall.getIncomingEntry();
    tupleEntry.setTuple(new AvroTuple(record));
    return true;
  }

  /**
   * sourceConfInit is called by cascading to set up the sources. This happens on the client side before the
   * job is distributed.
   * There is a check for the presence of a schema and if none has been provided the data is peeked at to get a schema.
   * After the schema check the conf object is given the options that Avro needs.
   *
   * @param flowProcess The cascading FlowProcess object. Should be passed in by cascading automatically.
   * @param tap         The cascading Tap object. Should be passed in by cascading automatically.
   * @param conf        The Hadoop JobConf object. This is passed in by cascading automatically.
   * @throws RuntimeException If no schema is present this halts the entire process.
   */
  @Override
  public void sourceConfInit(
      FlowProcess<JobConf> flowProcess,
      Tap<JobConf, RecordReader, OutputCollector> tap,
      JobConf conf) {

    retrieveSourceFields(flowProcess, tap);

    // Set the input schema and input class
    conf.set(AvroJob.INPUT_SCHEMA, schema.toString());
    conf.setInputFormat(AvroInputFormat.class);

    // add AvroSerialization to io.serializations
    Collection<String> serializations = conf.getStringCollection("io.serializations");
    if (!serializations.contains(AvroSerialization.class.getName())) {
      serializations.add(AvroSerialization.class.getName());
      conf.setStrings("io.serializations",
          serializations.toArray(new String[serializations.size()]));
    }


  }


  /**
   * This method peeks at the source data to get a schema when none has been provided.
   *
   * @param flowProcess The cascading FlowProcess object for this flow.
   * @param tap         The cascading Tap object.
   * @return Schema The schema of the peeked at data.
   * @throws RuntimeException If no schema can be found then we can't proceed.
   */
  private Schema getSourceSchema(FlowProcess<JobConf> flowProcess, Tap tap) throws IOException {

    if (tap instanceof CompositeTap) {
      tap = (Tap) ((CompositeTap) tap).getChildTaps().next();
    }
    final String path = tap.getIdentifier();
    Path p = new Path(path);
    final FileSystem fs = p.getFileSystem(flowProcess.getConfigCopy());
    // Get all the input dirs
    List<FileStatus> statuses = new LinkedList<FileStatus>(Arrays.asList(fs.globStatus(p, filter)));
    // Now get all the things that are one level down
    for (FileStatus status : new LinkedList<FileStatus>(statuses)) {
      if (status.isDir())
        for(FileStatus child : Arrays.asList(fs.listStatus(status.getPath(), filter))) {
          if (child.isDir()) {
            statuses.addAll(Arrays.asList(fs.listStatus(child.getPath(), filter)));
          }
        }
    }
    for (FileStatus status : statuses) {
      Path statusPath = status.getPath();
      if (fs.isFile(statusPath)) {
        // no need to open them all
        InputStream stream = new BufferedInputStream(fs.open(statusPath));
        @SuppressWarnings("unchecked") DataFileStream reader = new DataFileStream(stream, new GenericDatumReader());
        return reader.getSchema();
      }
    }
    // couldn't find any Avro files, return null schema
    return Schema.create(Schema.Type.NULL);
  }


  private static final PathFilter filter = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return !path.getName().startsWith("_");
    }
  };


  /**
   * Helper method to read in a schema when de-serializing the object
   *
   * @param in The ObjectInputStream containing the serialized object
   * @return Schema The parsed schema.
   */
  private static Schema readSchema(java.io.ObjectInputStream in) throws IOException {
    final Schema.Parser parser = new Schema.Parser();
    return parser.parse(in.readUTF());
  }

  private void writeObject(java.io.ObjectOutputStream out)
      throws IOException {
    out.writeUTF(this.schema.toString());
  }

  private void readObject(java.io.ObjectInputStream in)
      throws IOException {
    this.schema = readSchema(in);
  }
}

