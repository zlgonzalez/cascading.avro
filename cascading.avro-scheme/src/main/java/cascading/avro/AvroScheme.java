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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroKeyComparator;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroSerialization;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.CompositeTap;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


@SuppressWarnings("serial")
public class AvroScheme
		extends
		Scheme<JobConf, RecordReader<AvroWrapper<Record>, Writable>, OutputCollector<AvroWrapper<Record>, Writable>, Object[], Object[]> {

	private Schema schema = null;
	private String recordName = null;
	private boolean isNullable = true;
	private static String DEFAULT_RECORD_NAME = "CascadingAvroRecord";

	public AvroScheme(Schema schema, String recordName, boolean isNullable) {
		this.schema = schema;
		this.recordName = recordName;
		this.isNullable = isNullable;
	}

	public AvroScheme(String name) {
		this(null, name, true);
	}
	
	public AvroScheme(Schema userSchema) {
		this(userSchema, DEFAULT_RECORD_NAME, true);
	}

	public AvroScheme(boolean isNullable) {
		this(null, DEFAULT_RECORD_NAME, isNullable);
	}
	
	public AvroScheme() {
		this(null, DEFAULT_RECORD_NAME, true);
	}


	@Override
	public void sink(
			FlowProcess<JobConf> flowProcess,
			SinkCall<Object[], OutputCollector<AvroWrapper<Record>, Writable>> sinkCall)
			throws IOException {
		TupleEntry tupleEntry = sinkCall.getOutgoingEntry();
		System.out.println(tupleEntry);
		Record record = new Record((Schema)sinkCall.getContext()[0]);
		Object[] objs = CascadingToAvro.parseTupleEntry(tupleEntry, (Schema)sinkCall.getContext()[0]);
		for(int i=0; i < objs.length; i++) record.put(i, objs[i]);
		sinkCall.getOutput().collect(new AvroWrapper<Record>(record),
                NullWritable.get());

	}

	
	@Override
	public void sinkPrepare(
			FlowProcess<JobConf> flowProcess,
			SinkCall<Object[], OutputCollector<AvroWrapper<Record>, Writable>> sinkCall)
			throws IOException {
		TupleEntry tupleEntry = sinkCall.getOutgoingEntry();
		if (schema == null) {
			 schema = CascadingToAvro.generateAvroSchemaFromTupleEntry(tupleEntry, recordName, isNullable);
		}
		sinkCall.setContext( new Object[] {schema});
//		Record record = new Record((Schema)sinkCall.getContext()[0]);
//		Object[] objs = CascadingToAvro.parseTupleEntry(tupleEntry, (Schema)sinkCall.getContext()[0]);
//		
//		sinkCall.getOutput().collect(new AvroWrapper<Record>(record),
//                NullWritable.get());
	}

	
	@Override
	public void sinkConfInit(
			FlowProcess<JobConf> flowProcess,
			Tap<JobConf, RecordReader<AvroWrapper<Record>, Writable>, OutputCollector<AvroWrapper<Record>, Writable>> tap,
			JobConf conf) {
//        if ((schema == null)) {  // dirty hack to get around not knowing the schema until runtime
//        	AvroJob.setOutputSchema(conf, Schema.create(Schema.Type.NULL));
//        }
		if (schema == null ) {
			throw new RuntimeException("Must provide sink schema");
		}
        AvroJob.setOutputSchema(conf, schema);
        Fields cascadingFields = new Fields();
        for(Field avroField : schema.getFields()) {
        	cascadingFields = cascadingFields.append(new Fields(avroField.name()));
        }
        setSinkFields(cascadingFields);
	}

	@Override
	public Fields retrieveSourceFields(FlowProcess<JobConf> flowProcess, Tap tap) {
		if (schema == null) { 
			schema = getSourceSchema(flowProcess, tap);
		}

		int n = schema.getFields().size();
		Object[] fieldNames = new Object[n];
		for (int i = 0; i < n; i++)
			fieldNames[i] = schema.getFields().get(i).name();
		setSourceFields(new Fields(Arrays.copyOf(fieldNames, fieldNames.length,
				Comparable[].class)));

		return getSourceFields();
	}

	@Override
	public boolean source(
			FlowProcess<JobConf> jobConf,
			SourceCall<Object[], RecordReader<AvroWrapper<Record>, Writable>> sourceCall)
			throws IOException {

		RecordReader<AvroWrapper<Record>, Writable> input = sourceCall
				.getInput();
		AvroWrapper<Record> wrapper = input.createKey();
		if (!input.next(wrapper, input.createValue())) {
			return false;
		}
		Record record = wrapper.datum();

		Object[] split = AvroToCascading.parseRecord(record, schema);
		Tuple tuple = sourceCall.getIncomingEntry().getTuple();
		
		tuple.clear();
		tuple.addAll(split);
		return true;
	}

	@Override
	public void sourceConfInit(
			FlowProcess<JobConf> flowProcess,
			Tap<JobConf, RecordReader<AvroWrapper<Record>, Writable>, OutputCollector<AvroWrapper<Record>, Writable>> tap,
			JobConf conf) {
		if (schema == null) {
			schema = getSourceSchema(flowProcess, tap);
		}
		retrieveSourceFields(flowProcess, tap);
		AvroJob.setInputSchema(conf, schema);

	}


	private Schema getSourceSchema(FlowProcess<JobConf> flowProcess, Tap tap) {
		try {
			if (tap instanceof CompositeTap)
				tap = (Tap) ((CompositeTap) tap).getChildTaps().next();
			final String file = tap.getIdentifier();
			Path p = new Path(file);
			Configuration conf = new Configuration();
			final FileSystem fs = p.getFileSystem(conf);
			for (FileStatus status : fs.listStatus(p)) {
				p = status.getPath();
				// no need to open them all
				InputStream stream = new BufferedInputStream(fs.open(p));
				DataFileStream reader = new DataFileStream(stream,
						new GenericDatumReader());
				Schema dataSchema = reader.getSchema();
				return dataSchema;
			}
			throw new RuntimeException("no schema found in " + file);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	private void writeObject(java.io.ObjectOutputStream out)
            throws IOException {
        out.writeBoolean(isNullable);
        out.writeUTF(schema.toString());
        out.writeUTF(recordName);
    }

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        isNullable = in.readBoolean();
        schema = new Schema.Parser().parse(in.readUTF());
        recordName = in.readUTF();
    }
}

