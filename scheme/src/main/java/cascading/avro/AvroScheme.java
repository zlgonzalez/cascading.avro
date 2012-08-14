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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.LinkedHashMap;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileConstants;
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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.CompositeTap;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * Cascading scheme for reading data serialized using Avro. This scheme sources
 * and sinks tuples with fields named and ordered the same was as the Avro
 * schema used in the constructor.
 * <p>
 * The following Avro types are supported:
 * <ul>
 * <li>boolean</li>
 * <li>bytes (as BytesWritable)</li>
 * <li>double</li>
 * <li>fixed (as BytesWritable)</li>
 * <li>float</li>
 * <li>int</li>
 * <li>long</li>
 * <li>null</li>
 * <li>string</li>
 * <li>array</li>
 * <li>map</li>
 * <li>union of [type, null], treated as nullable value of the type</li>
 * </ul>
 */

@SuppressWarnings("serial")
public class AvroScheme
        extends
        AvroSchemeBase<JobConf, RecordReader<AvroWrapper<Record>, Writable>, OutputCollector<AvroWrapper<Record>, Writable>, Object[], Object> {

    /**
     * This constructor only makes sense when using AvroScheme as a source, not a sink - in
     * that situation, the schema can be extracted from (one of the) source files.
     */
    public AvroScheme() {
    }

    public AvroScheme(Schema dataSchema) {
        super(dataSchema);
    }

    /**
     * Constructor that supports the original AvroScheme approach to explicitly specifying the fields & types.
     * 
     * @param fields
     * @param classes
     */
    public AvroScheme(Fields fields, Class[] types) {
        super(fields, types);
    }

    /**
     * @param string
     * @param fields
     * @param types
     */
    public AvroScheme(String recordName, Fields fields, Class<?>[] types) {
        super(recordName, fields, types);
    }

    @Override
    public void sourceConfInit(
            FlowProcess<JobConf> process,
            Tap<JobConf, RecordReader<AvroWrapper<Record>, Writable>, OutputCollector<AvroWrapper<Record>, Writable>> tap,
            JobConf conf) {
        conf.setInputFormat(AvroInputFormat.class);
        addAvroSerialization(conf);
        if (dataSchema == null)
            retrieveSchema(process, tap);
        conf.set(AvroJob.INPUT_SCHEMA, dataSchema.toString());
    }

    @Override
    public boolean source(
            FlowProcess<JobConf> process,
            SourceCall<Object[], RecordReader<AvroWrapper<Record>, Writable>> call)
            throws IOException {
        final RecordReader<AvroWrapper<Record>, Writable> input = call
                .getInput();
        AvroWrapper<Record> wrapper = input.createKey();
        if (!input.next(wrapper, input.createValue())) {
            return false;
        }

        final Record record = wrapper.datum();
        return read(call, record);
    }

    @Override
    public void sinkConfInit(
            FlowProcess<JobConf> process,
            Tap<JobConf, RecordReader<AvroWrapper<Record>, Writable>, OutputCollector<AvroWrapper<Record>, Writable>> tap,
            JobConf conf) {

        conf.setOutputKeyComparatorClass(AvroKeyComparator.class);
        conf.setMapOutputKeyClass(AvroKey.class);
        conf.setMapOutputValueClass(AvroValue.class);

        // TODO - if the user did new AvroScheme(), then dataSchema will be null.
        // We should explicitly catch that case and throw a better exception here
        // that says unspecified schemas only make sense when AvroScheme is used as
        // a source, not a sink.
        conf.set(AvroJob.OUTPUT_SCHEMA, dataSchema.toString());
        conf.setOutputFormat(AvroOutputFormat.class);
        conf.setOutputKeyClass(AvroWrapper.class);
        addAvroSerialization(conf);
        // set compression
        AvroOutputFormat.setDeflateLevel(conf, 6);
        AvroJob.setOutputCodec(conf, DataFileConstants.DEFLATE_CODEC);
        AvroOutputFormat.setSyncInterval(conf, 1048576);
    }

    private void addAvroSerialization(JobConf conf) {
        // add AvroSerialization to io.serializations
        final Collection<String> serializations = conf
                .getStringCollection("io.serializations");
        if (!serializations.contains(AvroSerialization.class.getName())) {
            serializations.add(AvroSerialization.class.getName());
            conf.setStrings("io.serializations",
                    serializations.toArray(new String[serializations.size()]));
        }

    }

    @Override
    public void sink(
            FlowProcess<JobConf> process,
            SinkCall<Object, OutputCollector<AvroWrapper<Record>, Writable>> call)
            throws IOException {
        Record record = write(call);
        call.getOutput().collect(new AvroWrapper<Record>(record),
                NullWritable.get());
    }

    private void retrieveSchema(FlowProcess<JobConf> flowProcess, Tap tap) {
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
                dataSchema = reader.getSchema();
                retrieveSourceFields(tap);
                return;
            }
            throw new RuntimeException("no schema found in " + file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // TODO <tap> isn't used here. And this code mostly duplicates what's in
    // AvroSchemeBase's constructor; the two should be collapse.
    private Fields retrieveSourceFields(Tap tap) {
        if (!getSourceFields().isUnknown())
            return getSourceFields();
        final LinkedHashMap<String, FieldType> schemaFields = parseSchema(
                dataSchema, ALLOWED_TYPES);
        final Fields fields = fields(schemaFields);
        setSourceFields(fields);
        final Collection<FieldType> types = schemaFields.values();
        fieldTypes = types.toArray(new FieldType[types.size()]);
        return fields;
    }

    public Fields retrieveSourceFields(FlowProcess<JobConf> flowProcess, Tap tap) {
        if (dataSchema == null)
            retrieveSchema(flowProcess, tap);

        return getSourceFields();
    }

}
