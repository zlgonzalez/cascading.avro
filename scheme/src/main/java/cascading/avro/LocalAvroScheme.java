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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumReader;

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

public class LocalAvroScheme
        extends
        AvroSchemeBase<Properties, InputStream, OutputStream, DataFileStream<Record>, DataFileWriter<Record>> {

    private static final long serialVersionUID = 904972401237236251L;

    public LocalAvroScheme(Schema dataSchema) {
        super(dataSchema);
    }

    public LocalAvroScheme() {
    }

    /**
     * @param fields
     * @param types
     */
    public LocalAvroScheme(Fields fields, Class[] types) {
        super(fields, types);
    }

    @Override
    public void sourcePrepare(FlowProcess<Properties> process,
            SourceCall<DataFileStream<Record>, InputStream> sourceCall)
            throws IOException {
        DatumReader<Record> reader = new GenericDatumReader<Record>();
        DataFileStream<Record> input = new DataFileStream<Record>(
                sourceCall.getInput(), reader);
        sourceCall.setContext(input);
    }

    @Override
    public void sourceConfInit(FlowProcess<Properties> process,
            Tap<Properties, InputStream, OutputStream> tap, Properties conf) {
        if (dataSchema == null)
            retrieveSchema(process, tap);
    }

    private void retrieveSchema(FlowProcess<Properties> process, Tap tap) {
        try {
            if (tap instanceof CompositeTap)
                tap = (Tap) ((CompositeTap) tap).getChildTaps().next();
            File file = new File(tap.getIdentifier());
            if (file.isDirectory())
                for (File f : file.listFiles()) {
                    file = f;
                    break;
                }
            // no need to open them all
            InputStream stream = new FileInputStream(file);
            DataFileStream reader = new DataFileStream(stream,
                    new ReflectDatumReader());
            dataSchema = reader.getSchema();
            retrieveSourceFields(tap);
            return;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Fields retrieveSourceFields(FlowProcess<Properties> process, Tap tap) {
        if (dataSchema == null)
            retrieveSchema(process, tap);
        return getSourceFields();
    }

    public void retrieveSourceFields(Tap tap) {
        if (!getSourceFields().isUnknown())
            return;
        final LinkedHashMap<String, FieldType> schemaFields = parseSchema(
                dataSchema, ALLOWED_TYPES);
        final Fields fields = fields(schemaFields);
        setSourceFields(fields);
        final Collection<FieldType> types = schemaFields.values();
        fieldTypes = types.toArray(new FieldType[types.size()]);
    }

    @Override
    public void sinkPrepare(FlowProcess<Properties> flowProcess,
            SinkCall<DataFileWriter<Record>, OutputStream> call)
            throws IOException {
        DatumWriter<Record> reader = new GenericDatumWriter<Record>();
        DataFileWriter<Record> dataFileWriter = new DataFileWriter<Record>(
                reader);
        dataFileWriter.create(this.dataSchema, call.getOutput());
        call.setContext(dataFileWriter);
    }

    @Override
    public boolean source(FlowProcess<Properties> process,
            SourceCall<DataFileStream<Record>, InputStream> call)
            throws IOException {
        DataFileStream<Record> in = call.getContext();
        if (!in.hasNext()) {
            return false;
        }
        final Record record = in.next();
        return read(call, record);
    }

    @Override
    public void sinkCleanup(FlowProcess<Properties> flowProcess,
            SinkCall<DataFileWriter<Record>, OutputStream> call)
            throws IOException {
        super.sinkCleanup(flowProcess, call);
        call.getContext().close();
        call.getOutput().close();
    }

    @Override
    public void sink(FlowProcess<Properties> flowProcess,
            SinkCall<DataFileWriter<Record>, OutputStream> call)
            throws IOException {
        Record record = write(call);
        call.getContext().append(record);
    }

}
