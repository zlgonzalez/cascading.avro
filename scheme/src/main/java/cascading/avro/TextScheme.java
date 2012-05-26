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

import java.io.IOException;
import java.util.EnumSet;
import java.util.LinkedHashMap;

import org.apache.avro.Schema;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.avro.AvroSchemeBase.FieldType;
import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tuple.Fields;


/**
 * A variant of {@link TextDelimited} scheme that gets field name and type
 * information from an Avro schema.
 */
public class TextScheme extends
        Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {
    public static final EnumSet<Schema.Type> ALLOWED_TYPES = EnumSet.of(
            Schema.Type.BOOLEAN, Schema.Type.DOUBLE, Schema.Type.FLOAT,
            Schema.Type.INT, Schema.Type.LONG, Schema.Type.NULL,
            Schema.Type.STRING, Schema.Type.UNION);

    private final TextDelimited text;

    /**
     * Creates TAB-separated scheme
     */
    public TextScheme(Schema avroSchema) {
        this(avroSchema, "\t", null);
    }

    /**
     * Creates scheme with given delimiter
     */
    public TextScheme(Schema avroSchema, String delimiter) {
        this(avroSchema, delimiter, null);
    }

    /**
     * Creates scheme with given delimiter and quoting character
     */
    public TextScheme(Schema avroSchema, String delimiter, String quote) {
        final LinkedHashMap<String, FieldType> schemaFields = AvroSchemeBase
                .parseSchema(avroSchema, ALLOWED_TYPES);
        final Fields fields = AvroSchemeBase.fields(schemaFields);
        setSinkFields(fields);
        setSourceFields(fields);

        text = new TextDelimited(fields, delimiter, quote,
                AvroSchemeBase.inferClasses(schemaFields.values()));
    }

    @Override
    public void sinkConfInit(FlowProcess process, Tap tap, JobConf conf) {
        text.sinkConfInit(process, tap, (JobConf) conf);
    }

    @Override
    public void sourceConfInit(FlowProcess process, Tap tap, JobConf conf) {
        text.sourceConfInit(process, tap, (JobConf) conf);
    }

    @Override
    public void sink(FlowProcess arg0, SinkCall arg1) throws IOException {
        text.sink(arg0, arg1);

    }

    @Override
    public boolean source(FlowProcess arg0, SourceCall arg1) throws IOException {
        return text.source(arg0, arg1);
    }

    @Override
    public void sourcePrepare(FlowProcess<JobConf> flowProcess,
            SourceCall<Object[], RecordReader> sourceCall) throws IOException {
        text.sourcePrepare(flowProcess, sourceCall);
    }

    @Override
    public void sinkPrepare(FlowProcess<JobConf> flowProcess,
            SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
        text.sinkPrepare(flowProcess, sinkCall);
    }

}
