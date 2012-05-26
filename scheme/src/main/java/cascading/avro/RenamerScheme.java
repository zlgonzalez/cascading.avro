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

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * Adapts sink fields to match wrapped scheme.
 */
public class RenamerScheme
        extends
        Scheme<JobConf, RecordReader<AvroWrapper<Record>, Writable>, OutputCollector<AvroWrapper<IndexedRecord>, Writable>, Object[], Object> {
    private final Scheme<JobConf, RecordReader<AvroWrapper<Record>, Writable>, OutputCollector<AvroWrapper<IndexedRecord>, Writable>, Object[], Object> scheme;
    private final Fields toSinkFields;

    public RenamerScheme(
            Scheme<JobConf, RecordReader<AvroWrapper<Record>, Writable>, OutputCollector<AvroWrapper<IndexedRecord>, Writable>, Object[], Object> scheme,
            Fields from, Fields to) {
        super(scheme.getSourceFields(), from);
        this.scheme = scheme;
        this.toSinkFields = to;
        if (scheme.getSinkFields().select(to).size() != to.size()) {
            throw new IllegalArgumentException("Can't use " + to + " with "
                    + scheme.getSinkFields());
        }
    }

    @Override
    public void sourceConfInit(
            FlowProcess<JobConf> process,
            Tap<JobConf, RecordReader<AvroWrapper<Record>, Writable>, OutputCollector<AvroWrapper<IndexedRecord>, Writable>> tap,
            JobConf conf) {
        scheme.sourceConfInit(process, tap, conf);
    }

    @Override
    public void sinkConfInit(
            FlowProcess<JobConf> process,
            Tap<JobConf, RecordReader<AvroWrapper<Record>, Writable>, OutputCollector<AvroWrapper<IndexedRecord>, Writable>> tap,
            JobConf conf) {
        scheme.sinkConfInit(process, tap, conf);
    }

    @Override
    public boolean source(
            FlowProcess<JobConf> process,
            SourceCall<Object[], RecordReader<AvroWrapper<Record>, Writable>> call)
            throws IOException {
        return scheme.source(process, call);
    }

    @Override
    public void sink(
            FlowProcess<JobConf> process,
            SinkCall<Object, OutputCollector<AvroWrapper<IndexedRecord>, Writable>> call)
            throws IOException {
        TupleEntry tupleEntry = call.getOutgoingEntry();
        call.getOutgoingEntry().setTuple(
                tupleEntry.selectTuple(this.getSinkFields()));
        scheme.sink(process, call);
    }
}
