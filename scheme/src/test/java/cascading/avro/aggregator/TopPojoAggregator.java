package cascading.avro.aggregator;

import java.util.ArrayList;
import java.util.List;

import cascading.avro.pojo.EmbeddedPojo;
import cascading.avro.pojo.TopPojo;
import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class TopPojoAggregator extends BaseOperation<TopPojoAggregator.Context>
        implements Aggregator<TopPojoAggregator.Context> {

    private static final long serialVersionUID = 4175186487288287022L;

    public TopPojoAggregator(int numIncomingFields, Fields outputFields) {
        super(numIncomingFields, outputFields);
    }

    static class Context {
        public TopPojo topPojo;
        public List<EmbeddedPojo> things;
    }

    @Override
    public void start(@SuppressWarnings("rawtypes") FlowProcess fp, AggregatorCall<Context> aggCall) {
        Context ctx = new Context();
        ctx.things = new ArrayList<EmbeddedPojo>();
        aggCall.setContext(ctx);
    }

    @Override
    public void aggregate(@SuppressWarnings("rawtypes") FlowProcess fp, AggregatorCall<Context> aggCall) {
        TupleEntry entry = aggCall.getArguments();
        Context ctx = aggCall.getContext();

        if (ctx.topPojo == null) {
            ctx.topPojo = new TopPojo();
            ctx.topPojo.setId(entry.getInteger("Id"));
            ctx.topPojo.setState(entry.getString("State"));
        }

        EmbeddedPojo embPojo = new EmbeddedPojo();
        embPojo.setAge(entry.getString("Age"));
        embPojo.setCity(entry.getString("City"));
        embPojo.setLastName(entry.getString("LastName"));

        ctx.things.add(embPojo);
    }

    @Override
    public void complete(@SuppressWarnings("rawtypes") FlowProcess fp, AggregatorCall<Context> aggCall) {
        Context ctx = aggCall.getContext();
        if (ctx.things.size() > 0) {
            ctx.topPojo.setThings(ctx.things);
        }
        Tuple t = Tuple.size(1);
        t.set(0, ctx.topPojo);
        aggCall.getOutputCollector().add(t);
    }
}
