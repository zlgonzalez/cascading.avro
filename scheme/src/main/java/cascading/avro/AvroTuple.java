package cascading.avro;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleException;
import cascading.tuple.util.TupleViews;
import org.apache.avro.generic.GenericData;

/**
 * This is a thin wrapper on an incoming Avro object. Cascading is now using Tuple views so we can get away with
 * just providing adapters for the Tuple interface rather than doing any real conversions at read time.
 */
public class AvroTuple extends Tuple{

    public GenericData.Record getRecord() {
        return record;
    }

    private GenericData.Record record;


    public AvroTuple(GenericData.Record inrec) {
        record = inrec;
    }

    @Override
    public Object getObject(int i) {
        return record.get(i);
    }

    @Override
    public void set(int i, Object val) {
        record.put(i, val);
    }

    @Override
    public int[] getPos( Fields declarator, Fields selector )
    {
        if( !declarator.isUnknown() && record.getSchema().getFields().size() != declarator.size() )
            throw new TupleException( "field declaration: " + declarator.print() + ", does not match tuple: " + print() );

        return declarator.getPos( selector );
    }

    @Override
    public Tuple get( int[] pos )
    {
        if( pos == null || pos.length == 0 )
            return this;

//        Tuple results = new Tuple();
//
//        for( int i : pos )
//            results.add( record.get( i ) );

        return TupleViews.createNarrow(pos, this);
    }


}
