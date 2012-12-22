package cascading.avro;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleException;
import cascading.tuple.util.TupleViews;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;

/**
 * This is a thin wrapper on an incoming Avro object. Cascading is now using Tuple views so we can get away with
 * just providing adapters for the Tuple interface rather than doing any real conversions at read time.
 */
public class AvroTuple extends Tuple{

    private GenericData.Record record;
    private Schema schema;


    public AvroTuple(GenericData.Record inrec) {
        record = inrec;
        schema = inrec.getSchema();
    }

    /**
     * Method get returns the element at the given position of the underlying Record. A conversion
     * to the proper cascading type is also performed.
     *
     * @param pos of type int
     * @return Comparable
     */
    @Override
    public Object getObject(int pos) {
        return AvroToCascading.fromAvro(record.get(pos), schema.getFields().get(pos).schema());
    }

    /**
     * Method set sets the given value in the underlying record to the given index position in this instance.
     * A conversion to the proper avro type is performed.
     *
     * @param pos of type int
     * @param val of type Object
     */
    @Override
    public void set(int pos, Object val) {
        record.put(pos, CascadingToAvro.toAvro(val, schema.getFields().get(pos).schema()));
    }


    @Override
    public int[] getPos( Fields declarator, Fields selector )
    {
        if( !declarator.isUnknown() && schema.getFields().size() != declarator.size() )
            throw new TupleException( "field declaration: " + declarator.print() + ", does not match tuple: " + print() );

        return declarator.getPos( selector);
    }



    @Override
    public Tuple get( int[] pos )
    {
        if( pos == null || pos.length == 0 )
            return this;

        return TupleViews.createNarrow(pos, this);
    }

    /**
     * Method size returns the number of values in this Tuple instance.
     *
     * @return int
     */
    @Override
    public int size()
    {
        return schema.getFields().size();
    }


    public GenericData.Record getRecord() {
        return record;
    }
}
