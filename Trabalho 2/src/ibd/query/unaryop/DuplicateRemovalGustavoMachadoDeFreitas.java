package ibd.query.unaryop;

import ibd.query.Operation;
import ibd.query.OperationIterator;
import ibd.query.Tuple;
import ibd.query.lookup.LookupFilter;
import ibd.table.record.Record;

import java.util.Iterator;

public class DuplicateRemovalGustavoMachadoDeFreitas extends UnaryOperation
{
    private final Boolean IsDataSorted;

    public DuplicateRemovalGustavoMachadoDeFreitas(Operation childOperation, String dataSourceAlias, Boolean IsDataSorted) throws Exception {
        super(childOperation, dataSourceAlias);
        this.IsDataSorted = IsDataSorted;
    }

    @Override
    public Iterator<Tuple> lookUp(LookupFilter filter) {
        return new DuplicateRemovalIterator(filter);
    }

    @Override
    public void open() throws Exception {
        childOperation.open();
        super.open();
    }

    private class DuplicateRemovalIterator extends OperationIterator
    {
        //the iterator over the child operation
        Iterator<Tuple> tuples;
        //the lookup required from the parent operation
        LookupFilter lookup;

        public DuplicateRemovalIterator(LookupFilter filter) {
            lookup = filter;
            tuples = childOperation.lookUp(sourceTupleIndex);//pushes filter down to the child operation
        }

        @Override
        protected Tuple findNextTuple() {
            while (tuples.hasNext()) {
                Tuple tuple = tuples.next();

                if (lookup.match(tuple)) {
                    return tuple;
                }
            }

            return null;
        }
    }
}
