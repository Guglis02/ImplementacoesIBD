package ibd.query.unaryop;

import ibd.query.Operation;
import ibd.query.OperationIterator;
import ibd.query.Tuple;
import ibd.query.lookup.LookupFilter;
import ibd.query.unaryop.sort.PKSort;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DuplicateRemovalGustavoMachadoDeFreitas extends UnaryOperation
{
    Boolean IsDataSorted;

    ArrayList<Tuple> tuples;

    public DuplicateRemovalGustavoMachadoDeFreitas(Operation childOperation, String dataSourceAlias, boolean isDataSorted) throws Exception {
        super(childOperation, dataSourceAlias);
        IsDataSorted = isDataSorted;
    }

    @Override
    public Iterator<Tuple> lookUp(LookupFilter filter) {
        return new DuplicateRemovalIterator(filter);
    }

    public class DuplicateRemovalIterator extends OperationIterator
    {

        //the iterator over the child operation
        Iterator<Tuple> tupleIterator;
        //the lookup required from the parent operation
        LookupFilter lookup;

        String lastContent;

        public DuplicateRemovalIterator(LookupFilter lookup) {
            this.lookup = lookup;
            tupleIterator = childOperation.lookUp(lookup);//pushes filter down to the child operation

//            if (tuples == null) {
//                tuples = new ArrayList<>();
//                try {
//                    //accesses and stores all tuples that come from the child operation
//                    tupleIterator = childOperation.run();
//                    while (tupleIterator.hasNext()) {
//                        Tuple tuple = (Tuple) tupleIterator.next();
//                        tuples.add(tuple);
//                    }
//
//                    //sort collection
//                    Collections.sort(tuples, createComparator());
//                } catch (Exception ex) {
//                    Logger.getLogger(PKSort.class.getName()).log(Level.SEVERE, null, ex);
//                }
//            }
        }

        @Override
        protected Tuple findNextTuple() {
            while (tupleIterator.hasNext()) {
                Tuple tp = tupleIterator.next();
                String content = tp.sourceTuples[sourceTupleIndex].record.getContent();

                if (lookup.match(tp))
                {
                    if (IsDataSorted && !Objects.equals(content, lastContent))
                    {
                        lastContent = content;
                        return tp;
                    }
                    else
                    {

                    }
                }
            }
            return null;
        }
    }
}
