package ibd.query.unaryop;

import com.sun.org.apache.xpath.internal.operations.Bool;
import ibd.query.Operation;
import ibd.query.OperationIterator;
import ibd.query.Tuple;
import ibd.query.lookup.LookupFilter;
import ibd.query.lookup.RealLookupFilter;
import ibd.query.unaryop.sort.PKSort;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DuplicateRemovalGustavoMachadoDeFreitas extends UnaryOperation {
    Boolean IsDataSorted;

    ArrayList<Tuple> uniqueTuples;

    public DuplicateRemovalGustavoMachadoDeFreitas(Operation childOperation, String dataSourceAlias, boolean isDataSorted) throws Exception {
        super(childOperation, dataSourceAlias);
        IsDataSorted = isDataSorted;
    }

    @Override
    public Iterator<Tuple> lookUp(LookupFilter filter)
    {
        if (IsDataSorted)
        {
            return new SortedDuplicateRemovalIterator(filter);

        }
        return new UnsortedDuplicateRemovalIterator(filter);
    }

    public class SortedDuplicateRemovalIterator extends OperationIterator {
        // the iterator over the child operation
        Iterator<Tuple> tupleIterator;
        // the lookup required from the parent operation
        LookupFilter lookup;

        String lastContent;

        public SortedDuplicateRemovalIterator(LookupFilter lookup) {
            this.lookup = lookup;
            tupleIterator = childOperation.lookUp(lookup);
        }

        @Override
        protected Tuple findNextTuple() {
            while (tupleIterator.hasNext()) {
                Tuple tp = tupleIterator.next();
                String content = tp.sourceTuples[sourceTupleIndex].record.getContent();

                if (lookup.match(tp) && !Objects.equals(content, lastContent))
                {
                    lastContent = content;
                    return tp;
                }
            }
            return null;
        }
    }

    public class UnsortedDuplicateRemovalIterator extends OperationIterator {
        // the iterator over the child operation
        Iterator<Tuple> tupleIterator;
        // the lookup required from the parent operation
        LookupFilter lookup;

        public UnsortedDuplicateRemovalIterator(LookupFilter lookup) {
            this.lookup = lookup;
            if (uniqueTuples == null)
            {
                uniqueTuples = new ArrayList<>();
                try
                {
                    tupleIterator = childOperation.run();
                    while (tupleIterator.hasNext())
                    {
                       Tuple tuple = (Tuple) tupleIterator.next();
                       if (IsUnique(tuple))
                       {
                           uniqueTuples.add(tuple);
                       }
                    }
                } catch (Exception ex) {
                    Logger.getLogger(PKSort.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

            tupleIterator = uniqueTuples.iterator();
        }

        private Boolean IsUnique(Tuple tuple)
        {
            String content = tuple.sourceTuples[sourceTupleIndex].record.getContent();

            for (Tuple uniqueTuple : uniqueTuples)
            {
                String uniqueContent = uniqueTuple.sourceTuples[sourceTupleIndex].record.getContent();
                if (Objects.equals(content, uniqueContent))
                {
                    return false;
                }
            }

            return true;
        }

        @Override
        protected Tuple findNextTuple() {
            while (tupleIterator.hasNext())
            {
                Tuple tp = tupleIterator.next();

                if (lookup.match(tp)) {
                    return tp;
                }
            }
            return null;
        }
    }
}
