package ibd.query.unaryop;

import ibd.query.Operation;
import ibd.query.OperationIterator;
import ibd.query.Tuple;
import ibd.query.lookup.LookupFilter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Objects;

public class DuplicateRemovalGustavoMachadoDeFreitas extends UnaryOperation {
    Boolean IsDataSorted;

    ArrayList<String> uniqueContents;

    public DuplicateRemovalGustavoMachadoDeFreitas(Operation childOperation, String dataSourceAlias, boolean isDataSorted) throws Exception {
        super(childOperation, dataSourceAlias);
        IsDataSorted = isDataSorted;
    }

    @Override
    public Iterator<Tuple> lookUp(LookupFilter filter) {
        return new DuplicateRemovalIterator(filter);
    }

    public class DuplicateRemovalIterator extends OperationIterator {
        // the iterator over the child operation
        Iterator<Tuple> tupleIterator;
        // the lookup required from the parent operation
        LookupFilter lookup;

        String lastContent;

        public DuplicateRemovalIterator(LookupFilter lookup) {
            this.lookup = lookup;
            if (uniqueContents == null && !IsDataSorted) {
                uniqueContents = new ArrayList<>();
            }
            tupleIterator = childOperation.lookUp(lookup);//pushes filter down to the child operation
        }

        @Override
        protected Tuple findNextTuple() {
            while (tupleIterator.hasNext()) {
                Tuple tp = tupleIterator.next();
                String content = tp.sourceTuples[sourceTupleIndex].record.getContent();

                if (lookup.match(tp)) {
                    if (!IsDataSorted && !uniqueContents.contains(content)) {
                        uniqueContents.add(content);
                        return tp;
                    }

                    if (IsDataSorted && !Objects.equals(content, lastContent))
                    {
                        lastContent = content;
                        return tp;
                    }
                }
            }
            return null;
        }
    }
}
