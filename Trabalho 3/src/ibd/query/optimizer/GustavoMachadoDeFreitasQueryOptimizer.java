package ibd.query.optimizer;

import ibd.query.Operation;
import ibd.query.binaryop.BinaryOperation;
import ibd.query.binaryop.NestedLoopJoin;
import ibd.query.sourceop.IndexScan;
import ibd.query.unaryop.UnaryOperation;
import ibd.query.unaryop.filter.Filter;
import ibd.query.unaryop.filter.PKFilter;
import ibd.table.ComparisonTypes;

public class GustavoMachadoDeFreitasQueryOptimizer implements QueryOptimizer {

    private Operation rootOp;

    private Boolean isEqualityFilter (Operation op)
    {
        return (op instanceof PKFilter
                && ((PKFilter) op).getComparisonType() == ComparisonTypes.EQUAL);
    }

    @Override
    public Operation optimize(Operation op) {
        rootOp = op;
        recursiveOptimize(op);
        return rootOp;
    }

    private void recursiveOptimize(Operation op)
    {
        if (op instanceof UnaryOperation)
        {
            UnaryOperation uop = (UnaryOperation) op;
            Operation childOp = uop.getChildOperation();

            if (isEqualityFilter(op))
            {
                PKFilter filter = (PKFilter) op;
                tryToPushDownFilter(filter, filter);
            } else if (op instanceof Filter
                && !(((Filter) op).getChildOperation() instanceof BinaryOperation)
                && !(isEqualityFilter(((Filter) op).getChildOperation())))
            {
                Filter filter = (Filter) op;
                tryToPullUpFilter(filter, filter);
            }

            recursiveOptimize(childOp);
        }

        if (op instanceof BinaryOperation)
        {
            BinaryOperation bop = (BinaryOperation) op;
            recursiveOptimize(bop.getLeftOperation());
            recursiveOptimize(bop.getRightOperation());
        }
    }

    private void tryToPullUpFilter(Operation op, Filter filter) {
        if (op == null)
        {
            return;
        }

        Operation parentOp = op.getParentOperation();

        // Se encontrar um NestedLoopJoin, e estiver no lado direito dele, posiciona o filtro acima dele
        if (parentOp instanceof NestedLoopJoin
            && ((NestedLoopJoin) parentOp).getRightOperation() == op)
        {
            Operation grandParentOp = parentOp.getParentOperation();
            Operation filterChildOp = filter.getChildOperation();

            filter.setChildOperation(parentOp);
            reparentOperation(parentOp, filter, grandParentOp);

            ((NestedLoopJoin) parentOp).setRightOperation(filterChildOp);
            parentOp.setParentOperation(filter);

            parentOp = filter;

            if (grandParentOp == null) {
                rootOp = filter;
            }

        }

        tryToPullUpFilter(parentOp, filter);
    }

    private void tryToPushDownFilter(Operation op, PKFilter filter) {
        // Se encontrar um IndexScan que não possui um PKFilter
        // e que consulte a mesma tabela do filtro, posiciona o filtro acima dele
        if (op instanceof IndexScan
            && !(op.getParentOperation() instanceof PKFilter)
            && ((IndexScan) op).getDataSourceAlias().equals(filter.getDataSourceAlias()))
        {
            Operation filterParent = filter.getParentOperation();
            Operation opParent = op.getParentOperation();

            if (filterParent == null)
            {
                rootOp = filter.getChildOperation();
                filter.getChildOperation().setParentOperation(null);
            }

            reparentOperation(op, filter, opParent);

            op.setParentOperation(filter);
            filter.setChildOperation(op);

            reparentOperation(filter, opParent, filterParent);
        }

        if (op instanceof UnaryOperation) {
            UnaryOperation uop = (UnaryOperation) op;
            tryToPushDownFilter(uop.getChildOperation(), filter);
        } else if (op instanceof BinaryOperation) {
            BinaryOperation bop = (BinaryOperation) op;
            tryToPushDownFilter(bop.getLeftOperation(), filter);
            tryToPushDownFilter(bop.getRightOperation(), filter);
        }
    }

    private void reparentOperation(Operation oldChild, Operation newChild, Operation newParent)
    {
        if (newParent instanceof UnaryOperation) {
            UnaryOperation uop = (UnaryOperation) newParent;
            uop.setChildOperation(newChild);
        } else if (newParent instanceof BinaryOperation) {
            BinaryOperation bop = (BinaryOperation) newParent;
            if (bop.getLeftOperation() == oldChild) {
                bop.setLeftOperation(newChild);
            } else {
                bop.setRightOperation(newChild);
            }
        }

        newChild.setParentOperation(newParent);
    }
}
