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

    @Override
    public Operation optimize(Operation op)
    {
        rootOp = op;
        recursiveOptimize(op);
        return rootOp;
    }

    private void recursiveOptimize(Operation op)
    {
        if (op instanceof UnaryOperation)
        {
            UnaryOperation unaryOp = (UnaryOperation) op;
            Operation childOp = unaryOp.getChildOperation();

            if (isEqualityFilter(op))
            {
                PKFilter filter = (PKFilter) op;
                tryToPushDownFilter(filter, filter);
            }
            else if (op instanceof Filter
                    && !(((Filter) op).getChildOperation() instanceof BinaryOperation)
                    && !(isEqualityFilter(((Filter) op).getChildOperation())) )
            {
                Filter filter = (Filter) op;
                tryToPullUpFilter(filter, filter);
            }

            recursiveOptimize(childOp);

        }
        else if (op instanceof BinaryOperation)
        {
            BinaryOperation binaryOp = (BinaryOperation) op;
            recursiveOptimize(binaryOp.getLeftOperation());
            recursiveOptimize(binaryOp.getRightOperation());
        }
    }

    // Sobe a árvore com um filter, se encontrar um NestedLoopJoin, e estiver no lado direito dele,
    // posiciona o filter acima dele.
    private void tryToPullUpFilter(Operation op, Filter filter) {
        if (op == null)
        {
            return;
        }

        Operation parentOp = op.getParentOperation();

        if (parentOp instanceof NestedLoopJoin
            && ((NestedLoopJoin) parentOp).getRightOperation() == op)
        {
            NestedLoopJoin nestedJoin = (NestedLoopJoin) parentOp;

            Operation grandParentOp = nestedJoin.getParentOperation();
            Operation filterChildOp = filter.getChildOperation();

            filter.setChildOperation(nestedJoin);
            reparentOperation(nestedJoin, filter, grandParentOp);

            nestedJoin.setRightOperation(filterChildOp);
            nestedJoin.setParentOperation(filter);

            parentOp = filter;

            // Se o filtro passar a ser o novo nó raiz
            if (grandParentOp == null) {
                rootOp = filter;
            }
        }

        tryToPullUpFilter(parentOp, filter);
    }

    // Desce a árvore com um filter, se encontrar um IndexScan que não possui um PKFilter e que consulte a mesma tabela do filtro,
    // posiciona o filter acima dele.
    private void tryToPushDownFilter(Operation op, PKFilter filter) {
        if (op instanceof IndexScan
            && !(op.getParentOperation() instanceof PKFilter)
            && ((IndexScan) op).getDataSourceAlias().equals(filter.getDataSourceAlias()))
        {
            Operation filterParent = filter.getParentOperation();
            Operation opParent = op.getParentOperation();

            // Se o filtro era o nó raiz
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
            UnaryOperation unaryOperation = (UnaryOperation) op;
            tryToPushDownFilter(unaryOperation.getChildOperation(), filter);
        } else if (op instanceof BinaryOperation) {
            BinaryOperation binaryOperation = (BinaryOperation) op;
            tryToPushDownFilter(binaryOperation.getLeftOperation(), filter);
            tryToPushDownFilter(binaryOperation.getRightOperation(), filter);
        }
    }

    private void reparentOperation(Operation oldChild, Operation newChild, Operation newParent)
    {
        if (newParent instanceof UnaryOperation) {
            UnaryOperation unaryOperation = (UnaryOperation) newParent;
            unaryOperation.setChildOperation(newChild);
        } else if (newParent instanceof BinaryOperation) {
            BinaryOperation binaryOperation = (BinaryOperation) newParent;
            if (binaryOperation.getLeftOperation() == oldChild) {
                binaryOperation.setLeftOperation(newChild);
            } else {
                binaryOperation.setRightOperation(newChild);
            }
        }

        newChild.setParentOperation(newParent);
    }

    private Boolean isEqualityFilter (Operation op)
    {
        return (op instanceof PKFilter
                && ((PKFilter) op).getComparisonType() == ComparisonTypes.EQUAL);
    }
}
