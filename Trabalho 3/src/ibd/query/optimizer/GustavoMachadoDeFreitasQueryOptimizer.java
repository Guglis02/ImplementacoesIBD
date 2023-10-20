package ibd.query.optimizer;

import ibd.query.Operation;
import ibd.query.binaryop.BinaryOperation;
import ibd.query.sourceop.IndexScan;
import ibd.query.unaryop.UnaryOperation;
import ibd.query.unaryop.filter.PKFilter;
import ibd.table.ComparisonTypes;

import java.util.Iterator;
import java.util.List;

import static java.sql.DriverManager.println;


public class GustavoMachadoDeFreitasQueryOptimizer implements QueryOptimizer {

    Operation rootOp;

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

            if (op instanceof PKFilter
                && ((PKFilter) op).getComparisonType() == ComparisonTypes.EQUAL)
            {
                PKFilter filter = (PKFilter) op;
                findIndexScan(filter, filter);
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

    private void findIndexScan(Operation op, PKFilter filter) {
        // Se encontrar um IndexScan que não possui um PKFilter
        // e que consulte a mesma tabela do filtro, posiciona o filtro acima dele
        if (op instanceof IndexScan
            && !(op.getParentOperation() instanceof PKFilter)
            && ((IndexScan) op).getDataSourceAlias().equals(filter.getDataSourceAlias())) {
                Operation filterParent = filter.getParentOperation();
                Operation opParent = op.getParentOperation();

                reparentOperation(op, filter, opParent);

                op.setParentOperation(filter);
                filter.setChildOperation(op);
                reparentOperation(filter, opParent, filterParent);
        }

        if (op instanceof UnaryOperation) {
            UnaryOperation uop = (UnaryOperation) op;
            findIndexScan(uop.getChildOperation(), filter);
        } else if (op instanceof BinaryOperation) {
            BinaryOperation bop = (BinaryOperation) op;
            findIndexScan(bop.getLeftOperation(), filter);
            findIndexScan(bop.getRightOperation(), filter);
        }
    }

    private void reparentOperation(Operation oldChild, Operation newChild, Operation newParent)
    {
        if (newParent == null) {
            return;
        }

        if (newChild.getParentOperation() == null)
        {
            if (newChild instanceof UnaryOperation)
            {
                UnaryOperation uop = (UnaryOperation) newChild;
                rootOp = uop.getChildOperation();
                uop.getChildOperation().setParentOperation(null);
            }
        }

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
