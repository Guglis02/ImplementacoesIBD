package ibd.query.optimizer;

import ibd.query.Operation;
import ibd.query.binaryop.BinaryOperation;
import ibd.query.sourceop.IndexScan;
import ibd.query.unaryop.UnaryOperation;
import ibd.query.unaryop.filter.PKFilter;
import ibd.table.ComparisonTypes;

import java.util.List;

import static java.sql.DriverManager.println;


public class GustavoMachadoDeFreitasQueryOptimizer implements QueryOptimizer {

    Operation rootOp;
    List<PKFilter> filterList;

    @Override
    public Operation optimize(Operation op) {
        filterList = new java.util.ArrayList<>();
        rootOp = op;
        recursiveOptimize(op);
        return rootOp;
    }

    private void recursiveOptimize(Operation op)
    {
        if (op instanceof PKFilter
            && ((PKFilter) op).getComparisonType() == ComparisonTypes.EQUAL)
        {
            PKFilter filter = (PKFilter) op;
            if (filter.getParentOperation() == null)
            {
                rootOp = filter.getChildOperation();
            }
            filterList.add(filter);
        }

        if (op instanceof IndexScan)
        {
            println("Found IndexScan: " + op);
            checkForFilter((IndexScan) op);
        }

        if (op instanceof UnaryOperation)
        {
            UnaryOperation uop = (UnaryOperation) op;
            recursiveOptimize(uop.getChildOperation());
        }

        if (op instanceof BinaryOperation)
        {
            BinaryOperation bop = (BinaryOperation) op;
            recursiveOptimize(bop.getLeftOperation());
            recursiveOptimize(bop.getRightOperation());
        }
    }

    private void checkForFilter(IndexScan op) {
        for (PKFilter filter : filterList)
        {
            if (filter.getDataSourceAlias().equals(op.getDataSourceAlias()))
            {
                Operation opParent = op.getParentOperation();

                if (opParent instanceof UnaryOperation)
                {
                    UnaryOperation uop = (UnaryOperation) opParent;
                    uop.setChildOperation(filter);
                }

                if (opParent instanceof BinaryOperation)
                {
                    BinaryOperation bop = (BinaryOperation) opParent;
                    if (bop.getLeftOperation() == op)
                    {
                        bop.setLeftOperation(filter);
                    }
                    else
                    {
                        bop.setRightOperation(filter);
                    }
                }

                op.setParentOperation(filter);
                filter.setChildOperation(op);
                filter.setParentOperation(opParent);
            }
        }
    }
}
