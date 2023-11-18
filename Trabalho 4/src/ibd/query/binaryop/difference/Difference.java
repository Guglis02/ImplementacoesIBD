/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.binaryop.difference;

import ibd.query.binaryop.*;
import ibd.query.Operation;
import ibd.query.OperationIterator;
import ibd.query.Tuple;
import ibd.query.lookup.LookupFilter;
import ibd.query.lookup.PKLookupFilter;
import ibd.table.ComparisonTypes;
import java.util.Iterator;

/**
 * Performs a nested loop join between the left and the right operations.
 * This operation performs an equi-join between a left pk and a right pk.
 * The pks used as the join condition comes from the source tuples defined by the specified aliases.
 * @author Sergio
 */
public class Difference extends BinaryOperation {

    /**
     * 
     * @param leftOperation the left side operation
     * @param rightOperation the right side operation
     * @throws Exception
     */
    public Difference(Operation leftOperation, Operation rightOperation) throws Exception {
        super(leftOperation, rightOperation);
    }

    /**
     *
     * @param leftOperation the left side operation
     * @param leftDataSourceAlias the alias of the data source from the left side used to
     * perform the join condition of this nested lopp join
     * @param rightOperation the right side operation
     * @param rigthtDataSourceAlias the alias of the data source from the right side used to
     * perform oin condition of this nested lopp join
     * @throws Exception
     */
    public Difference(Operation leftOperation, String leftDataSourceAlias, Operation rightOperation, String rigthtDataSourceAlias) throws Exception {
        super(leftOperation, leftDataSourceAlias, rightOperation, rigthtDataSourceAlias);
    }

    /**
     *
     * @return the name of the operation
     */
    @Override
    public String toString() {
        return "Nested Loop Join";
    }

    /**
     * {@inheritDoc }
     * @return an iterator that performs a simple nested loop join over the tuples from the left and right sides
     */
    @Override
    public Iterator<Tuple> lookUp(LookupFilter lookup) {
        return new DifferenceIterator(lookup);
    }

    /**
     * the class that produces resulting tuples from the join between the two underlying operations.
     */
    private class DifferenceIterator extends OperationIterator {

        //keeps the current state of the left side of the join: The current tuple read
        Tuple leftTuple;
        
        Tuple rightTuple;
        //the iterator over the operation on the left side
        Iterator<Tuple> leftTuples;
        //the iterator over the operation on the left side
        Iterator<Tuple> rightTuples;
        //the lookup required from the parent operation
        LookupFilter lookup;

        /**
         *
         * @param lookup the condition from the parent operation that needs to be satisfied
         */
        public DifferenceIterator(LookupFilter lookup) {
            leftTuple = null;
            this.lookup = lookup;
            leftTuples = leftOperation.run(); //iterate over all tuples from the left side
            rightTuples = rightOperation.run(); //iterate over all tuples from the left side
        }

        /**
         * 
         * @return the next satisfying tuple, if any
         */
        @Override
        protected Tuple findNextTuple() {

            while (leftTuples.hasNext()) {
            Tuple leftTuple = leftTuples.next();
            if (!hasEqual(leftTuple)) {
                Tuple tuple = new Tuple();
                tuple.setSourceTuples(leftTuple);
                return tuple;
            }
        }
        return null;
        }
    
    
    /**
     * Retorna se a tupla de entrada possui
     * uma correspondente na operação direita
     */
    private boolean hasEqual(Tuple leftTuple)  {
        while (rightTuple != null || rightTuples.hasNext()) {
            if (rightTuple == null) {
                rightTuple = rightTuples.next();
            }
            int compare = Long.compare(leftTuple.sourceTuples[leftSourceTupleIndex].record.getPrimaryKey(), rightTuple.sourceTuples[rightSourceTupleIndex].record.getPrimaryKey());
            if (compare < 0) {
                return false;
            } else if (compare == 0) {
                rightTuple = null;
                return true;
            }
            rightTuple = null;
        }
        return false;
    }

}

}