/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.binaryop.join;

import ibd.query.Operation;
import ibd.query.OperationIterator;
import ibd.query.Tuple;
import ibd.query.binaryop.BinaryOperation;
import ibd.query.lookup.LookupFilter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Performs a block nested loop join between the left and the right operations.
 * This operation performs an equi-join between a left pk and a right pk.
 * The pks used as the join condition comes from the source tuples defined by the specified aliases.
 * @author Sergio
 */
public class BlockNestedLoopJoin extends BinaryOperation {

    /**
     * the join buffer size
     */
    protected int joinBufferSize = 10;
    
    /**
     * 
     * @param leftOperation the left side operation
     * @param rightOperation the right side operation
     * @param joinBufferSize the join buffer size
     * @throws Exception
     */
    public BlockNestedLoopJoin(Operation leftOperation, Operation rightOperation, int joinBufferSize) throws Exception {
        super(leftOperation, rightOperation);
        this.joinBufferSize = joinBufferSize;
        
    }

    /**
     *
     * @param leftOperation the left side operation
     * @param leftDataSourceAlias the alias of the data source from the left side
     * @param rightOperation the right side operation
     * @param rigthtDataSourceAlias the alias of the data source from the right side
     * @param joinBufferSize the join buffer size
     * @throws Exception
     */
    public BlockNestedLoopJoin(Operation leftOperation, String leftDataSourceAlias, Operation rightOperation, String rigthtDataSourceAlias, int joinBufferSize) throws Exception {
        super(leftOperation, leftDataSourceAlias, rightOperation, rigthtDataSourceAlias);
        this.joinBufferSize = joinBufferSize;
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public void open() throws Exception {

        super.open();
    }

    /**
     * {@inheritDoc }
     * @return an iterator that performs a block nested loop join over the tuples from the left and right sides
     */
    @Override
    public Iterator<Tuple> lookUp(LookupFilter lookup) {
        return new BlockNestedLoopJoinIterator(lookup);
    }

    /**
     * the class that produces resulting tuples from the block nested loop join between the two
     * underlying operations
     */
    private class BlockNestedLoopJoinIterator extends OperationIterator {

        
        //the iterator over the operation on the left side
        Iterator<Tuple> leftTuples;
        //the iterator over the operation on the right side
        Iterator<Tuple> rightTuples;
        //the lookup required from the parent operation
        LookupFilter lookup;
        //this buffer contains tuples from the left side
        List<Tuple> joinBuffer = new ArrayList();
        //keeps the current state of the right side of the join: The current right tuple read
        Tuple rightTuple;
        //keeps the current state of the left side of the join: The current left index read
        int currentLeftIndex = 0;
        //indicates if the right side is empty
        boolean rightSideEmpty = false;

        /**
         *
         * @param lookup the condition from the parent operation that needs to
         * be satisfied
         */
        public BlockNestedLoopJoinIterator(LookupFilter lookup) {
            this.lookup = lookup;
            //iterate over all tuples from the left side
            leftTuples = leftOperation.run(); 
            //iterate over all tuples from the right side
            rightTuples = rightOperation.run();
            if (!rightTuples.hasNext())
                rightSideEmpty = true;
            feedBuffer();
        }

        /**
         * feeds the buffer with tuples from the left side
         */
        private void feedBuffer() {
            int count = 0;
            joinBuffer.clear();
            while (leftTuples.hasNext() && count < joinBufferSize) {
                joinBuffer.add(leftTuples.next());
                count++;
            }

        }

        /**
         *
         * @return the next satisfying tuple, if any
         */
        @Override
        protected Tuple findNextTuple() {

            //the right side is empty
            if (rightSideEmpty)
                return null;
            
            //the block nested loop join inverses processing:the right tuple drives the search
            //the right tuple is checked against all buffered tuples from the left side
            while (true) {
                //if necessary, 
                if (rightTuple == null) {
                    //advances the right tuple 
                    rightTuple = rightTuples.next();
                    //resets the join buffer cursor
                    currentLeftIndex = 0;
                    
                    //if all right tuples were processed, we need to start over from the right side and feed the join buffer again
                    if (rightTuple == null) {

                        feedBuffer();

                        //if no tuples were added to the join buffer, it means the left side tuples were all processed and we can end execution
                        if (joinBuffer.isEmpty()) {
                            return null;
                        }

                        //load all tuples from the right side
                        rightTuples = rightOperation.run();
                        //sets the cursor to the first tuple from the right side
                        rightTuple = rightTuples.next();
                    }
                    
                }
                //iterate through the join buffer to find tuples that satisfy the lookup
                for (int i = currentLeftIndex; i < joinBuffer.size(); i++) {

                    Tuple curLeftTuple = joinBuffer.get(i);
                    currentLeftIndex = i + 1;
                    if (Objects.equals(curLeftTuple.sourceTuples[leftSourceTupleIndex].record.getPrimaryKey(), rightTuple.sourceTuples[rightSourceTupleIndex].record.getPrimaryKey())) {
                        //create returning tuple and add the joined tuples
                        Tuple tuple = new Tuple();
                        tuple.setSourceTuples(curLeftTuple, rightTuple);
                        //a tuple must satisfy the lookup filter that comes from the parent operation
                        if (lookup.match(tuple)) {
                            return tuple;
                        }

                    }
                }

                //All corresponding tuples from the right side processed. 
                //set null to allow right side cursor to advance
                rightTuple = null;

            }

        }

        @Override
        public String toString() {
            return "Block Nested Loop Join";
        }
    }
}
