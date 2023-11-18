/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.lookup;

import ibd.query.Tuple;
import ibd.table.ComparisonTypes;


/**
 * this filter finds tuples based on pk comparison. 
 * @author Sergio
 */
public class PKRangeLookupFilter extends RealLookupFilter{

    /*
    * the first operand of this filter
    */
    long pk;
    
    long pk2;
    
    int comparisonType2;
    
    /**
     * Defines a filter as a range condition (the first operando) and a pkvalue (the second operand).
     * @param pk the lower value of the range 
     * @param comparisonType the comparison type to be applied over the lower value
     * @param pk2 the upper value of the range 
     * @param comparisonType2 the comparison type to be applied over the upper value 
     * @param tupleIndex the index of the source tuple that corresponds to the second operand
     */
    public PKRangeLookupFilter(long pk,int comparisonType,long pk2,int comparisonType2, int tupleIndex){
        
        super(comparisonType, tupleIndex);
        
        this.pk = pk;
        this.pk2 = pk2;
        this.comparisonType2 = comparisonType2;
        
        validateRange();
    }
    
     // The range condition is only valid if the comparison are chosen correctly. Also, the upper limit must be at least equal to the lower limit.
    // the parameters are adjusted if necessary, by using closed range and upper value equal to the lower value. 
    private void validateRange(){
    if (comparisonType!=ComparisonTypes.LOWER_EQUAL_THAN && comparisonType!=ComparisonTypes.LOWER_THAN)
            comparisonType = ComparisonTypes.LOWER_EQUAL_THAN;
        if (comparisonType2!=ComparisonTypes.GREATER_EQUAL_THAN && comparisonType!=ComparisonTypes.GREATER_THAN)
            comparisonType = ComparisonTypes.GREATER_EQUAL_THAN;
    
    }
    
    /**
     *
     * @return the lower value of the range of the first operand
     */
    public Long getPk(){
        return pk;
    }
    
    /**
     *
     * @return the upper value of the range of the first operand
     */
    public Long getPk2(){
        return pk2;
    }
    
    /**
     * {@inheritDoc }
     */
    @Override
    public boolean match(Tuple tuple) {
        //compares the pk against the pk that comes from a source tuple
        //a tuples must satisfy both the lower and upper range conditions
        boolean ok = LookupFilter.match(tuple.sourceTuples[tupleIndex].record.getPrimaryKey(), pk, comparisonType);
        if (!ok) return false;
        return LookupFilter.match(tuple.sourceTuples[tupleIndex].record.getPrimaryKey(),pk2, comparisonType2);
    }
    
}
