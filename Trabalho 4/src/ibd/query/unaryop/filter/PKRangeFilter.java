/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.unaryop.filter;

import ibd.query.Operation;
import ibd.query.lookup.LookupFilter;
import ibd.query.lookup.PKRangeLookupFilter;
import ibd.table.ComparisonTypes;

/**
 * A pk filter operation filters tuples that come from its child operation.
 * The source tuple used as the filter condition comes from the data source defined by the specify alias.
 * The filter compares the pk of the source tuple against a fixed value. 
 * @author Sergio
 */
public class PKRangeFilter extends Filter {

    /*
    * the pk value to be compared against the pk of the chosen source tuple.
    */
    long pkvalue;
    
    Long pkvalue2 = -1L;
    
    int comparisonType2 = -1;

    /**
     *
     * @param childOperation the child operation
     * @param dataSourceAlias the alias of the data source
     * @param comparisonType the type of comparison that needs to be satisfied (e.g. =, < , >)
     * @param value the value to be compared against the pk of the source tuple
     * @throws Exception
     */
    public PKRangeFilter(Operation childOperation, String dataSourceAlias, int comparisonType, long value,int comparisonType2, long value2) throws Exception {
        super(childOperation, dataSourceAlias, comparisonType);
        this.pkvalue = value;
        this.pkvalue2 = value2;
        this.comparisonType2 = comparisonType2;
    }

    /**
     *
     * @return the pk value to be compared against the pk of the source tuple
     */
    public Long getValue() {
        return pkvalue;
    }
    
    public Long getValue2(){
        return pkvalue2;
    }
    
    public int getComparisonType2(){
        return comparisonType2;
    }

    /**
     *
     * @return
     */
    @Override
    public String toString() {
        if (dataSourceAlias == null) {
            return "PK Filter " + ComparisonTypes.getComparisonType(comparisonType) + " " + pkvalue;
        } else {
            return "PK Filter [" + dataSourceAlias + "]" + ComparisonTypes.getComparisonType(comparisonType) + " " + pkvalue;
        }
    }

    /**
     * {@inheritDoc }
     * The filter condition defined compares a range (defined by two pkValues and comparison types) against a fixed value.
     */
    @Override
    public LookupFilter createLookupFilter(int tupleIndex) {
        return new PKRangeLookupFilter(pkvalue, comparisonType, pkvalue2, comparisonType2, tupleIndex);
    }
}
