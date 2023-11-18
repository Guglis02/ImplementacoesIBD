/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.unaryop.sort;

import ibd.query.Tuple;
import java.util.Comparator;

/**
 *
 * @author Sergio
 */
public class TupleComparator implements Comparator<Tuple>{


    int sortIndex;
    public TupleComparator(int sortIndex){
        this.sortIndex = sortIndex; 
    }

    @Override
    public int compare(Tuple t1, Tuple t2) {
        return Long.compare(t1.sourceTuples[sortIndex].record.getPrimaryKey(),
                        t2.sourceTuples[sortIndex].record.getPrimaryKey());
    }
    
}
