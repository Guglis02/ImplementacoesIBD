/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.transaction.instruction;

import ibd.table.ComparisonTypes;
import ibd.query.Operation;
import ibd.query.Tuple;
import ibd.query.sourceop.FullTableScan;
import ibd.query.unaryop.filter.PKFilter;
import ibd.query.unaryop.filter.PKRangeFilter;
import ibd.table.record.Record;
import ibd.table.Table;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author pccli
 */
public class MultiReadInstruction extends Instruction {

    int comparisonType;
    int comparisonType2 = -1;
    long pk2;
    
    Operation query;

    public MultiReadInstruction(Table table, long pk, int comparisonType) throws Exception {
        super(table, pk);
        this.comparisonType = comparisonType;
        mode = Instruction.READ;
    }
    
    public MultiReadInstruction(Table table, long pk, int comparisonType, long pk2, int comparisonType2) throws Exception {
        this(table, pk, comparisonType);
        this.comparisonType2 = comparisonType2;
        this.pk2 = pk2;
    }

    public Long getLastPK(){
        return pk2;
    }
    /*
    public List<AbstractRecord> processX() throws Exception {
        List l = new ArrayList();
        if (query==null){
            Operation scan1 = new TableScan("t1", getTable());
            if (comparisonType2==-1)
                query = new PKFilter(scan1, "t1", comparisonType, pk);
            else query = new PkRangeFilter(scan1, "t1", comparisonType, pk, comparisonType2, pk2); 
            query.open();
        }
        
        if (query.hasNext()){
            
            Tuple t = query.next();
            l.add(t.sourceTuples[0].record);
        }
        if (!query.hasNext()){
            endProcessing = true;
            query.close();
        }

        return l;
    }
    */
    
    @Override
    public List<Record> process() throws Exception {
        List l = new ArrayList();

        Operation scan1 = new FullTableScan("t1", getTable());
        if (comparisonType2==-1)
            query = new PKFilter(scan1, "t1", comparisonType, pk);
        else query = new PKRangeFilter(scan1, "t1", comparisonType, pk, comparisonType2, pk2); 

        query.open();
        Iterator<Tuple> it = query.run();
        while (it.hasNext()) {
            Tuple t = it.next();
            l.add(t.sourceTuples[0].record);
        }
        query.close();
        endProcessing = true;
        return l;
    }

    @Override
    public int getMode() {
        return Instruction.READ;
    }

    @Override
    public String getUniqueKey() {
        return getTable().tableKey + " " + ComparisonTypes.getComparisonType(comparisonType) + " " + pk;
    }

}
