/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ibd.transaction.instruction;

import ibd.table.record.Record;
import ibd.table.Table;
import ibd.transaction.SimulatedIterations;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author pccli
 */
public class SingleDeleteInstruction extends Instruction{
    
    
    public SingleDeleteInstruction(Table table, long pk) throws Exception{
        super(table, pk);
        mode = Instruction.DELETE;
    }
    
    public SingleDeleteInstruction(Table table, String pk) throws Exception{
        this(table, SimulatedIterations.getValue(pk));
    }
    
    
    
    @Override
    public List<Record> process() throws Exception{
        Record rec = getTable().getRecord(pk);
        //if (transaction.getLogger()!=null)
            //transaction.getLogger().transactionWrite(transaction,getTable(), pk, rec.getContent(), content);
        
        endProcessing = true;
        List l = new ArrayList();
        l.add(getTable().removeRecord(pk));
        return l;
        
    }
    
    
}
