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
public class SingleUpdateInstruction extends Instruction{
    
    
    private String content;
    
    public SingleUpdateInstruction(Table table, long pk, String content) throws Exception{
        super(table, pk);
        this.content = content;
        mode = Instruction.UPDATE;
    }
    
    public SingleUpdateInstruction(Table table, String pk, String content) throws Exception{
        this(table, SimulatedIterations.getValue(pk), content);
    }
    
    
    
    @Override
    public List<Record> process() throws Exception{
        Record rec = getTable().getRecord(pk);
        if (transaction.getLogger()!=null)
            transaction.getLogger().transactionWrite(transaction,getTable(), pk, rec.getContent(), content);
        
        endProcessing = true;
        List l = new ArrayList();
        l.add(getTable().updateRecord(getPk(), content));
        return l;
        
    }
    
    /**
     * @return the content
     */
    public String getContent() {
        return content;
    }

    
}
