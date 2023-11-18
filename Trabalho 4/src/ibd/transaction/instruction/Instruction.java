/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ibd.transaction.instruction;

import ibd.transaction.concurrency.Item;
import ibd.table.record.Record;
import ibd.table.Table;
import ibd.transaction.Transaction;
import java.util.List;

/**
 *
 * @author pccli
 */
public abstract class Instruction {
    
    public static final int READ = 0;
    public static final int WRITE = 1;
    public static final int UPDATE = 2;
    public static final int DELETE = 3;
    
    protected int mode;
    protected Table table;
    
    
    //protected Item item;
    protected Transaction transaction;
    
    protected long pk;
    
    boolean endProcessing = false;
    
    public Instruction(Table table, long pk) throws Exception{
        this.table = table;
        this.pk = pk;
    }
    
    public abstract List<Record> process() throws Exception;
    
    public int getMode(){
        return mode;
    }
    
    /**
     * @return the pk
     */
    public long getPk() {
        return pk;
    }

    /**
     * @param pk the pk to set
     */
    public void setPk(long pk) {
        this.pk = pk;
    }
    
    public static String getModeType(int mode){
        switch(mode){
            case READ: return   "read  ";
            case WRITE: return  "write ";
            case UPDATE: return "update";
            case DELETE: return "delete";
            default: return "error";
        }
        
        
    }
    
    public String getModeType(){
        return Instruction.getModeType(mode);
    }
    
    
    @Override
    public String toString (){
        
        return getMode()+" "+getTable().tableKey;
    }

    /**
     * @return the transaction
     */
    public Transaction getTransaction() {
        return transaction;
    }

    /**
     * @param transaction the transaction to set
     */
    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    /**
     * @return the table
     */
    public Table getTable() {
        return table;
    }

    /**
     * @param table the table to set
     */
    public void setTable(Table table) {
        this.table = table;
    }

    
//    /**
//     * @return the item
//     */
//    public Item getItem() {
//        return item;
//    }
//
//    /**
//     * @param item the item to set
//     */
//    public void setItem(Item item) {
//        this.item = item;
//    }
    
    
    
        public String getUniqueKey() {
        return getTable().tableKey + "(" + getPk() + ")";
    }
        
        public boolean endProcessing(){
            return endProcessing;
        }
    
}
