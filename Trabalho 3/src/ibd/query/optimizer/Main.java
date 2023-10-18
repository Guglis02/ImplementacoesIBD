/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.optimizer;

import ibd.table.Directory;
import ibd.query.binaryop.NestedLoopJoin;
import ibd.table.Params;
import ibd.table.ComparisonTypes;
import ibd.query.Operation;
import ibd.query.Tuple;
import ibd.query.Utils;
import ibd.query.sourceop.IndexScan;
import ibd.query.unaryop.filter.PKFilter;
import ibd.table.Table;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Sergio
 */
public class Main {

    

    public void testOptimization(QueryOptimizer opt, Operation query, boolean showTree, boolean runQuery) throws Exception {

        Params.BLOCKS_LOADED = 0;
        Params.BLOCKS_SAVED = 0;
        
        System.out.println("BEFORE");
        if (showTree) {
            Utils.toString(query, 0);
            System.out.println("");
        }

        if (runQuery) {
            System.out.println("Data");
            Params.BLOCKS_LOADED = 0;
            Params.BLOCKS_SAVED = 0;
            query.open();
            Iterator<Tuple> it = query.run();
            while (it.hasNext()) {
                Tuple r = (Tuple) it.next();
                System.out.println(r);
            }
            query.close();

            System.out.println("blocks loaded during reorganization " + Params.BLOCKS_LOADED);
            System.out.println("blocks saved during reorganization " + Params.BLOCKS_SAVED);
        }

        query = opt.optimize(query);

        System.out.println("AFTER");
        if (showTree) {
            Utils.toString(query, 0);
            System.out.println("");
        }

        if (runQuery) {
            System.out.println("Data");
            Params.BLOCKS_LOADED = 0;
            Params.BLOCKS_SAVED = 0;
            query.open();
            Iterator<Tuple> it = query.run();
            while (it.hasNext()) {
                Tuple r = (Tuple) it.next();
                System.out.println(r);
            }
            query.close();

            System.out.println("blocks loaded during reorganization " + Params.BLOCKS_LOADED);
            System.out.println("blocks saved during reorganization " + Params.BLOCKS_SAVED);
        }
        System.out.println("");
    }
    
        
    private Operation createQuery1() throws Exception{
        
        Table table1 = Directory.getTable("c:\\teste\\ibd", "t1", Table.DEFULT_PAGE_SIZE, false);
        Table table2 = Directory.getTable("c:\\teste\\ibd", "t2", Table.DEFULT_PAGE_SIZE, false);

        Operation scan1 = new IndexScan("t1", table1);
        Operation scan2 = new IndexScan("t2", table2);

        Operation join1 = new NestedLoopJoin(scan1, scan2);

        Operation filter1 = new PKFilter(join1, null, ComparisonTypes.EQUAL, 6L);

        return filter1;
    }
    
    private Operation createQuery2() throws Exception{
        
        Table table1 = Directory.getTable("c:\\teste\\ibd", "t1", Table.DEFULT_PAGE_SIZE, false);
        Table table2 = Directory.getTable("c:\\teste\\ibd", "t2", Table.DEFULT_PAGE_SIZE, false);
        Table table3 = Directory.getTable("c:\\teste\\ibd", "t3", Table.DEFULT_PAGE_SIZE, false);

        Operation scan1 = new IndexScan("t1", table1);
        Operation scan2 = new IndexScan("t2", table2);
        Operation scan3 = new IndexScan("t3", table3);

        Operation diff = new NestedLoopJoin(scan1, scan2);
        Operation join2 = new NestedLoopJoin(diff, scan3);

        Operation filter1 = new PKFilter(join2, "t1", ComparisonTypes.EQUAL, 20L);

        return filter1;
    }
    
    
    public static void main(String[] args) {
        try {
            Main m = new Main();
            QueryOptimizer opt = new XXXQueryOptimizer();
            
            //uncomment these lines the first time you execute
            //createTable("c:\\teste\\ibd", "t1", Table.DEFULT_PAGE_SIZE, 100, false, 2,50);
            //createTable("c:\\teste\\ibd", "t2", Table.DEFULT_PAGE_SIZE, 100, false, 3,50);
            //createTable("c:\\teste\\ibd", "t3", Table.DEFULT_PAGE_SIZE, 100, false, 4,50);
            //createTable("c:\\teste\\ibd", "t4", Table.DEFULT_PAGE_SIZE, 100, false, 1,50);
            
            
            System.out.println("**********TESTE 1");
            m.testOptimization(opt,m.createQuery1(), true, true);
            
            System.out.println("**********TESTE 2");
            m.testOptimization(opt,m.createQuery2(), true, true);


        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
