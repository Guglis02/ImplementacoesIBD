/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.binaryop.join;

import ibd.table.Params;
import static ibd.table.Utils.createTable;
import ibd.query.Operation;
import ibd.query.Tuple;
import ibd.query.sourceop.FullTableScan;
import ibd.table.Table;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Sergio
 */
public class Main1 {


    public Operation testJoin() throws Exception {

        Table table1 = createTable("c:\\teste\\ibd", "t1", Table.DEFULT_PAGE_SIZE, 100, false, 1,50);
        Table table2 = createTable("c:\\teste\\ibd", "t2", Table.DEFULT_PAGE_SIZE, 100, false, 1,50);
        Table table3 = createTable("c:\\teste\\ibd", "t3", Table.DEFULT_PAGE_SIZE, 100, false, 1,50);
        Table table4 = createTable("c:\\teste\\ibd", "t4", Table.DEFULT_PAGE_SIZE, 100, false, 1,50);

        Operation scan = new FullTableScan("t1", table1);
        //PKFilter scan1 = new PKFilter(scan, "t1", ComparisonTypes.LOWER_THAN, 2L);
        Operation scan2 = new FullTableScan("t2", table2);
        Operation scan3 = new FullTableScan("t3", table3);
        Operation scan4 = new FullTableScan("t4", table4);

        //balanced
        //blocks loaded during reorganization 1171629
        //blocks saved during reorganization 0
//        Operation join2 = new NestedLoopJoin(scan, scan2);
//        Operation join3 = new NestedLoopJoin(scan3, scan4);
//        Operation join1 = new NestedLoopJoin(join2, join3);

        //worst
        //Operation join3 = new NestedLoopJoin(scan3, scan4);
        //Operation join2 = new NestedLoopJoin(scan2, join3);
        //Operation join1 = new NestedLoopJoin(scan, join2);

        //best
        Operation join3 = new NestedLoopJoin(scan, scan2);
        Operation join2 = new NestedLoopJoin(join3, scan3);
        Operation join1 = new NestedLoopJoin(join2, scan4);

        //really best
//        Operation join3 = new NestedLoopJoin(scan3, scan1);
//        Operation join2 = new NestedLoopJoin(join3, scan1);
//        Operation join1 = new NestedLoopJoin(join2, scan2);

        return join1;

    }


    public static void main(String[] args) {
        try {
            Main1 m = new Main1();

            Operation op = m.testJoin();

            Params.BLOCKS_LOADED = 0;
            Params.BLOCKS_SAVED = 0;

            op.open();
            Iterator<Tuple> tuples = op.run();
            while (tuples.hasNext()) {
                Tuple r = tuples.next();
                System.out.println(r);
            }

            System.out.println("blocks loaded during reorganization " + Params.BLOCKS_LOADED);
            System.out.println("blocks saved during reorganization " + Params.BLOCKS_SAVED);

        } catch (Exception ex) {
            Logger.getLogger(Main1.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
