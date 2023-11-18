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
public class Main {


    public Operation testJoin() throws Exception {

        Table table1 = createTable("c:\\teste\\ibd", "t1", Table.DEFULT_PAGE_SIZE, 50, false, 1, 50);
        Table table2 = createTable("c:\\teste\\ibd", "t2", Table.DEFULT_PAGE_SIZE, 50, false, 1, 50);
        Table table3 = createTable("c:\\teste\\ibd", "t3", Table.DEFULT_PAGE_SIZE, 50, false, 1, 50);
        Table table4 = createTable("c:\\teste\\ibd", "t4", Table.DEFULT_PAGE_SIZE, 50, false, 1, 50);

        Operation scan = new FullTableScan("t1", table1);
        //PKFilter scan1 = new PKFilter(scan, "t1", ComparisonTypes.LOWER_THAN, 2L);
        Operation scan2 = new FullTableScan("t2", table2);
        Operation scan3 = new FullTableScan("t3", table3);
        Operation scan4 = new FullTableScan("t4", table4);

        Operation join = null;
        //construa aqui as junções 

        return join;

    }


    public static void main(String[] args) {
        try {
            Main m = new Main();

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
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
