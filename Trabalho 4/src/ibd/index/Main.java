/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.index;

import ibd.table.ComparisonTypes;
import ibd.table.Directory;
import ibd.table.Table;
import ibd.table.record.Record;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.List;

/**
 *
 * @author Sergio
 */
public class Main {

    public static void main(String[] args) {
        try {
            Main m = new Main();
            Table table = Directory.getTable("c:\\teste\\ibd", "table", Table.DEFULT_PAGE_SIZE, true);

            table.addRecord(9L, "reg 9");
            table.addRecord(7L, "reg 7");
            table.addRecord(8L, "reg 8");
            table.addRecord(5L, "reg 5");
            table.addRecord(6L, "reg 6");
            table.addRecord(3L, "reg 3");
            table.addRecord(4L, "reg 4");
            table.addRecord(1L, "reg 1");
            table.removeRecord(5L);
            table.flushDB();
            Record rec = table.getRecord(3L);
            if (rec == null) {
                System.out.println("não tem");
            } else {
                System.out.println(rec.getContent());
            }
            List<? extends Record> recs;
            System.out.println(">=3 ************");
            recs = table.getRecords(3L, ComparisonTypes.GREATER_EQUAL_THAN);
            for (int i = 0; i < recs.size(); i++) {
                rec = recs.get(i);
                System.out.println("found " + rec.getContent());
            }
            System.out.println(">3 ************");
            recs = table.getRecords(3L, ComparisonTypes.GREATER_THAN);
            for (int i = 0; i < recs.size(); i++) {
                rec = recs.get(i);
                System.out.println("found " + rec.getContent());
            }
            System.out.println("<=3 ************");
            recs = table.getRecords(3L, ComparisonTypes.LOWER_EQUAL_THAN);
            for (int i = 0; i < recs.size(); i++) {
                rec = recs.get(i);
                System.out.println("found " + rec.getContent());
            }
            System.out.println("<3 ************");
            recs = table.getRecords(3L, ComparisonTypes.LOWER_THAN);
            for (int i = 0; i < recs.size(); i++) {
                rec = recs.get(i);
                System.out.println("found " + rec.getContent());
            }
            System.out.println("diff 3************");
            recs = table.getRecords(3L, ComparisonTypes.DIFF);
            for (int i = 0; i < recs.size(); i++) {
                rec = recs.get(i);
                System.out.println("found " + rec.getContent());
            }
            System.out.println("equal 3************");
            recs = table.getRecords(3L, ComparisonTypes.EQUAL);
            for (int i = 0; i < recs.size(); i++) {
                rec = recs.get(i);
                System.out.println("found " + rec.getContent());
            }

            System.out.println(">=2 ************");
            recs = table.getRecords(2L, ComparisonTypes.GREATER_EQUAL_THAN);
            for (int i = 0; i < recs.size(); i++) {
                rec = recs.get(i);
                System.out.println("found " + rec.getContent());
            }
            System.out.println(">2 ************");
            recs = table.getRecords(2L, ComparisonTypes.GREATER_THAN);
            for (int i = 0; i < recs.size(); i++) {
                rec = recs.get(i);
                System.out.println("found " + rec.getContent());
            }
            System.out.println("<=2 ************");
            recs = table.getRecords(2L, ComparisonTypes.LOWER_EQUAL_THAN);
            for (int i = 0; i < recs.size(); i++) {
                rec = recs.get(i);
                System.out.println("found " + rec.getContent());
            }
            System.out.println("<2 ************");
            recs = table.getRecords(2L, ComparisonTypes.LOWER_THAN);
            for (int i = 0; i < recs.size(); i++) {
                rec = recs.get(i);
                System.out.println("found " + rec.getContent());
            }
            System.out.println("diff 2************");
            recs = table.getRecords(2L, ComparisonTypes.DIFF);
            for (int i = 0; i < recs.size(); i++) {
                rec = recs.get(i);
                System.out.println("found " + rec.getContent());
            }
            System.out.println("equal 2************");
            recs = table.getRecords(2L, ComparisonTypes.EQUAL);
            for (int i = 0; i < recs.size(); i++) {
                rec = recs.get(i);
                System.out.println("found " + rec.getContent());
            }

        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
