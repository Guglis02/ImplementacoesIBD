/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.table;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 * @author pccli
 */
public class Utils {

    static public Table createTable(String folder, String name, int pageSize, int size, boolean shuffled, int range) throws Exception {
        Table table = Directory.getTable(folder, name, pageSize, true);
        //table.initLoad();

        Long[] array1 = new Long[(int) Math.ceil((double) size / range)];
        for (int i = 0; i < array1.length; i++) {
            array1[i] = new Long(i * range);
        }

        if (shuffled) {
            shuffleArray(array1);
        }

        for (int i = 0; i < array1.length; i++) {
            String text = name + "(" + array1[i] + ")";
            table.addRecord(array1[i], Utils.pad(text, 400));
            //table.addRecord(array1[i], String.valueOf(array1[i]));
            //table.addRecord(array1[i], "0");
        }
        table.flushDB();
        return table;
    }
    
    static public Table createTable2(String folder, String name, int pageSize, int size, boolean shuffled, int range) throws Exception {
        Table table = Directory.getTable(folder, name, pageSize, true);
        //table.initLoad();

        Long[] array1 = new Long[(int) Math.ceil((double) size / range)];
        for (int i = 0; i < array1.length; i++) {
            array1[i] = new Long(i * range);
        }

        if (shuffled) {
            shuffleArray(array1);
        }

        for (int i = 0; i < array1.length; i++) {
            String text = array1[i] + "";
            table.addRecord(array1[i], text);
        }
        table.flushDB();
        return table;
    }

    public static String pad(String text, int len) {
        if (text.length() > len) {
            return text;
        }
        StringBuffer buf = new StringBuffer(text);
        int len_ = len - text.length();
        for (int i = 0; i < len_; i++) {
            buf.append(" ");
        }
        return buf.toString();
    }

    public static void shuffleArray(Long[] ar) {
        // If running on Java 6 or older, use `new Random()` on RHS here
        Random rnd = ThreadLocalRandom.current();
        for (int i = ar.length - 1; i > 0; i--) {
            int index = rnd.nextInt(i + 1);
            // Simple swap
            long a = ar[index];
            ar[index] = ar[i];
            ar[i] = a;
        }
    }

    public static boolean match(Long value1, Long value2, int comparisonType) {

        int resp = value1.compareTo(value2);
        if (resp == 0 && (comparisonType == ComparisonTypes.EQUAL
                || comparisonType == ComparisonTypes.LOWER_EQUAL_THAN
                || comparisonType == ComparisonTypes.GREATER_EQUAL_THAN)) {
            return true;
        } else if (resp < 0 && (comparisonType == ComparisonTypes.LOWER_THAN
                || comparisonType == ComparisonTypes.LOWER_EQUAL_THAN)) {
            return true;
        } else if (resp > 0 && (comparisonType == ComparisonTypes.GREATER_THAN
                || comparisonType == ComparisonTypes.GREATER_EQUAL_THAN)) {
            return true;
        } else if (resp != 0 && comparisonType == ComparisonTypes.DIFF) {
            return true;
        } else {
            return false;
        }
    }

}
