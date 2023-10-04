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

    static final String[] names = {"Alexandre", "Alice", "Ana", "Andre", "Antonia", "Arthur", 
        "Beatriz","Carla","Carlos","Carolina","Clara","Cristiano","Daniel","Denise","Diana",
"Eduardo","Elias","Eva","Evandro","Fabiano","Fernanda","Felipe","Francisco","Fred",
"Gabriela","Gisele","Gustavo","Helena","Henrique","Hugo","Ines","Irene","Isabel",
"Joao","Joaquim","Jorge","Julia","Laura","Lucas","Luis","Luisa",
"Manuel","Marcos","Maria","Mario","Marta","Mateus","Miguel","Monica",
"Nelson","Nicole","Ofelia","Patricia","Paula","Paulo","Pierre",
"Raquel","Raul","Ricardo","Rita","Rodrigo","Rosa","Sandra","Sara","Sofia",
"Tatiana","Teresa","Tiago","Ursula","Vera","Vitor","Yasmin"
};

    static public String[] generateUniqueNames(int size) {
        String[] names_ = new String[size];
        if (size < names.length) {
            System.arraycopy(names, 0, names_, 0, size);
            return names_;
        }
        int turns = size / names.length + 1;
        for (int i = 0; i < turns; i++) {
            for (int j = 0; j < names.length; j++) {
                if (i * names.length + j == size) {
                    return names_;
                }
                names_[i * names.length + j] = names[j] + i;

            }
        }

        return names_;

    }

    static public String[] generateNames(int cardinality, int size) {
        String[] names_ = new String[size];
        String uniqueNames[] = generateUniqueNames(cardinality);
        if (cardinality > size) {
            System.arraycopy(names, 0, names_, 0, size);
            return names_;

        }
        int turns = size / cardinality + 1;
        for (int i = 0; i < turns; i++) {
            for (int j = 0; j < cardinality; j++) {
                if (i * cardinality + j == size) {
                    return names_;
                }
                names_[i * cardinality + j] = uniqueNames[j];

            }
        }

        return names_;

    }

    static public Table createTable(String folder, String name, int pageSize, int size, boolean shuffled, int range, int cardinality) throws Exception {
        Table table = Directory.getTable(folder, name, pageSize, true);
        //table.initLoad();

        Long[] array1 = new Long[(int) Math.ceil((double) size / range)];
        for (int i = 0; i < array1.length; i++) {
            array1[i] = Long.valueOf(i * range);
        }

        if (shuffled) {
            shuffleArray(array1);
        }

        String names_[] = generateNames(cardinality, array1.length);
        
        for (int i = 0; i < array1.length; i++) {
            //String text = name + "(" + array1[i] + ")";
            String text = names_[i];
            //text = Utils.pad(text, 40);
            table.addRecord(array1[i],text );
            
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
            array1[i] = Long.valueOf(i * range);
        }

        if (shuffled) {
            shuffleArray(array1);
        }

        for (Long array11 : array1) {
            String text = array11 + "";
            table.addRecord(array11, text);
        }
        table.flushDB();
        return table;
    }

    public static String pad(String text, int len) {
        if (text.length() > len) {
            return text;
        }
        StringBuilder buf = new StringBuilder(text);
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

    public static boolean match(Comparable value1, Comparable value2, int comparisonType) {

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

    public static void main(String[] args) {
        String names_[] = Utils.generateNames(7, 100);
        int c = 0;
        for (String name : names_) {
            System.out.println(name);
            System.out.println(c++);
        }
    }

}
