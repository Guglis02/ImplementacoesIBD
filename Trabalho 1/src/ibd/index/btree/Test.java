/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.index.btree;

import ibd.persistent.PersistentPageFile;
import java.nio.file.Paths;

/**
 *
 * @author Sergio
 */
public class Test {

    public boolean add(BPlusTreeFile tree, RowSchema keySchema, RowSchema valueSchema, long value1, long value2, long value3) {
        Key key = new Key(keySchema);
        key.setKeys(new Long[]{value1, value2, value3});
        Value value = new Value(valueSchema);
        value.set(0, "record:" + (value1 + "-" + value2 + "-" + value3));

        return tree.insert(key, value);

    }

    public boolean delete(BPlusTreeFile tree, RowSchema keySchema, long value1, long value2, long value3) {
        Key key = new Key(keySchema);
        key.setKeys(new Long[]{value1, value2, value3});

        Value value = tree.delete(key);
        return (value != null);

    }

    public String query(BPlusTreeFile tree, RowSchema keySchema, long value1, long value2, long value3) {
        Key key = new Key(keySchema);
        key.setKeys(new Long[]{value1, value2, value3});

        Value value = tree.search(key);
        if (value != null) {
            return value.toString();
        }

        return null;
    }

    public void queryInserted(BPlusTreeFile tree, RowSchema keySchema, long value1, long value2, long value3) {
        Key key = new Key(keySchema);
        key.setKeys(new Long[]{value1, value2, value3});
        String result = query(tree, keySchema, value1, value2, value3);
        if (result != null) {
            System.out.println("Success. Found " + result);
        } else {
            System.out.println("Error. Not found " + value1 + "-" + value2 + "-" + value3);
        }
    }

    public void queryDeleted(BPlusTreeFile tree, RowSchema keySchema, long value1, long value2, long value3) {
        Key key = new Key(keySchema);
        key.setKeys(new Long[]{value1, value2, value3});
        String result = query(tree, keySchema, value1, value2, value3);
        if (result == null) {
            System.out.println("Success. Not found " + value1 + "-" + value2 + "-" + value3);
        } else {
            System.out.println("Error. Should not find " + value1 + "-" + value2 + "-" + value3);
        }
    }

    public static void main(String[] args) {
        try {

            boolean newDatabase = false;

            RowSchema keySchema = new RowSchema(3);
            keySchema.addLong();
            keySchema.addLong();
            keySchema.addLong();

            RowSchema valueSchema = new RowSchema(1);
            valueSchema.addString();

            PersistentPageFile p = new PersistentPageFile(4096, Paths.get("c:\\teste\\mtree\\mtree"), newDatabase);
            BPlusTreeFile tree = new BPlusTreeFile(5, 7, p, keySchema, valueSchema);

            Test test = new Test();

            if (newDatabase) {
                test.add(tree, keySchema, valueSchema, 0, 0, 0);
                test.add(tree, keySchema, valueSchema, 0, 0, 1);
                test.add(tree, keySchema, valueSchema, 0, 0, 2);
                test.delete(tree, keySchema, 0, 0, 0);
            }

            test.queryInserted(tree, keySchema, 0, 0, 1);
            test.queryDeleted(tree, keySchema, 0, 0, 0);
            test.queryInserted(tree, keySchema, 0, 0, 2);
//            test.update(tree, 1,

            tree.close();
        } catch (Exception ex) {
            //Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
            ex.printStackTrace();
        }
    }

}
