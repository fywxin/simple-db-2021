package io.iamazy.github.simpledb.test;

import io.iamazy.github.simpledb.common.Database;
import io.iamazy.github.simpledb.common.Type;
import io.iamazy.github.simpledb.execution.SeqScan;
import io.iamazy.github.simpledb.storage.HeapFile;
import io.iamazy.github.simpledb.storage.Tuple;
import io.iamazy.github.simpledb.storage.TupleDesc;
import io.iamazy.github.simpledb.transaction.TransactionId;

import java.io.File;

// 相当于 `SELECT * FROM some_data_file`
public class Lab1Test {

    public static void main(String[] args) {

        Type[] types = new Type[] {Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String[] names = new String[] {"field0", "field1", "field2"};
        TupleDesc tupleDesc = new TupleDesc(types, names);

        HeapFile table1 = new HeapFile(new File("docs/lab1/some_data_file.dat"), tupleDesc);
        Database.getCatalog().addTable(table1, "test");

        TransactionId tid = new TransactionId();
        SeqScan scan = new SeqScan(tid, table1.getId());

        try {
            scan.open();
            while (scan.hasNext()) {
                Tuple tuple = scan.next();
                System.out.println(tuple);
            }
            scan.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            System.out.println("Exception: " + e);
        }
    }
}
