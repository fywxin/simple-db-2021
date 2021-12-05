package io.iamazy.github.simpledb.systemtest;

import static org.junit.Assert.*;

import io.iamazy.github.simpledb.common.DbException;
import io.iamazy.github.simpledb.execution.Filter;
import io.iamazy.github.simpledb.execution.Predicate;
import io.iamazy.github.simpledb.execution.SeqScan;
import io.iamazy.github.simpledb.storage.HeapFile;
import io.iamazy.github.simpledb.transaction.TransactionAbortedException;
import io.iamazy.github.simpledb.transaction.TransactionId;

public class FilterTest extends FilterBase {
    @Override
    protected int applyPredicate(HeapFile table, TransactionId tid, Predicate predicate)
            throws DbException, TransactionAbortedException {
        SeqScan ss = new SeqScan(tid, table.getId(), "");
        Filter filter = new Filter(predicate, ss);
        filter.open();

        int resultCount = 0;
        while (filter.hasNext()) {
            assertNotNull(filter.next());
            resultCount += 1;
        }

        filter.close();
        return resultCount;
    }

    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(FilterTest.class);
    }
}
