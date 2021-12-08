package io.iamazy.github.simpledb.storage;

import io.iamazy.github.simpledb.common.DbException;
import io.iamazy.github.simpledb.index.BTreeFile;
import io.iamazy.github.simpledb.transaction.TransactionAbortedException;
import io.iamazy.github.simpledb.transaction.TransactionId;

public class HeapFileIterator extends AbstractDbFileIterator{

    final TransactionId tid;
    final HeapFile f;

    public HeapFileIterator(HeapFile f, TransactionId tid) {
        this.f = f;
        this.tid = tid;
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        return null;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {

    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {

    }
}
