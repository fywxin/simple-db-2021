package io.iamazy.github.simpledb.execution;

import io.iamazy.github.simpledb.common.Database;
import io.iamazy.github.simpledb.common.DbException;
import io.iamazy.github.simpledb.common.Debug;
import io.iamazy.github.simpledb.common.Type;
import io.iamazy.github.simpledb.storage.BufferPool;
import io.iamazy.github.simpledb.storage.IntField;
import io.iamazy.github.simpledb.storage.Tuple;
import io.iamazy.github.simpledb.storage.TupleDesc;
import io.iamazy.github.simpledb.transaction.TransactionAbortedException;
import io.iamazy.github.simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private OpIterator child;
    private final TupleDesc tupleDesc;
    private final AtomicInteger calledCount = new AtomicInteger(0);

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (calledCount.getAndIncrement() == 0) {
            int records = 0;
            while (child.hasNext()) {
                Tuple next = child.next();
                try {
                    Database.getBufferPool().deleteTuple(tid, next);
                    records++;
                } catch (IOException e) {
                    Debug.log(e.getMessage());
                }
            }
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0, new IntField(records));
            return tuple;
        } else {
            return null;
        }
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (children != null && children.length >= 1) {
            child = children[0];
        }
    }

}
