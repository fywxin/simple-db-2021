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
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private OpIterator child;
    private final int tableId;
    private final TupleDesc tupleDesc;
    private final AtomicInteger calledCount = new AtomicInteger(0);

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
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
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (calledCount.getAndIncrement() == 0) {
            int records = 0;
            while (child.hasNext()) {
                Tuple next = child.next();
                try {
                    Database.getBufferPool().insertTuple(tid, tableId, next);
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
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (children != null && children.length >= 1) {
            child = children[0];
        }
    }
}
