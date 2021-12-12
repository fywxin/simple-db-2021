package io.iamazy.github.simpledb.execution;

import io.iamazy.github.simpledb.common.DbException;
import io.iamazy.github.simpledb.common.Type;
import io.iamazy.github.simpledb.storage.Tuple;
import io.iamazy.github.simpledb.storage.TupleDesc;
import io.iamazy.github.simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private final int aggField;
    private final int groupField;
    private final Aggregator.Op aggOp;
    private OpIterator aggIterator;
    private TupleDesc tupleDesc;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.aggField = afield;
        this.groupField = gfield;
        this.aggOp = aop;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        if (groupField <= -1) {
            return Aggregator.NO_GROUPING;
        }
        return groupField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        if (groupField() >= 0) {
            return child.getTupleDesc().getFieldName(groupField());
        } else {
            return null;
        }
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return aggField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return child.getTupleDesc().getFieldName(aggregateField());
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return aggOp;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();

        Type aggFieldType = child.getTupleDesc().getFieldType(aggField);
        Type groupByFieldType = child.getTupleDesc().getFieldType(groupField);

        Aggregator aggregator;
        if (aggFieldType == Type.INT_TYPE) {
            aggregator = new IntegerAggregator(groupField, groupByFieldType, aggField, aggOp);
        } else {
            aggregator = new StringAggregator(groupField, groupByFieldType, aggField, aggOp);
        }
        while (child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }
        this.aggIterator = aggregator.iterator();
        this.aggIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (aggIterator.hasNext()) {
            return aggIterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        if (tupleDesc == null) {
            int fieldNum = groupField() != Aggregator.NO_GROUPING ? 2 : 1;
            Type[] types = new Type[fieldNum];
            String[] fieldNames = new String[fieldNum];
            if (groupField() != Aggregator.NO_GROUPING) {
                types[0] = child.getTupleDesc().getFieldType(groupField());
                types[1] = child.getTupleDesc().getFieldType(aggregateField());
                fieldNames[0] = groupFieldName();
                fieldNames[1] = aggregateFieldName() + "(" + nameOfAggregatorOp(aggOp) + ")";
            } else {
                types[0] = child.getTupleDesc().getFieldType(aggregateField());
                fieldNames[0] = aggregateFieldName() + "(" + nameOfAggregatorOp(aggOp) + ")";
            }
            tupleDesc = new TupleDesc(types, fieldNames);
        }
        return tupleDesc;

    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
        aggIterator.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (children != null && children.length > 1) {
            child = children[0];
        }
    }

}
