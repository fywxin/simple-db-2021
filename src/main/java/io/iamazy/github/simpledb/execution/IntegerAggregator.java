package io.iamazy.github.simpledb.execution;

import io.iamazy.github.simpledb.common.Type;
import io.iamazy.github.simpledb.storage.Field;
import io.iamazy.github.simpledb.storage.IntField;
import io.iamazy.github.simpledb.storage.StringField;
import io.iamazy.github.simpledb.storage.Tuple;
import io.iamazy.github.simpledb.storage.TupleDesc;
import io.iamazy.github.simpledb.storage.TupleIterator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int groupByField;
    private final Type groupByFieldType;
    private final int aggField;
    private final Op op;
    private TupleDesc tupleDesc;
    private final Map<Object, Tuple> aggMap;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.groupByField = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggField = afield;
        this.op = what;
        this.aggMap = new ConcurrentHashMap<>();
        initTupleDesc();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field field = tup.getField(groupByField);
        Object value;
        if (groupByFieldType == Type.INT_TYPE) {
            IntField intField = (IntField) field;
            value = intField.getValue();
        } else {
            StringField stringField = (StringField) field;
            value = stringField.getValue();
        }
        int updateField = groupByField == NO_GROUPING ? 0 : 1;
        if (!aggMap.containsKey(value)) {
            Tuple tuple = new Tuple(tupleDesc);
            if (groupByField != NO_GROUPING) {
                tuple.setField(0, field);
            }
            if (op == Op.COUNT) {
                tuple.setField(updateField, new IntField(1));
            } else {
                tuple.setField(updateField, tup.getField(aggField));
            }
            aggMap.put(value, tuple);
            return;
        }
        IntField intAggField = (IntField) aggMap.get(value).getField(updateField);
        IntField tupAggField = (IntField) tup.getField(aggField);
        switch (op) {
            case COUNT: {
                aggMap.get(value).setField(updateField, new IntField(intAggField.getValue() + 1));
                break;
            }
            case AVG: {
                aggMap.get(value).setField(updateField, new IntField((intAggField.getValue() + tupAggField.getValue()) / 2));
                break;
            }
            case MAX: {
                boolean compare = tupAggField.compare(Predicate.Op.GREATER_THAN, intAggField);
                if (compare) {
                    aggMap.get(value).setField(updateField, tupAggField);
                }
                break;
            }
            case MIN: {
                boolean compare = tupAggField.compare(Predicate.Op.LESS_THAN, intAggField);
                if (compare) {
                    aggMap.get(value).setField(updateField, tupAggField);
                }
                break;
            }
            case SUM: {
                aggMap.get(value).setField(updateField, new IntField(intAggField.getValue() + tupAggField.getValue()));
                break;
            }
            default: {
                throw new IllegalArgumentException("aggregate operator is invalid.");
            }
        }
    }

    private void initTupleDesc() {
        Type[] types;
        if (groupByField == NO_GROUPING && groupByFieldType == null) {
            types = new Type[]{Type.INT_TYPE};
        } else {
            types = new Type[]{groupByFieldType, Type.INT_TYPE};
        }
        tupleDesc = new TupleDesc(types);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new TupleIterator(tupleDesc, aggMap.values());
    }

}
