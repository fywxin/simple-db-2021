package io.iamazy.github.simpledb.optimizer;

import io.iamazy.github.simpledb.execution.Predicate;

import java.util.Arrays;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private final int min;
    private final int max;
    private final int[] values;
    private int ntups;
    private final double width;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.values = new int[buckets];
        this.max = max;
        this.min = min;
        this.ntups = 0;
        this.width = (max - min + 1) / (double) buckets;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        if (v >= min && v <= max) {
            int idx = (int) ((v - min) / width);
            this.values[idx]++;
            this.ntups++;
        }
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        // some code goes here
        if (op == Predicate.Op.NOT_EQUALS) {
            return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
        }

        if (op == Predicate.Op.GREATER_THAN) {
            return 1 - estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v);
        }

        if (op == Predicate.Op.LESS_THAN) {
            return 1 - estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ, v);
        }

        int idx = (int) ((v - min) / width);
        double left = (idx * width);
        double right = ((idx + 1) * width);
        switch (op) {
            case EQUALS: {
                if (v < min || v > max) {
                    return 0;
                }
                int h = values[idx];
                return (h / width) / ntups;
            }
            case LESS_THAN_OR_EQ: {
                if (v < min) {
                    return 0;
                }
                if (v > max) {
                    return 1;
                }
                int less = Arrays.stream(values).limit(idx).sum();
                double hit = ((v - left) * values[idx] / width);
                return (less + hit) / ntups;
            }
            case GREATER_THAN_OR_EQ: {
                if (v < min) {
                    return 1;
                }
                if (v > max) {
                    return 0;
                }
                int greater = Arrays.stream(values).skip(idx).sum();
                double hit = ((right - v) * values[idx]) / width;
                return (greater + hit) / ntups;
            }
        }
        return 0;
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        int sum = Arrays.stream(values).sum();
        return (double) sum / ntups;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return Arrays.toString(values);
    }
}
