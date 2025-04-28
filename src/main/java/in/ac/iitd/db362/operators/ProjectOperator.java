package in.ac.iitd.db362.operators;

import in.ac.iitd.db362.storage.Tuple;

import java.util.*;

/**
 * Implementation of a simple project operator that implements the operator interface.
 *
 *
 * TODO: Implement the open(), next(), and close() methods!
 * Do not change the constructor or existing member variables.
 */
public class ProjectOperator extends OperatorBase implements Operator {
    private Operator child;
    private List<String> projectedColumns;
    private boolean distinct;

    private Set<List<Object>> seenTuples;
    private List<Integer> columnIndices;
    private Tuple bufferedTuple; // For pushed back tuple


    /**
     * Project operator. If distinct is set to true, it does duplicate elimination
     * @param child
     * @param projectedColumns
     * @param distinct
     */
    public ProjectOperator(Operator child, List<String> projectedColumns, boolean distinct) {
        this.child = child;
        this.projectedColumns = projectedColumns;
        this.distinct = distinct;
    }

    @Override
    public void open() {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Open()");
        // -------------------------

        child.open();
        
        if (distinct) {
            seenTuples = new HashSet<>();
        }
        
        // Get first tuple to determine schema
        bufferedTuple = child.next();
        if (bufferedTuple != null) {
            columnIndices = new ArrayList<>();
            List<String> childSchema = bufferedTuple.getSchema();
            for (String col : projectedColumns) {
                int idx = childSchema.indexOf(col);
                if (idx == -1) {
                    throw new IllegalArgumentException("Column " + col + " not found in input schema");
                }
                columnIndices.add(idx);
            }
        }
    }

    @Override
    public Tuple next() {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Next()");
        // ------------------------

        Tuple inputTuple;
        if (bufferedTuple != null) {
            inputTuple = bufferedTuple;
            bufferedTuple = null;
        } else {
            inputTuple = child.next();
        }
        
        if (inputTuple == null) {
            return null;
        }
        
        // Project the tuple
        List<Object> projectedValues = new ArrayList<>();
        List<String> projectedSchema = new ArrayList<>(projectedColumns);
        
        for (int idx : columnIndices) {
            projectedValues.add(inputTuple.get(idx));
        }
        
        Tuple projectedTuple = new Tuple(projectedValues, projectedSchema);
        
        if (distinct) {
            List<Object> valueList = projectedTuple.getValues();
            if (seenTuples.contains(valueList)) {
                return next(); // Skip duplicate
            }
            seenTuples.add(valueList);
        }
        
        return projectedTuple;
    }

    @Override
    public void close() {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Close()");
        // ------------------------

        child.close();
        if (seenTuples != null) {
            seenTuples.clear();
        }
    }

    // do not remvoe these methods!
    public Operator getChild() {
        return child;
    }

    public List<String> getProjectedColumns() {
        return projectedColumns;
    }

    public boolean isDistinct() {
        return distinct;
    }
}
