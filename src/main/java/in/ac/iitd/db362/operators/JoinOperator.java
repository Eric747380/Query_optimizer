package in.ac.iitd.db362.operators;

import in.ac.iitd.db362.storage.Tuple;

import java.util.*;

/**
 * The join operator performs a Hash Join.
 * TODO: Implement the open(), next(), and close() methods.
 *
 * Do not change the constructor and member variables or getters
 * Do not remove logging! otherwise your test cases will fail!
 */
public class JoinOperator extends OperatorBase implements Operator {
    private Operator leftChild;
    private Operator rightChild;
    private JoinPredicate predicate;


    // For hash join implementation
    private Map<Object, List<Tuple>> hashTable;
    private Iterator<Tuple> rightTuples;
    private Tuple currentLeftTuple;
    private Tuple nextRightTuple;
    private List<Tuple> currentMatches;
    private int currentMatchIndex;  

    public JoinOperator(Operator leftChild, Operator rightChild, JoinPredicate predicate) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.predicate = predicate;
    }
    public JoinOperator copyWithNewLeft(Operator newLeft) {
        return new JoinOperator(newLeft, this.rightChild, this.predicate);
    }
    
    public JoinOperator copyWithNewRight(Operator newRight) {
        return new JoinOperator(this.leftChild, newRight, this.predicate);
    }

    @Override
    public void open() {
        // Do not remove logging--
        logger.trace("Open()");
        // ----------------------

        leftChild.open();
        rightChild.open();
        
        // Build phase: Hash the right child (build side)
        hashTable = new HashMap<>();
        rightTuples = new Iterator<Tuple>() {
            Tuple nextTuple = rightChild.next();
            
            @Override
            public boolean hasNext() {
                return nextTuple != null;
            }
            
            @Override
            public Tuple next() {
                Tuple current = nextTuple;
                nextTuple = rightChild.next();
                return current;
            }
        };
        
        // Build hash table from right child
        while (rightTuples.hasNext()) {
            Tuple rightTuple = rightTuples.next();
            Object joinKey = rightTuple.get(((EqualityJoinPredicate)predicate).getRightColumn());
            hashTable.computeIfAbsent(joinKey, k -> new ArrayList<>()).add(rightTuple);
        }
        
        currentLeftTuple = null;
        nextRightTuple = null;
        currentMatches = null;
        currentMatchIndex = 0;

    }

    @Override
    public Tuple next() {
        // Do not remove logging--
        logger.trace("Next()");
        // ----------------------

        while (true) {
            // Get next left tuple if needed
            if (currentLeftTuple == null) {
                currentLeftTuple = leftChild.next();
                if (currentLeftTuple == null) {
                    return null; // No more tuples
                }
                
                // Probe hash table
                Object joinKey = currentLeftTuple.get(((EqualityJoinPredicate)predicate).getLeftColumn());
                currentMatches = hashTable.getOrDefault(joinKey, Collections.emptyList());
                currentMatchIndex = 0;
            }
            
            // Check if we have more matches for current left tuple
            if (currentMatchIndex < currentMatches.size()) {
                Tuple rightTuple = currentMatches.get(currentMatchIndex++);
                if (predicate.evaluate(currentLeftTuple, rightTuple)) {
                    return combineTuples(currentLeftTuple, rightTuple);
                }
            } 
            else {
                // No more matches for this left tuple
                currentLeftTuple = null;
            }
        }
    }

    @Override
    public void close() {
        // Do not remove logging ---
        logger.trace("Close()");
        // ------------------------

        leftChild.close();
        rightChild.close();
        hashTable = null;
        rightTuples = null;
        currentLeftTuple = null;
        currentMatches = null;
    }

    private Tuple combineTuples(Tuple left, Tuple right) {
        // Combine values
        List<Object> combinedValues = new ArrayList<>();
        combinedValues.addAll(left.getValues());
        combinedValues.addAll(right.getValues());
        
        // Combine schemas
        List<String> combinedSchema = new ArrayList<>();
        combinedSchema.addAll(left.getSchema());
        combinedSchema.addAll(right.getSchema());
        
        return new Tuple(combinedValues, combinedSchema);
    }


    // Do not remove these methods!
    public Operator getLeftChild() {
        return leftChild;
    }

    public Operator getRightChild() {
        return rightChild;
    }

    public JoinPredicate getPredicate() {
        return predicate;
    }
}
