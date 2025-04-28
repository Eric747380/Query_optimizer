package in.ac.iitd.db362.operators;

import in.ac.iitd.db362.storage.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class implements the JoinPredicate interface.
 *
 * DO NOT CHANGE the constructor or existing member variables!
 * TODO: Implement the evaluate() method and use it in your implementation of the join operator
 *
 */
public class EqualityJoinPredicate implements JoinPredicate {

    protected final static Logger logger = LogManager.getLogger();

    private final String leftColumn; // left column name
    private final String rightColumn; // right column name

    public EqualityJoinPredicate(String leftColumn, String rightColumn) {
        this.leftColumn = leftColumn;
        this.rightColumn = rightColumn;
    }

    /**
     * Evaluates if left and right tuples.
     * @param left  the tuple from the left input
     * @param right the tuple from the right input
     * @return true if the tuples match based on their leftColumn and rightColumn value
     */
    @Override
    public boolean evaluate(Tuple left, Tuple right) {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Left tuple " + left.getValues() + "[" + left.getSchema() + "]");
        logger.trace("Right tuple " + right.getValues() + "[" + right.getSchema() + "]");
        logger.trace("Condition " + leftColumn + " = " + rightColumn);
        // -------------------------

        try {
            // Get values from both tuples
            Object leftValue = left.get(leftColumn);
            Object rightValue = right.get(rightColumn);
            
            // Handle null values
            if (leftValue == null || rightValue == null) {
                return false;
            }
            
            // Numeric comparison
            if (leftValue instanceof Number && rightValue instanceof Number) {
                return ((Number)leftValue).doubleValue() == 
                       ((Number)rightValue).doubleValue();
            }
            // String comparison
            else {
                return leftValue.equals(rightValue);
            }
        } catch (IllegalArgumentException e) {
            // Column not found in one of the tuples
            return false;
        }
    }


    // DO NOT REMOVE THESE METHODS
    public String getLeftColumn() {
        return leftColumn;
    }

    public String getRightColumn() {
        return rightColumn;
    }

    @Override
    public String toString() {
        return "EqualityJoinPredicate[" +
                "leftColumn='" + leftColumn + '\'' +
                ", rightColumn='" + rightColumn + '\'' +
                ']';
    }
}
