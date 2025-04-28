package in.ac.iitd.db362.operators;

import in.ac.iitd.db362.storage.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Note: ONLY IMPLEMENT THE EVALUATE METHOD.
 * TODO: Implement the evaluate() method
 *
 * DO NOT CHANGE the constructor or existing member variables.
 *
 * A comparison predicate for simple atomic predicates.
 */
public class ComparisonPredicate implements Predicate {

    protected final static Logger logger = LogManager.getLogger();
    private final Object leftOperand;   // Either a constant or a column reference (String)
    private final String operator;        // One of: =, >, >=, <, <=, !=
    private final Object rightOperand;  // Either a constant or a column reference (String)

    public ComparisonPredicate(Object leftOperand, String operator, Object rightOperand) {
        this.leftOperand = leftOperand;
        this.operator = operator;
        this.rightOperand = rightOperand;
    }

    /**
     * Evaluate a tuple
     * @param tuple the tuple to evaluate
     * @return return true if leftOperator operator righOperand holds in that tuple
     */
    // @Override
    // public boolean evaluate(Tuple tuple) {
    //     // DO NOT REMOVE LOGGING ---
    //     logger.trace("Evaluating tuple " + tuple.getValues() + " with schema " + tuple.getSchema());
    //     logger.trace("[Predicate] " + leftOperand + " " + operator + " " + rightOperand);
    //     // -------------------------

    //     //TODO: Implement me!

    //     // Remove me after your implementation
    //     throw new RuntimeException("Method not yet implemented");
    // }
    @Override
    public boolean evaluate(Tuple tuple) {
        logger.trace("Evaluating tuple " + tuple.getValues() + " with schema " + tuple.getSchema());
        logger.trace("[Predicate] " + leftOperand + " " + operator + " " + rightOperand);

        // Get left value (could be column name or constant)
        Object leftValue = (leftOperand instanceof String) ? 
                        tuple.get((String)leftOperand) : leftOperand;
        
        // Get right value (could be column name or constant)
        Object rightValue = (rightOperand instanceof String) ? 
                        tuple.get((String)rightOperand) : rightOperand;

        // Handle null values
        if (leftValue == null || rightValue == null) {
            return false;
        }

        // Compare values based on their types
        if (leftValue instanceof Number && rightValue instanceof Number) {
            double left = ((Number)leftValue).doubleValue();
            double right = ((Number)rightValue).doubleValue();
            
            switch (operator) {
                case "=":  return left == right;
                case "!=": return left != right;
                case ">":  return left > right;
                case ">=": return left >= right;
                case "<":  return left < right;
                case "<=": return left <= right;
                default: throw new IllegalArgumentException("Unknown operator: " + operator);
            }
        } 
        else { // String comparison
            String leftStr = leftValue.toString();
            String rightStr = rightValue.toString();
            
            switch (operator) {
                case "=":  return leftStr.equals(rightStr);
                case "!=": return !leftStr.equals(rightStr);
                case ">":  return leftStr.compareTo(rightStr) > 0;
                case ">=": return leftStr.compareTo(rightStr) >= 0;
                case "<":  return leftStr.compareTo(rightStr) < 0;
                case "<=": return leftStr.compareTo(rightStr) <= 0;
                default: throw new IllegalArgumentException("Unknown operator: " + operator);
            }
        }
    }

    // DO NOT REMOVE these functions! ---
    @Override
    public String toString() {
        return "ComparisonPredicate[" +
                "leftOperand=" + leftOperand +
                ", operator='" + operator + '\'' +
                ", rightOperand=" + rightOperand +
                ']';
    }
    public Object getLeftOperand() {
        return leftOperand;
    }

    public String getOperator() {
        return operator;
    }
    public Object getRightOperand() {
        return rightOperand;
    }

}
