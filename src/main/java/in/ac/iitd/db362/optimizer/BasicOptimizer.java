package in.ac.iitd.db362.optimizer;

import in.ac.iitd.db362.catalog.Catalog;
import in.ac.iitd.db362.api.PlanPrinter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import in.ac.iitd.db362.catalog.StatisticsQueryService;
import in.ac.iitd.db362.operators.*;
/**
 * A basic optimizer implementation. Feel free and be creative in designing your optimizer.
 * Do not change the constructor. Use the catalog for various statistics that are available.
 * For everything in your optimization logic, you are free to do what ever you want.
 * Make sure to write efficient code!
 */
public class BasicOptimizer implements Optimizer {

    // Do not remove or rename logger
    protected final Logger logger = LogManager.getLogger(this.getClass());

    // Do not remove or rename catalog. You'll need it in your optimizer
    private final Catalog catalog;
    // private final Logger logger = LogManager.getLogger(this.getClass());
    // private final Catalog catalog;
    private final StatisticsQueryService statsService;
    

    /**
     * DO NOT CHANGE THE CONSTRUCTOR!
     *
     * @param catalog
     */
    public BasicOptimizer(Catalog catalog) {
        this.catalog = catalog;
        this.statsService = new StatisticsQueryService(catalog);
    }

    /**
     * Basic optimization that currently does not modify the plan. Your goal is to come up with
     * an optimization strategy that should find an optimal plan. Come up with your own ideas or adopt the ones
     * discussed in the lecture to efficiently enumerate plans, a search strategy along with a cost model.
     *
     * @param plan The original query plan.
     * @return The (possibly) optimized query plan.
     */
    @Override
    public Operator optimize(Operator plan) {
        // System.out.println("HI");


        logger.info("Optimizing Plan:\n{}", PlanPrinter.getPlanString(plan));
        if (plan == null) {
            logger.warn("Received null plan, returning null");
            return null;
        }
        
        
        
        
        Operator optimized = pushDownFilters(plan);
        // System.out.println("HI");
        optimized = reorderJoins(optimized);
        // System.out.println("HI");
        optimized = pushDownProjects(optimized);

        
        
        // Ensure we never return null
        if (optimized == null) {
            logger.warn("Optimization returned null, returning original plan");
            return plan;
        }
        
        logger.info("Optimized Plan:\n{}", PlanPrinter.getPlanString(optimized));
        return optimized;
    }

    

    private Operator pushDownFilters(Operator plan) {
        if (plan == null) return null;
        
        if (plan instanceof FilterOperator) {
            FilterOperator filter = (FilterOperator) plan;
            Operator child = filter.getChild();
            
            if (child instanceof JoinOperator) {
                JoinOperator join = (JoinOperator) child;
                
                // Try to push filter to left branch
                if (canPushToLeft(filter, join.getLeftChild())) {
                    Operator newLeft = pushDownFilters(
                        new FilterOperator(join.getLeftChild(), filter.getPredicate()));
                    if (newLeft != null) {
                        return pushDownFilters(
                            new JoinOperator(newLeft, join.getRightChild(), join.getPredicate()));
                    }
                }
                
                // Try to push filter to right branch
                if (canPushToRight(filter, join.getRightChild())) {
                    Operator newRight = pushDownFilters(
                        new FilterOperator(join.getRightChild(), filter.getPredicate()));
                    if (newRight != null) {
                        return pushDownFilters(
                            new JoinOperator(join.getLeftChild(), newRight, join.getPredicate()));
                    }
                }
            }
            
            // If we couldn't push down, optimize the child
            Operator newChild = pushDownFilters(child);
            return newChild != null ? new FilterOperator(newChild, filter.getPredicate()) : null;
        }
        return plan;
    }

    private boolean canPushToLeft(FilterOperator filter, Operator leftChild) {
        // Check if filter only references left child's columns
        Set<String> filterCols = extractColumnReferences(filter.getPredicate());
        Set<String> leftCols = getSchemaColumns(leftChild);
        return leftCols.containsAll(filterCols);
    }
    private boolean canPushToRight(FilterOperator filter, Operator rightChild) {
        Set<String> filterCols = extractColumnReferences(filter.getPredicate());
        Set<String> rightCols = getSchemaColumns(rightChild);
        return rightCols.containsAll(filterCols);
    }


    private Operator reorderJoins(Operator plan) {
        if (plan == null) {
            return null;
        }
        // System.out.println("hola");
    
        // Handle JoinOperator specifically
        if (plan instanceof JoinOperator) {
            JoinOperator join = (JoinOperator) plan;
            Operator leftChild = join.getLeftChild();
            Operator rightChild = join.getRightChild();
            
            if (leftChild == null || rightChild == null) {
                return plan; // can't reorder if children are null
            }
            
            double leftCost = estimateCost(leftChild);
            double rightCost = estimateCost(rightChild);
            // System.out.println("leftCost: " + leftCost + ", rightCost: " + rightCost);

            
            
            // Only swap if left is significantly smaller (20% threshold)
            if (leftCost * 0.8 < rightCost) {
                return new JoinOperator(
                    join.getRightChild(),
                    join.getLeftChild(),
                    swapJoinPredicate(join.getPredicate())
                );
            }
            // Still need to optimize children even if we don't swap
            Operator newLeft = reorderJoins(join.getLeftChild());
            Operator newRight = reorderJoins(join.getRightChild());
            return new JoinOperator(newLeft, newRight, join.getPredicate());
        }
    
        // System.out.println("shit");
    
        // Handle other operator types
        if (plan instanceof FilterOperator) {
            FilterOperator filter = (FilterOperator) plan;
            Operator child = filter.getChild();
            if (child == null) {
                return plan;
            }
            Operator newChild = reorderJoins(child);
            return newChild != null ? new FilterOperator(newChild, filter.getPredicate()) : plan;
        }
        else if (plan instanceof ProjectOperator) {
            ProjectOperator project = (ProjectOperator) plan;
            Operator child = project.getChild();
            if (child == null) {
                return plan;
            }
            Operator newChild = reorderJoins(child);
            return newChild != null ? new ProjectOperator(newChild, project.getProjectedColumns(), project.isDistinct()) : plan;
        }
        else if (plan instanceof ScanOperator || plan instanceof SinkOperator) {
            // Base case - no children to optimize
            return plan;
        }
    
        // System.out.println("FUCK");
    
        // Should never reach here for valid plans
        throw new UnsupportedOperationException("Unsupported operator type: " + plan.getClass());
    }




    


    private double estimateCost(Operator op) {

        // System.out.println("estimating");

        if (op == null) {
            return Double.MAX_VALUE; // or some high value
        }
        if (op instanceof ScanOperator) {
            // System.out.println("scanning");
            String tableName = extractTableName((ScanOperator)op);
            // System.out.println("scandone");
            return catalog.getTableStatistics(tableName).getNumRows();
            
        }
        else if (op instanceof FilterOperator) {
            FilterOperator filter = (FilterOperator)op;
            double childCost = estimateCost(filter.getChild());
            double selectivity = estimateFilterSelectivity(filter);
            return childCost * selectivity;
        }
        else if (op instanceof JoinOperator) {
            JoinOperator join = (JoinOperator)op;
            return estimateCost(join.getLeftChild()) * estimateCost(join.getRightChild());
        }
        return 1.0;
    }



    private Operator pushDownProjects(Operator plan) {
        if (plan instanceof ProjectOperator) {
            ProjectOperator project = (ProjectOperator) plan;
            Set<String> requiredColumns = new HashSet<>(project.getProjectedColumns());
            Operator child = project.getChild();
            
            if (child instanceof JoinOperator) {
                JoinOperator join = (JoinOperator) child;
                
                // Create new projections for children
                Operator newLeft = createChildProjection(join.getLeftChild(), requiredColumns);
                Operator newRight = createChildProjection(join.getRightChild(), requiredColumns);
                
                // Create new join with projected children
                Operator newJoin = new JoinOperator(newLeft, newRight, join.getPredicate());
                
                // Keep the original projection (may need to eliminate duplicates)
                return new ProjectOperator(newJoin, project.getProjectedColumns(), project.isDistinct());
            }
            
            // Recursively optimize child
            Operator newChild = pushDownProjects(child);
            return new ProjectOperator(newChild, project.getProjectedColumns(), project.isDistinct());
        }
        return plan;
    }
    private Operator createChildProjection(Operator child, Set<String> requiredColumns) {
        Set<String> childColumns = getSchemaColumns(child);
        Set<String> columnsToKeep = requiredColumns.stream()
            .filter(childColumns::contains)
            .collect(Collectors.toSet());
        
        if (!columnsToKeep.isEmpty()) {
            return new ProjectOperator(child, new ArrayList<>(columnsToKeep), false);
        }
        return child;
    }

    private Operator pushProjectToChild(Operator child, Set<String> requiredColumns) {
        Set<String> childColumns = getSchemaColumns(child);
        Set<String> columnsToKeep = requiredColumns.stream()
            .filter(childColumns::contains)
            .collect(Collectors.toSet());
        
        if (!columnsToKeep.isEmpty()) {
            return new ProjectOperator(child, new ArrayList<>(columnsToKeep), false);
        }
        return child;
    }

    

    private Set<String> extractColumnReferences(Predicate predicate) {
        Set<String> columns = new HashSet<>();
        if (predicate instanceof ComparisonPredicate) {
            ComparisonPredicate cmp = (ComparisonPredicate) predicate;
            if (cmp.getLeftOperand() instanceof String) {
                columns.add((String) cmp.getLeftOperand());
            }
            if (cmp.getRightOperand() instanceof String) {
                columns.add((String) cmp.getRightOperand());
            }
        }
        return columns;
    }

    private Set<String> getSchemaColumns(Operator op) {
        if (op instanceof ScanOperator) {
            return new HashSet<>(((ScanOperator) op).getSchema());
        } else if (op instanceof ProjectOperator) {
            return new HashSet<>(((ProjectOperator) op).getProjectedColumns());
        } else if (op instanceof FilterOperator) {
            return getSchemaColumns(((FilterOperator) op).getChild());
        } else if (op instanceof JoinOperator) {
            Set<String> columns = new HashSet<>();
            columns.addAll(getSchemaColumns(((JoinOperator) op).getLeftChild()));
            columns.addAll(getSchemaColumns(((JoinOperator) op).getRightChild()));
            return columns;
        }
        return Collections.emptySet();
    }

    private JoinPredicate swapJoinPredicate(JoinPredicate predicate) {
        if (predicate instanceof EqualityJoinPredicate) {
            EqualityJoinPredicate eq = (EqualityJoinPredicate) predicate;
            return new EqualityJoinPredicate(eq.getRightColumn(), eq.getLeftColumn());
        }
        return predicate;
    }

    private String extractTableName(ScanOperator scan) {
        return scan.getFilePath();  // Use full path instead of just filename
    }

    private double estimateFilterSelectivity(FilterOperator filter) {
        if (filter.getPredicate() instanceof ComparisonPredicate) {
            ComparisonPredicate cmp = (ComparisonPredicate) filter.getPredicate();
            String column = null;
            Object value = null;
            
            // Determine which side is the column reference
            if (cmp.getLeftOperand() instanceof String && cmp.getRightOperand() instanceof Number) {
                column = (String) cmp.getLeftOperand();
                value = cmp.getRightOperand();
            } else if (cmp.getRightOperand() instanceof String && cmp.getLeftOperand() instanceof Number) {
                column = (String) cmp.getRightOperand();
                value = cmp.getLeftOperand();
            }
            
            if (column != null) {
                String tableName = catalog.getTableForColumn(column);
                if (tableName != null) {
                    String operator = cmp.getOperator();
                    if (operator.equals("=")) {
                        return statsService.getEqualitySelectivity(tableName, column, value);
                    } else if (operator.equals(">") || operator.equals("<") || 
                            operator.equals(">=") || operator.equals("<=")) {
                        // For range predicates, estimate 10% selectivity
                        return 0.1;
                    }
                }
            }
        }
        // Default selectivity
        return 0.5;
    }







}

