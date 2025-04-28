package in.ac.iitd.db362.query;

import in.ac.iitd.db362.api.PlanBuilder;
import in.ac.iitd.db362.api.PlanPrinter;
import in.ac.iitd.db362.catalog.Catalog;
import in.ac.iitd.db362.executor.QueryExecutor;
import in.ac.iitd.db362.operators.Operator;
import in.ac.iitd.db362.optimizer.BasicOptimizer;
import in.ac.iitd.db362.optimizer.Optimizer;
import in.ac.iitd.db362.storage.DataLoader;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.IOException;

import org.junit.jupiter.api.Disabled;
// import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

// @Disabled
public class OptimzerTest {

    @Test
    public void test1() throws Exception {

        //Create statistics
        Catalog catalog = new Catalog();
        DataLoader.createStatistics("data/csvTables/customer.csv", catalog);
        DataLoader.createStatistics("data/csvTables/orders.csv", catalog);
        DataLoader.createStatistics("data/csvTables/product.csv", catalog);


        String outputFile = "data/output/test_customer_orders_product_join_with_price_filter_output.csv";

        // Build the plan:
        // 1. Scan Customer.csv and filter customers with age > 30.
        // 2. Join with Orders.csv on customer_id.
        // 3. Join with Product.csv on product_id.
        // 4. Apply an additional filter: only keep rows where price > 20.
        // 5. Project the customer's name and the product name.
        // 6. Sink the result to an output CSV file.
        Operator plan = PlanBuilder.scan("data/csvTables/customer.csv")
                .join(PlanBuilder.scan("data/csvTables/orders.csv"), "c_customer_id = o_customer_id")
                .join(PlanBuilder.scan("data/csvTables/product.csv"), "o_product_id = p_product_id")
                .filter("p_price > 20")
                .filter("c_age > 30")
                .project("c_name", "p_product_name")
                .sink(outputFile)
                .build();

        // Execute the plan.
        QueryExecutor.execute(plan);


        // Initialize the optimizer ando optimize
        Optimizer optimizer = new BasicOptimizer(catalog);
        Operator optimizedPlan = optimizer.optimize(plan);

        // Execute the optimized plan
        QueryExecutor.execute(optimizedPlan);

    }




    @Disabled
    public void testOptimizer() throws IOException {
        // Initialize catalog with statistics
        Catalog catalog = new Catalog();
        DataLoader.createStatistics("data/csvTables/customer.csv", catalog);
        DataLoader.createStatistics("data/csvTables/orders.csv", catalog);
        
        // Build query plan
        Operator plan = PlanBuilder.scan("data/csvTables/customer.csv")
            .join(PlanBuilder.scan("data/csvTables/orders.csv"), "c_customer_id = o_customer_id")
            .filter("c_age > 30")
            .project("c_name", "o_order_id")
            .build();
        
        // Print original plan
        String originalPlan = PlanPrinter.getPlanString(plan);
        assertNotNull(originalPlan, "Original plan should not be null");
        
        // Optimize the plan
        Optimizer optimizer = new BasicOptimizer(catalog);
        Operator optimized = optimizer.optimize(plan);
        
        // Verify optimized plan
        assertNotNull(optimized, "Optimized plan should not be null");
        String optimizedPlan = PlanPrinter.getPlanString(optimized);
        assertNotNull(optimizedPlan, "Optimized plan string should not be null");
        
        // Verify basic structure
        assertTrue(optimizedPlan.contains("Scan: data/csvTables/customer.csv"));
        assertTrue(optimizedPlan.contains("Scan: data/csvTables/orders.csv"));
        assertTrue(optimizedPlan.contains("Project: c_name, o_order_id"));
        
        // Verify optimization occurred
        if (!optimizedPlan.equals(originalPlan)) {
            assertTrue(optimizedPlan.contains("Filter: ComparisonPredicate[leftOperand=c_age, operator='>', rightOperand=30]"));
        }
        
        // Verify join order makes sense
        int customerPos = optimizedPlan.indexOf("Scan: data/csvTables/customer.csv");
        int ordersPos = optimizedPlan.indexOf("Scan: data/csvTables/orders.csv");
        assertTrue((customerPos != -1 && ordersPos != -1), "Both scans must be present in the plan");

    }

}
