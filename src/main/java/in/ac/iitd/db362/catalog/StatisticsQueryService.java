package in.ac.iitd.db362.catalog;

/**
 *
 * MODIFY/UPDATE -- DO WHAT EVER YOU WANT WITH THIS CLASS!
 *
 * This class provides some methods that the optimizer can use to query the catalog.
 * The starter code is only for your reference. **Feel free** to modify this by
 * implementing the methods * and/or by adding your own methods that you think your
 * optimizer can use when optimizing plans.
 */
public class StatisticsQueryService {
    private final Catalog catalog;

    public StatisticsQueryService(Catalog catalog) {
        this.catalog = catalog;
    }

    public double getEqualitySelectivity(String tableName, String columnName, Object value) {
        TableStatistics tableStats = catalog.getTableStatistics(tableName);
        if (tableStats == null) return 0.1; // Default if no stats
        
        ColumnStatistics<?> colStats = tableStats.getColumnStatistics(columnName);
        if (colStats == null) return 0.1;
        
        // Basic selectivity: 1/distinct_values
        return 1.0 / Math.max(1, colStats.getCardinality());
    }

    public double getEqualitySelectivityUsingHistogram(String tableName, String columnName, Object value) {
        TableStatistics tableStats = catalog.getTableStatistics(tableName);
        if (tableStats == null) return 0.1;
        
        ColumnStatistics<?> colStats = tableStats.getColumnStatistics(columnName);
        if (!(colStats instanceof IntColumnStatistics)) {
            return getEqualitySelectivity(tableName, columnName, value);
        }
        
        IntColumnStatistics intStats = (IntColumnStatistics)colStats;
        int[] histogram = intStats.getHistogram();
        int total = intStats.getNumValues();
        
        // Find which bucket the value falls into
        int bucketSize = (intStats.getMax() - intStats.getMin() + 1) / histogram.length;
        int bucket = ((Integer)value - intStats.getMin()) / bucketSize;
        bucket = Math.min(bucket, histogram.length - 1);
        
        return (double)histogram[bucket] / total;
    }


    public double getRangeSelectivity(String tableName, String columnName, Object lowerBound, Object upperBound) {
        TableStatistics tableStats = catalog.getTableStatistics(tableName);
        if (tableStats == null) return 0.3; // Default guess
        
        ColumnStatistics<?> colStats = tableStats.getColumnStatistics(columnName);
        if (!(colStats instanceof IntColumnStatistics)) return 0.3;
        
        IntColumnStatistics intStats = (IntColumnStatistics)colStats;
        double range = intStats.getMax() - intStats.getMin();
        double covered = ((Number)upperBound).doubleValue() - 
                        ((Number)lowerBound).doubleValue();
        return Math.min(1.0, Math.max(0.0, covered / range));
    }


    public double getRangeSelectivityUsingHistogram(String tableName, String columnName, Object lowerBound, Object upperBound) {
        throw new UnsupportedOperationException("Please implement me first!");
    }


    public Object getMin(String tableName, String columnName) {
        TableStatistics tableStats = catalog.getTableStatistics(tableName);
        if (tableStats == null) return null;
        
        ColumnStatistics<?> colStats = tableStats.getColumnStatistics(columnName);
        return colStats != null ? colStats.getMin() : null;
    }


    public Object getMax(String tableName, String columnName) {
        TableStatistics tableStats = catalog.getTableStatistics(tableName);
        if (tableStats == null) return null;
        
        ColumnStatistics<?> colStats = tableStats.getColumnStatistics(columnName);
        return colStats != null ? colStats.getMax() : null;
    }
}
