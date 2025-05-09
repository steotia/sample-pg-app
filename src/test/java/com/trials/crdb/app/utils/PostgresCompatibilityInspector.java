package com.trials.crdb.app.utils;

import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A database schema inspector that uses only PostgreSQL-compatible information_schema
 * queries to test compatibility across different database systems.
 */
public class PostgresCompatibilityInspector {

    private final JdbcTemplate jdbcTemplate;

    public PostgresCompatibilityInspector(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Print constraints for a table using only PostgreSQL-compatible information_schema queries
     * This will work in PostgreSQL and should work in any database that properly implements 
     * the SQL standard information_schema tables
     */
    private void printTableConstraints(String tableName) {
        System.out.println("\n- Table Constraints (Using PostgreSQL compatibility):");
        
        try {
            // Query for all constraints using information_schema
            String sql = 
                "SELECT tc.constraint_name, tc.constraint_type, kcu.column_name " +
                "FROM information_schema.table_constraints tc " +
                "JOIN information_schema.key_column_usage kcu " +
                "  ON tc.constraint_name = kcu.constraint_name " +
                "  AND tc.table_name = kcu.table_name " +
                "WHERE tc.table_name = ? " +
                "ORDER BY tc.constraint_type, tc.constraint_name, kcu.ordinal_position";
            
            List<Map<String, Object>> constraints = jdbcTemplate.queryForList(sql, tableName);
            
            if (constraints.isEmpty()) {
                System.out.println("  No constraints found in information_schema");
                return;
            }
            
            // Group constraints by type and name for better readability
            Map<String, Map<String, List<String>>> groupedConstraints = new HashMap<>();
            
            for (Map<String, Object> constraint : constraints) {
                String type = (String) constraint.get("constraint_type");
                String name = (String) constraint.get("constraint_name");
                String column = (String) constraint.get("column_name");
                
                if (!groupedConstraints.containsKey(type)) {
                    groupedConstraints.put(type, new HashMap<>());
                }
                
                if (!groupedConstraints.get(type).containsKey(name)) {
                    groupedConstraints.get(type).put(name, new ArrayList<>());
                }
                
                groupedConstraints.get(type).get(name).add(column);
            }
            
            // Print grouped constraints
            for (String type : groupedConstraints.keySet()) {
                System.out.println("  " + type + " Constraints:");
                for (Map.Entry<String, List<String>> entry : groupedConstraints.get(type).entrySet()) {
                    System.out.println("    - " + entry.getKey() + " (" + String.join(", ", entry.getValue()) + ")");
                }
            }
            
            // Special handling for UNIQUE constraints - they may be implemented as unique indexes
            // in some databases, so we need an additional check
            boolean hasUniqueConstraints = groupedConstraints.containsKey("UNIQUE");
            
            if (!hasUniqueConstraints) {
                System.out.println("\n- Checking for UNIQUE indexes that might implement UNIQUE constraints:");
                try {
                    // This query works in PostgreSQL to find unique indexes
                    String indexSql = 
                        "SELECT i.relname AS index_name, " +
                        "       array_agg(a.attname ORDER BY array_position(i.indkey, a.attnum)) AS columns, " +
                        "       i.indisunique AS is_unique " +
                        "FROM pg_index i " +
                        "JOIN pg_class t ON i.indrelid = t.oid " +
                        "JOIN pg_class idx ON i.indexrelid = idx.oid " +
                        "JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(i.indkey) " +
                        "WHERE t.relname = ? AND i.indisunique = true " +
                        "GROUP BY i.indexrelid, i.indisunique, i.indrelid, idx.relname, i.relname";
                    
                    List<Map<String, Object>> uniqueIndexes = jdbcTemplate.queryForList(indexSql, tableName);
                    
                    if (uniqueIndexes.isEmpty()) {
                        System.out.println("  No unique indexes found");
                    } else {
                        System.out.println("  Found unique indexes:");
                        for (Map<String, Object> idx : uniqueIndexes) {
                            System.out.println("    - " + idx.get("index_name") + " on columns: " + idx.get("columns"));
                        }
                    }
                } catch (Exception e) {
                    System.out.println("  Could not check for unique indexes: " + e.getMessage());
                    System.out.println("  This may indicate the database doesn't use PostgreSQL-compatible system catalogs");
                }
            }
        } catch (Exception e) {
            System.out.println("  ❌ Error retrieving constraints: " + e.getMessage());
            System.out.println("  This may indicate the database doesn't implement PostgreSQL-compatible information_schema");
        }
    }

    /**
     * Print table schema using only PostgreSQL-compatible information_schema queries
     * If a database doesn't follow the PostgreSQL information_schema structure, this will fail,
     * which helps identify compatibility issues.
     */
    public void inspectTableSchema(String tableName) {
        try {
            // Log which database we're running against
            String dbType = detectDatabaseType();
            System.out.println("\n---------- Table Schema Inspection for " + tableName + " on " + dbType + " ----------");
            
            // Get column details - standard PostgreSQL information_schema query
            printTableColumns(tableName);
            
            // Get primary key information - standard PostgreSQL information_schema query
            printPrimaryKeyInfo(tableName);

            // Get constraint information including UNIQUE constraints
            printTableConstraints(tableName);
            
            // Try to get identity column info - PostgreSQL approach
            printIdentityColumnInfo(tableName);
            
            // Reconstruct CREATE TABLE statement
            printReconstructedDDL(tableName);
            
            System.out.println("---------------------------------------------------\n");
        } catch (Exception e) {
            System.out.println("❌ COMPATIBILITY ERROR: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Detect database type by querying version information
     */
    private String detectDatabaseType() {
        try {
            String versionSql = "SELECT version()";
            String version = jdbcTemplate.queryForObject(versionSql, String.class);
            
            if (version.toLowerCase().contains("cockroach")) {
                return "CockroachDB";
            } else if (version.toLowerCase().contains("postgresql")) {
                return "PostgreSQL";
            } else if (version.toLowerCase().contains("spanner")) {
                return "Spanner";
            } else {
                return "Unknown: " + version;
            }
        } catch (Exception e) {
            return "Unknown: " + e.getMessage();
        }
    }
    
    /**
     * Print column details for a table
     */
    private void printTableColumns(String tableName) {
        String sql = 
            "SELECT column_name, data_type, is_nullable, column_default, " +
            "       character_maximum_length, numeric_precision, numeric_scale, ordinal_position " +
            "FROM information_schema.columns " +
            "WHERE table_name = ? " +
            "ORDER BY ordinal_position";
        
        System.out.println("\n- Column Information:");
        List<Map<String, Object>> columns = jdbcTemplate.queryForList(sql, tableName);
        
        if (columns.isEmpty()) {
            System.out.println("  No columns found (may indicate incompatible information_schema)");
            return;
        }
        
        for (Map<String, Object> column : columns) {
            StringBuilder sb = new StringBuilder();
            sb.append("  ").append(column.get("column_name")).append(": ");
            
            // Data type with length/precision if applicable
            String dataType = column.get("data_type").toString();
            if (column.get("character_maximum_length") != null) {
                dataType += "(" + column.get("character_maximum_length") + ")";
            } else if (column.get("numeric_precision") != null && column.get("numeric_scale") != null) {
                dataType += "(" + column.get("numeric_precision") + "," + column.get("numeric_scale") + ")";
            }
            sb.append(dataType).append(", ");
            
            // Nullable
            sb.append("Nullable: ").append(column.get("is_nullable")).append(", ");
            
            // Default value - this is where IDENTITY information would appear in PostgreSQL
            if (column.get("column_default") != null) {
                sb.append("Default: ").append(column.get("column_default"));
            } else {
                sb.append("No Default");
            }
            
            System.out.println(sb.toString());
        }
    }
    
    /**
     * Print primary key information for a table
     */
    private void printPrimaryKeyInfo(String tableName) {
        String sql = 
            "SELECT kcu.column_name, kcu.ordinal_position " +
            "FROM information_schema.table_constraints tc " +
            "JOIN information_schema.key_column_usage kcu " +
            "  ON tc.constraint_name = kcu.constraint_name " +
            "WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_name = ? " +
            "ORDER BY kcu.ordinal_position";
        
        System.out.println("\n- Primary Key Information:");
        List<Map<String, Object>> pkColumns = jdbcTemplate.queryForList(sql, tableName);
        
        if (pkColumns.isEmpty()) {
            System.out.println("  No primary key found (may indicate incompatible information_schema)");
            return;
        }
        
        System.out.print("  Primary Key: ");
        for (int i = 0; i < pkColumns.size(); i++) {
            if (i > 0) System.out.print(", ");
            System.out.print(pkColumns.get(i).get("column_name"));
        }
        System.out.println();
    }
    
    /**
     * Try to get identity column information in PostgreSQL-compatible way
     */
    private void printIdentityColumnInfo(String tableName) {
        try {
            // This query works in PostgreSQL 10+ for IDENTITY columns
            String sql = 
                "SELECT column_name, identity_generation " +
                "FROM information_schema.columns " +
                "WHERE table_name = ? AND identity_generation IS NOT NULL";
            
            System.out.println("\n- Identity Column Information:");
            List<Map<String, Object>> identityColumns = jdbcTemplate.queryForList(sql, tableName);
            
            if (identityColumns.isEmpty()) {
                System.out.println("  No identity columns found");
            } else {
                for (Map<String, Object> col : identityColumns) {
                    System.out.println("  " + col.get("column_name") + ": " + col.get("identity_generation"));
                }
            }
        } catch (Exception e) {
            System.out.println("  ❌ Error checking identity columns: " + e.getMessage());
            System.out.println("  This might indicate the database doesn't support PostgreSQL identity column metadata");
        }
    }
    
    /**
     * Reconstruct the CREATE TABLE statement from information_schema
     */
    private void printReconstructedDDL(String tableName) {
        // Get column definitions
        String columnSql = 
            "SELECT column_name, data_type, is_nullable, column_default, " +
            "       character_maximum_length, numeric_precision, numeric_scale " +
            "FROM information_schema.columns " +
            "WHERE table_name = ? " +
            "ORDER BY ordinal_position";
        
        // Get primary key information
        String pkSql = 
            "SELECT kcu.column_name " +
            "FROM information_schema.table_constraints tc " +
            "JOIN information_schema.key_column_usage kcu " +
            "  ON tc.constraint_name = kcu.constraint_name " +
            "WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_name = ? " +
            "ORDER BY kcu.ordinal_position";
        
        System.out.println("\n- Reconstructed CREATE TABLE statement:");
        
        // Build the CREATE TABLE statement
        StringBuilder createTableSQL = new StringBuilder();
        createTableSQL.append("CREATE TABLE ").append(tableName).append(" (\n");
        
        // Add columns
        List<Map<String, Object>> columns = jdbcTemplate.queryForList(columnSql, tableName);
        
        for (int i = 0; i < columns.size(); i++) {
            Map<String, Object> column = columns.get(i);
            createTableSQL.append("  ").append(column.get("column_name")).append(" ");
            
            // Data type with length/precision if applicable
            String dataType = column.get("data_type").toString();
            if (column.get("character_maximum_length") != null) {
                dataType += "(" + column.get("character_maximum_length") + ")";
            } else if (column.get("numeric_precision") != null && column.get("numeric_scale") != null) {
                dataType += "(" + column.get("numeric_precision") + "," + column.get("numeric_scale") + ")";
            }
            createTableSQL.append(dataType).append(" ");
            
            // Nullable
            if ("NO".equals(column.get("is_nullable"))) {
                createTableSQL.append("NOT NULL ");
            }
            
            // Default value
            if (column.get("column_default") != null) {
                createTableSQL.append("DEFAULT ").append(column.get("column_default")).append(" ");
            }
            
            if (i < columns.size() - 1) {
                createTableSQL.append(",\n");
            }
        }
        
        // Add primary key constraint
        List<Map<String, Object>> pkColumns = jdbcTemplate.queryForList(pkSql, tableName);
        if (!pkColumns.isEmpty()) {
            createTableSQL.append(",\n  PRIMARY KEY (");
            for (int i = 0; i < pkColumns.size(); i++) {
                if (i > 0) createTableSQL.append(", ");
                createTableSQL.append(pkColumns.get(i).get("column_name"));
            }
            createTableSQL.append(")");
        }
        
        createTableSQL.append("\n);");
        
        // Print the CREATE TABLE statement
        System.out.println(createTableSQL.toString());
    }
}