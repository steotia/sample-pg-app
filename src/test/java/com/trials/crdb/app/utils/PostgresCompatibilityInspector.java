package com.trials.crdb.app.utils;

import org.springframework.jdbc.core.JdbcTemplate;
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