package com.trials.crdb.app.repositories;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.jdbc.SqlConfig;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.trials.crdb.app.model.Project;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.BeforeEach;

import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.slf4j.LoggerFactory;

import org.springframework.jdbc.core.JdbcTemplate;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import javax.sql.DataSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Testcontainers
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
// TOIL - this is not working
// @Sql(
//     scripts = "/schema.sql",
//     config = @SqlConfig(transactionMode = SqlConfig.TransactionMode.ISOLATED)
// )
public class ProjectRepositorySpannerTests {

    private static final String PROJECT_ID = "emulator-project";
    private static final String INSTANCE_ID = "test-instance";
    private static final String DATABASE_ID = "test-database";
    
    // Create empty credentials file for test
    @BeforeAll
    public static void setupCredentials() throws IOException {
        Files.writeString(Path.of("/tmp/empty-credentials.json"), "{}");
    }
    
    // Create a shared network for containers to communicate
    private static final Network NETWORK = Network.newNetwork();

    private static final OutputFrame.OutputType[] OUTPUT_FRAME_TYPES = { 
        OutputFrame.OutputType.STDOUT, 
        OutputFrame.OutputType.STDERR 
    };


    // Spanner emulator container
    @Container
    static final GenericContainer<?> spannerEmulator = 
        new GenericContainer<>("gcr.io/cloud-spanner-emulator/emulator")
            .withNetwork(NETWORK)
            .withNetworkAliases("spanner-emulator")
            .withExposedPorts(9010, 9020)
            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("spanner-emulator")))
            .withStartupTimeout(Duration.ofMinutes(2));
    
    // PGAdapter container with matched configuration
    @Container
    static final GenericContainer<?> pgAdapter = 
        new GenericContainer<>("gcr.io/cloud-spanner-pg-adapter/pgadapter")
            .withNetwork(NETWORK)
            .dependsOn(spannerEmulator)
            .withExposedPorts(5432)
            .withFileSystemBind("/tmp/empty-credentials.json", "/credentials.json", BindMode.READ_ONLY)
            .withCommand(
                "-p", PROJECT_ID,
                "-i", INSTANCE_ID,
                "-d", DATABASE_ID,
                "-e", "spanner-emulator:9010",
                "-c", "/credentials.json",
                "-r", "autoConfigEmulator=true",
                "-x"
            )
            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("pgadapter")))
            .withStartupTimeout(Duration.ofMinutes(2));

    // Use DynamicPropertySource instead of ApplicationContextInitializer
    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
        registry.add("spring.datasource.url", () -> {
            String pgHost = pgAdapter.getHost();
            int pgPort = pgAdapter.getMappedPort(5432);
            return String.format("jdbc:postgresql://%s:%d/%s", pgHost, pgPort, DATABASE_ID);
        });
        registry.add("spring.datasource.username", () -> "postgres");
        registry.add("spring.datasource.password", () -> "");
        // TOIL
        // registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "none");
        registry.add("spring.datasource.hikari.connection-init-sql", 
            () -> "SET spanner.support_drop_cascade=true");
        registry.add("spring.jpa.properties.hibernate.dialect", 
            () -> "org.hibernate.dialect.PostgreSQLDialect");
        registry.add("spring.jpa.show-sql", () -> "true");
        registry.add("logging.level.pgadapter", () -> "DEBUG");
        registry.add("logging.level.spanner-emulator", () -> "DEBUG");
    }

    @Autowired
    private TestEntityManager entityManager;

    @Autowired
    private ProjectRepository projectRepository;

    // TOIL 
    // Below doesn't work as it gets wrapped in transactions and DDL fails
    // ALTENATIVE - use @SQL annotation
    // @Autowired
    // private JdbcTemplate jdbcTemplate;

    // // TOIL: constaint is not supported
    // // Need to drop down to running SQL
    // @BeforeEach
    // public void setupSchema() {
    //     // Drop table if it exists
    //     jdbcTemplate.execute("DROP TABLE IF EXISTS projects");
        
    //     // Create the table with Spanner-compatible SQL
    //     jdbcTemplate.execute("CREATE TABLE projects (" +
    //         "id BIGINT NOT NULL GENERATED BY DEFAULT AS IDENTITY," +
    //         "create_time TIMESTAMPTZ NOT NULL," +
    //         "description TEXT," +
    //         "name VARCHAR(255) NOT NULL," +
    //         "PRIMARY KEY (id)" +
    //         ")");
        
    //     // Add unique constraint separately
    //     jdbcTemplate.execute("ALTER TABLE projects ADD CONSTRAINT uk_projects_name UNIQUE (name)");
    // }

    @Autowired
    private DataSource dataSource;

    @BeforeEach
    void setupSchema() throws SQLException {
        // Using raw JDBC connection to avoid Spring's transaction management
        try (Connection conn = dataSource.getConnection()) {
            // Disable auto-commit to control our own transactions
            // TOIL
            //conn.setAutoCommit(false);
            
            // Execute DDL statements one by one
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SET spanner.support_drop_cascade=true");
                // Commit this change
                // TOIL - since we re-enabled auto-commit
                // conn.commit();
                
                stmt.execute("DROP TABLE IF EXISTS projects");
                // Commit this change
                // conn.commit();
                
                stmt.execute("CREATE TABLE projects (" +
                    // TOIL 
                    // "id BIGINT NOT NULL GENERATED BY DEFAULT AS IDENTITY," +
                    "id BIGINT GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE)," +
                    "create_time TIMESTAMPTZ NOT NULL," +
                    "description TEXT," +
                    "name VARCHAR(255) NOT NULL," +
                    "PRIMARY KEY (id)" +
                    ")");
                // Commit this change
                // conn.commit();
                
                // stmt.execute("ALTER TABLE projects ADD CONSTRAINT uk_projects_name UNIQUE (name)");
                // Commit this change
                // conn.commit();
            }
        }
    }

    @Autowired
    private JdbcTemplate jdbcTemplate;

    // TOIL - doesn't work - SHOW CREATE TABLE
    // private void printTableDDL(String tableName) {
    //     String sql = "SHOW CREATE TABLE " + tableName;
    //     String ddl = jdbcTemplate.queryForObject(sql, String.class);
    //     System.out.println("\n---------- Table DDL for " + tableName + " ----------");
    //     System.out.println(ddl);
    //     System.out.println("---------------------------------------------------\n");
    // }
    // ALTERNATIVE 
    // Add these methods for Spanner schema inspection
    /**
     * Print the table schema information using INFORMATION_SCHEMA queries
     */
    private void printSpannerTableSchema(String tableName) {
        String sql = "SELECT " +
                    "  COLUMN_NAME, " +
                    "  SPANNER_TYPE, " +
                    "  IS_NULLABLE, " +
                    "  COLUMN_DEFAULT " +
                    "FROM INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE TABLE_NAME = ? " +
                    "ORDER BY ORDINAL_POSITION";
                    
        System.out.println("\n---------- Table Schema for " + tableName + " ----------");
        List<Map<String, Object>> columns = jdbcTemplate.queryForList(sql, tableName);
        for (Map<String, Object> column : columns) {
            System.out.println("Column: " + column.get("COLUMN_NAME") + 
                            ", Type: " + column.get("SPANNER_TYPE") + 
                            ", Nullable: " + column.get("IS_NULLABLE") +
                            ", Default: " + column.get("COLUMN_DEFAULT"));
        }
        
        // Get primary key information
        String pkSql = "SELECT " +
                    "  COLUMN_NAME " +
                    "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
                    "WHERE TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY_KEY' " +
                    "ORDER BY ORDINAL_POSITION";
        
        // Fixed: Extract column names from the result map
        List<Map<String, Object>> pkColumns = jdbcTemplate.queryForList(pkSql, tableName);
        List<String> primaryKeys = new ArrayList<>();
        for (Map<String, Object> row : pkColumns) {
            primaryKeys.add(row.get("COLUMN_NAME").toString());
        }
        
        System.out.println("Primary Key Columns: " + String.join(", ", primaryKeys));
        System.out.println("---------------------------------------------------\n");
    }

    // Add a new test method
    @Test
    public void inspectProjectTableSchema() {
        // Force schema creation by performing an operation
        Project project = new Project("Schema Test", "Project for schema inspection");
        projectRepository.save(project);
        
        // Print table schema details
        printSpannerTableSchema("projects");
    }

    @Test
    public void whenSaveProject_withValidName_thenProjectIsPersistedWithGeneratedIntegerId() {
        // The test remains the same
        String projectName = "A Spanner project";
        Project project = new Project(projectName, "A very critical project with Spanner");
        Project savedProject = projectRepository.save(project);

        assertThat(savedProject).isNotNull();
        assertThat(savedProject.getId()).isNotNull();
        assertThat(savedProject.getId()).isGreaterThan(0L);
        assertThat(savedProject.getName()).isEqualTo(projectName);
        assertThat(savedProject.getCreateTime()).isNotNull();

        Project foundInDb = entityManager.find(Project.class, savedProject.getId());
        assertThat(foundInDb).isNotNull();
        assertThat(foundInDb.getName()).isEqualTo(projectName);

        // printTableDDL("projects");
        printSpannerTableSchema("projects");
    }
}