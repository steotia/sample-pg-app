package com.trials.crdb.app.repositories;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.trials.crdb.app.model.Project;
import com.trials.crdb.app.model.Ticket;
import com.trials.crdb.app.model.User;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
public class AdvancedSqlSpannerTests {

    private static final String PROJECT_ID = "emulator-project";
    private static final String INSTANCE_ID = "test-instance";
    private static final String DATABASE_ID = "test-database";
    
    // Create empty credentials file for test
    @BeforeAll
    public static void setupCredentials() throws IOException {
        Path credentialsPath = Path.of("/tmp/empty-credentials.json");
        
        // TOIL - /tmp/empty-credentials.json exists as directory from previous runs
        // Delete if it exists as directory or file
        if (Files.exists(credentialsPath)) {
            if (Files.isDirectory(credentialsPath)) {
                // WORKAROUND - Remove directory and its contents if it exists
                Files.walk(credentialsPath)
                    .sorted((a, b) -> b.compareTo(a)) // Reverse order for deletion
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            // Ignore deletion errors
                        }
                    });
            } else {
                Files.delete(credentialsPath);
            }
        }
        
        // Create the empty credentials file
        Files.writeString(credentialsPath, "{}");
    }
    // Create a shared network for containers
    private static final Network NETWORK = Network.newNetwork();

    // Spanner emulator container
    @Container
    static final GenericContainer<?> spannerEmulator = 
        new GenericContainer<>("gcr.io/cloud-spanner-emulator/emulator")
            .withNetwork(NETWORK)
            .withNetworkAliases("spanner-emulator")
            .withExposedPorts(9010, 9020)
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
            .withStartupTimeout(Duration.ofMinutes(2));

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
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "none");
        registry.add("spring.datasource.hikari.connection-init-sql", 
            () -> "SET spanner.support_drop_cascade=true");
        registry.add("spring.jpa.properties.hibernate.dialect", 
            () -> "org.hibernate.dialect.PostgreSQLDialect");
        registry.add("spring.jpa.show-sql", () -> "true");
    }

    @Autowired
    private TestEntityManager entityManager;

    @Autowired
    private TicketRepository ticketRepository;
    
    @Autowired
    private UserRepository userRepository;
    
    @Autowired
    private ProjectRepository projectRepository;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private JdbcTemplate jdbcTemplate;
    
    // Test data references
    private User user1, user2;
    private Project project1, project2;
    private Ticket ticket1, ticket2, ticket3, ticket4, ticket5;

    @BeforeEach
    void setupSchema() throws SQLException {
        // Create schema manually for Spanner
        try (Connection conn = dataSource.getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                // Enable Spanner settings
                stmt.execute("SET spanner.support_drop_cascade=true");
                
                // Drop tables if they exist
                stmt.execute("DROP TABLE IF EXISTS tickets");
                stmt.execute("DROP TABLE IF EXISTS user_projects");
                stmt.execute("DROP TABLE IF EXISTS users");
                stmt.execute("DROP TABLE IF EXISTS projects");
                
                // Create projects table
                stmt.execute("CREATE TABLE projects (" +
                    "id BIGINT GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE)," +
                    "create_time TIMESTAMPTZ NOT NULL," +
                    "description TEXT," +
                    "name VARCHAR(255) NOT NULL," +
                    "PRIMARY KEY (id)" +
                    ")");
                
                // Create users table
                stmt.execute("CREATE TABLE users (" +
                    "id BIGINT GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE)," +
                    "create_time TIMESTAMPTZ NOT NULL," +
                    "username VARCHAR(255) NOT NULL," +
                    "email VARCHAR(255) NOT NULL," +
                    "full_name VARCHAR(255)," +
                    "PRIMARY KEY (id)" +
                    ")");
                
                // Create unique indexes
                stmt.execute("CREATE UNIQUE INDEX uk_users_username ON users (username)");
                stmt.execute("CREATE UNIQUE INDEX uk_projects_name ON projects (name)");
                
                // Create join table
                stmt.execute("CREATE TABLE user_projects (" +
                    "user_id BIGINT NOT NULL," +
                    "project_id BIGINT NOT NULL," +
                    "PRIMARY KEY (user_id, project_id)" +
                    ")");
                
                // Create tickets table with all required columns
                stmt.execute("CREATE TABLE tickets (" +
                    "id BIGINT GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE)," +
                    "title VARCHAR(255) NOT NULL," +
                    "description TEXT," +
                    "status VARCHAR(20) NOT NULL," +
                    "priority VARCHAR(20) NOT NULL," +
                    "metadata JSONB," +
                    "assignee_id BIGINT," +
                    "reporter_id BIGINT NOT NULL," +
                    "project_id BIGINT NOT NULL," +
                    "create_time TIMESTAMPTZ NOT NULL," +
                    "update_time TIMESTAMPTZ," +
                    // Add missing columns from Ticket entity
                    "due_date TIMESTAMPTZ," +
                    "estimated_hours FLOAT," +
                    "resolved_date TIMESTAMPTZ," +
                    "dependent_on_id BIGINT," +
                    "tags TEXT[]," +
                    "version BIGINT NOT NULL DEFAULT 0," +
                    "PRIMARY KEY (id)" +
                    ")");
            }
        }
        
        // Set up test data after schema creation
        createTestData();
    }
    
    private void createTestData() {
        // Create users
        user1 = new User("john", "john@example.com", "John Smith");
        user2 = new User("jane", "jane@example.com", "Jane Doe");
        entityManager.persist(user1);
        entityManager.persist(user2);
        
        // Create projects
        project1 = new Project("Advanced SQL Project", "Testing advanced SQL features");
        project2 = new Project("Another Project", "Second test project");
        entityManager.persist(project1);
        entityManager.persist(project2);
        
        // Create tickets with various properties for testing
        ticket1 = new Ticket("Base Ticket", "First ticket", user1, project1);
        ticket1.setEstimatedHours(10.0);
        ticket1.setTags(new String[]{"sql", "test", "important"});
        entityManager.persist(ticket1);
        
        ticket2 = new Ticket("Dependent Ticket", "Depends on ticket1", user2, project1);
        ticket2.setEstimatedHours(5.0);
        ticket2.setTags(new String[]{"sql", "dependent"});
        ticket2.setDependentOn(ticket1);
        entityManager.persist(ticket2);
        
        ticket3 = new Ticket("Critical Task", "High priority task", user1, project1);
        ticket3.setPriority(Ticket.TicketPriority.CRITICAL);
        ticket3.setEstimatedHours(8.0);
        ticket3.setTags(new String[]{"critical", "urgent"});
        entityManager.persist(ticket3);
        
        ticket4 = new Ticket("Project 2 Task", "Task for second project", user2, project2);
        ticket4.setEstimatedHours(12.0);
        ticket4.setTags(new String[]{"project2", "test"});
        entityManager.persist(ticket4);
        
        ticket5 = new Ticket("Deep Dependency", "Depends on ticket2", user1, project1);
        ticket5.setDependentOn(ticket2);
        ticket5.setEstimatedHours(3.0);
        entityManager.persist(ticket5);
        
        entityManager.flush();
        
        // Print dependencies to verify
        System.out.println("Ticket5 depends on: " + (ticket5.getDependentOn() != null ? ticket5.getDependentOn().getId() : "null"));
        System.out.println("Ticket2 depends on: " + (ticket2.getDependentOn() != null ? ticket2.getDependentOn().getId() : "null"));
    }
    
    @Test
    public void testArithmeticAndAggregation() {
        Map<String, Object> stats = ticketRepository.calculateProjectEstimationStatistics(project1.getId());
        
        assertThat(stats).isNotNull();
        assertThat(((Number) stats.get("total")).doubleValue()).isEqualTo(26.0); // 10 + 5 + 8 + 3
        assertThat(((Number) stats.get("average")).doubleValue()).isEqualTo(6.5); // (10 + 5 + 8 + 3) / 4
        assertThat(((Number) stats.get("maximum")).doubleValue()).isEqualTo(10.0);
        assertThat(((Number) stats.get("rms")).doubleValue()).isCloseTo(14.071, Offset.offset(0.1)); // sqrt(10² + 5² + 8² + 3²)
    }
    
    @Test
    public void testBasicCTE() {
        // Basic (non-recursive) CTE should work in Spanner
        List<Object[]> projectTickets = ticketRepository.findTicketsWithCTEByProjectName("Advanced SQL Project");
        assertThat(projectTickets).hasSize(4); // 4 tickets in project1
    }
    
    // TOIL - didnt work - count mismatch
    // @Test
    // public void testArrayFunctions() {
    //     List<Object[]> sqlTaggedTickets = ticketRepository.findTicketsByTagWithArrayFunctions("sql");
        
    //     assertThat(sqlTaggedTickets).hasSize(2);
        
    //     // Handle the type difference specifically for Spanner
    //     Object tagCount1 = sqlTaggedTickets.get(0)[3];
    //     Object tagCount2 = sqlTaggedTickets.get(1)[3];
        
    //     // For Spanner, these will be Long values
    //     assertThat(((Number)tagCount1).intValue()).isEqualTo(3);
    //     assertThat(((Number)tagCount2).intValue()).isEqualTo(2);
    // }

    // TOIL - issue with order and bit reversed sequence 
    // @Test
    // public void testArrayFunctions() {
    //     // Step 1: Find tickets with "sql" tag
    //     List<Object[]> sqlTaggedTickets = ticketRepository.findTicketIdsByTag("sql");
    //     assertThat(sqlTaggedTickets).hasSize(2);
        
    //     // Step 2: Verify basic ticket properties
    //     String firstTicketTitle = (String) sqlTaggedTickets.get(0)[1];
    //     String secondTicketTitle = (String) sqlTaggedTickets.get(1)[1];
        
    //     assertThat(firstTicketTitle).isEqualTo("Base Ticket");
    //     assertThat(secondTicketTitle).isEqualTo("Dependent Ticket");
        
    //     // Step 3: Get tag counts for each ticket
    //     Long ticket1Id = (Long) sqlTaggedTickets.get(0)[0];
    //     Long ticket2Id = (Long) sqlTaggedTickets.get(1)[0];
        
    //     Integer tagCount1 = ticketRepository.getTagCountForTicket(ticket1Id);
    //     Integer tagCount2 = ticketRepository.getTagCountForTicket(ticket2Id);
        
    //     // Step 4: Verify counts
    //     assertThat(tagCount1).isEqualTo(3); // First ticket has 3 tags
    //     assertThat(tagCount2).isEqualTo(2); // Second ticket has 2 tags
    // }

    @Test
    public void testArrayFunctions() {
        // Find tickets with "sql" tag
        List<Object[]> sqlTaggedTickets = ticketRepository.findTicketIdsByTag("sql");
        assertThat(sqlTaggedTickets).hasSize(2);
        
        // Extract ticket information into a map for lookup by title
        Map<String, Long> titleToId = new HashMap<>();
        for (Object[] row : sqlTaggedTickets) {
            Long id = (Long) row[0];
            String title = (String) row[1];
            titleToId.put(title, id);
        }
        
        // Verify both tickets were found (regardless of order)
        assertThat(titleToId.keySet()).containsExactlyInAnyOrder("Base Ticket", "Dependent Ticket");
        
        // Get tag counts by looking up each ticket by title
        Long baseTicketId = titleToId.get("Base Ticket");
        Long dependentTicketId = titleToId.get("Dependent Ticket");
        
        Integer baseTicketTagCount = ticketRepository.getTagCountForTicket(baseTicketId);
        Integer dependentTicketTagCount = ticketRepository.getTagCountForTicket(dependentTicketId);
        
        // Verify tag counts for each ticket
        assertThat(baseTicketTagCount).isEqualTo(3); // Base ticket has 3 tags
        assertThat(dependentTicketTagCount).isEqualTo(2); // Dependent ticket has 2 tags
    }

    // TOIL - Window functions didnt work
    @Disabled("Feature not supported in Spanner")
    @Test
    public void testWindowFunctions() {
        // This test will fail on Spanner since window functions aren't supported
        List<Object[]> rankedTickets = ticketRepository.findTicketsWithPriorityRanking();
        
        assertThat(rankedTickets).isNotNull();
        assertThat(rankedTickets).hasSize(5);
        
        // Verify ranking - critical ticket should be first
        Object[] criticalTicket = rankedTickets.stream()
            .filter(row -> row[0].equals(ticket3.getId()))
            .findFirst()
            .orElse(null);
            
        assertThat(criticalTicket).isNotNull();
        assertThat(criticalTicket[3]).isEqualTo(1L); // Rank 1
    }
    
    // TOIL - Recursive CTE didnt work
    @Disabled("Feature not supported in Spanner")
    @Test
    public void testRecursiveCTE() {
        // This test will fail on Spanner since recursive CTEs aren't supported
        List<Object[]> dependencyChain = ticketRepository.findTicketDependencyChain(ticket5.getId());
        
        // Should have 3 levels: ticket5 -> ticket2 -> ticket1
        assertThat(dependencyChain).hasSize(3);
        
        // Sort by depth for consistent ordering
        dependencyChain.sort(Comparator.comparing(row -> ((Number)row[2]).intValue()));
        
        // Verify each level in the chain
        assertThat(dependencyChain.get(0)[0]).isEqualTo(ticket5.getId());
        assertThat(((Number)dependencyChain.get(0)[2]).intValue()).isEqualTo(1); // Depth 1
        
        assertThat(dependencyChain.get(1)[0]).isEqualTo(ticket2.getId());
        assertThat(((Number)dependencyChain.get(1)[2]).intValue()).isEqualTo(2); // Depth 2
        
        assertThat(dependencyChain.get(2)[0]).isEqualTo(ticket1.getId());
        assertThat(((Number)dependencyChain.get(2)[2]).intValue()).isEqualTo(3); // Depth 3
    }
}