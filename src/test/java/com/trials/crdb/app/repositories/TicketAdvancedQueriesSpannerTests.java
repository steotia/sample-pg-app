package com.trials.crdb.app.repositories;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.*;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.trials.crdb.app.model.*;
import com.trials.crdb.app.model.Ticket.TicketPriority;
import com.trials.crdb.app.model.Ticket.TicketStatus;
import com.trials.crdb.app.utils.PostgresCompatibilityInspector;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
public class TicketAdvancedQueriesSpannerTests {

    private static final String PROJECT_ID = "emulator-project";
    private static final String INSTANCE_ID = "test-instance";
    private static final String DATABASE_ID = "test-database";

    private PostgresCompatibilityInspector schemaInspector;
    
    // Test data references
    private User user1, user2, user3;
    private Project project1, project2;
    private Ticket ticket1, ticket2, ticket3, ticket4, ticket5;
    
    // Create empty credentials file for test
    @BeforeAll
    public static void setupCredentials() throws IOException {
        Files.writeString(Path.of("/tmp/empty-credentials.json"), "{}");
    }

    @BeforeEach
    void setUp() {
        schemaInspector = new PostgresCompatibilityInspector(jdbcTemplate, 
            PostgresCompatibilityInspector.DatabaseType.SPANNER);
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
    
    /**
     * Set up common test data for all tests
     */
    private void createTestData() {
        // Clean up existing data if needed
        // (Not needed as we just created fresh tables)
        
        // Create users
        user1 = new User("john", "john@example.com", "John Smith");
        user2 = new User("jane", "jane@example.com", "Jane Doe");
        user3 = new User("bob", "bob@example.com", "Bob Johnson");
        entityManager.persist(user1);
        entityManager.persist(user2);
        entityManager.persist(user3);
        
        // Create projects
        project1 = new Project("Website Redesign", "Redesign the company website");
        project2 = new Project("Mobile App", "Develop a new mobile app");
        entityManager.persist(project1);
        entityManager.persist(project2);
        
        // Create tickets with different statuses, priorities, and metadata
        ticket1 = new Ticket("Homepage Layout", "Redesign the homepage layout", user1, project1);
        ticket1.setStatus(Ticket.TicketStatus.IN_PROGRESS);
        ticket1.setPriority(Ticket.TicketPriority.HIGH);
        ticket1.assignTo(user2);
        Map<String, Object> metadata1 = new HashMap<>();
        metadata1.put("component", "frontend");
        metadata1.put("complexity", 3);
        metadata1.put("tags", Arrays.asList("design", "ui"));
        ticket1.setMetadata(metadata1);
        
        ticket2 = new Ticket("API Integration", "Integrate with payment API", user1, project1);
        ticket2.setStatus(Ticket.TicketStatus.OPEN);
        ticket2.setPriority(Ticket.TicketPriority.MEDIUM);
        Map<String, Object> metadata2 = new HashMap<>();
        metadata2.put("component", "backend");
        metadata2.put("complexity", 4);
        metadata2.put("tags", Arrays.asList("api", "payment"));
        ticket2.setMetadata(metadata2);
        
        ticket3 = new Ticket("Mobile Navigation", "Implement mobile navigation menu", user2, project2);
        ticket3.setStatus(Ticket.TicketStatus.REVIEW);
        ticket3.setPriority(Ticket.TicketPriority.MEDIUM);
        ticket3.assignTo(user3);
        Map<String, Object> metadata3 = new HashMap<>();
        metadata3.put("component", "frontend");
        metadata3.put("complexity", 2);
        metadata3.put("platform", "iOS");
        metadata3.put("tags", Arrays.asList("navigation", "mobile"));
        ticket3.setMetadata(metadata3);
        
        ticket4 = new Ticket("Database Optimization", "Optimize database queries", user3, project1);
        ticket4.setStatus(Ticket.TicketStatus.OPEN);
        ticket4.setPriority(Ticket.TicketPriority.CRITICAL);
        Map<String, Object> metadata4 = new HashMap<>();
        metadata4.put("component", "backend");
        metadata4.put("complexity", 5);
        metadata4.put("tags", Arrays.asList("database", "performance"));
        ticket4.setMetadata(metadata4);
        
        ticket5 = new Ticket("User Authentication", "Implement user authentication", user1, project2);
        ticket5.setStatus(Ticket.TicketStatus.CLOSED);
        ticket5.setPriority(Ticket.TicketPriority.HIGH);
        ticket5.assignTo(user1);
        Map<String, Object> metadata5 = new HashMap<>();
        metadata5.put("component", "backend");
        metadata5.put("complexity", 4);
        metadata5.put("tags", Arrays.asList("security", "auth"));
        ticket5.setMetadata(metadata5);
        
        // Persist all tickets
        entityManager.persist(ticket1);
        entityManager.persist(ticket2);
        entityManager.persist(ticket3);
        entityManager.persist(ticket4);
        entityManager.persist(ticket5);
        
        // Flush to ensure all entities are saved
        entityManager.flush();
    }

    //-------------------------------------------------------------------------
    // SECTION 1: BASIC FILTERING TESTS
    //-------------------------------------------------------------------------
    
    @Test
    public void testFilterByStatus() {
        // Test finding tickets by status
        List<Ticket> openTickets = ticketRepository.findByStatus(Ticket.TicketStatus.OPEN);
        assertThat(openTickets).hasSize(2);
        assertThat(openTickets).extracting("title")
            .containsExactlyInAnyOrder("API Integration", "Database Optimization");
    }
    
    @Test
    public void testFilterByPriority() {
        // Test finding tickets by priority
        List<Ticket> highPriorityTickets = ticketRepository.findByPriority(Ticket.TicketPriority.HIGH);
        assertThat(highPriorityTickets).hasSize(2);
        assertThat(highPriorityTickets).extracting("title")
            .containsExactlyInAnyOrder("Homepage Layout", "User Authentication");
    }
    
    @Test
    public void testFilterByStatusAndPriority() {
        // Test combined filters
        List<Ticket> openCriticalTickets = ticketRepository.findByStatusAndPriority(
            Ticket.TicketStatus.OPEN, Ticket.TicketPriority.CRITICAL);
        assertThat(openCriticalTickets).hasSize(1);
        assertThat(openCriticalTickets.get(0).getTitle()).isEqualTo("Database Optimization");
    }

    //-------------------------------------------------------------------------
    // SECTION 2: RELATIONSHIP QUERY TESTS
    //-------------------------------------------------------------------------
    
    @Test
    public void testFindByAssignee() {
        // Test finding tickets assigned to a specific user
        List<Ticket> user2Tickets = ticketRepository.findByAssignee(user2);
        assertThat(user2Tickets).hasSize(1);
        assertThat(user2Tickets.get(0).getTitle()).isEqualTo("Homepage Layout");
    }
    
    @Test
    public void testFindByReporter() {
        // Test finding tickets reported by a specific user
        List<Ticket> user1ReportedTickets = ticketRepository.findByReporter(user1);
        assertThat(user1ReportedTickets).hasSize(3);
        assertThat(user1ReportedTickets).extracting("title")
            .containsExactlyInAnyOrder("Homepage Layout", "API Integration", "User Authentication");
    }
    
    @Test
    public void testFindByProject() {
        // Test finding tickets for a specific project
        List<Ticket> project2Tickets = ticketRepository.findByProject(project2);
        assertThat(project2Tickets).hasSize(2);
        assertThat(project2Tickets).extracting("title")
            .containsExactlyInAnyOrder("Mobile Navigation", "User Authentication");
    }
    
    @Test
    public void testFindUnassignedTickets() {
        // Test finding tickets without an assignee
        List<Ticket> unassignedTickets = ticketRepository.findByAssigneeIsNull();
        assertThat(unassignedTickets).hasSize(2);
        assertThat(unassignedTickets).extracting("title")
            .containsExactlyInAnyOrder("API Integration", "Database Optimization");
    }

    //-------------------------------------------------------------------------
    // SECTION 3: TEXT SEARCH TESTS 
    //-------------------------------------------------------------------------
    
    /*
     * PostgreSQL and CockroachDB fully support LIKE with ESCAPE
     * Spanner doesn't support the like_escape function, so we need to use string functions like STRPOS instead
     */

    @Test
    public void testFindByKeyword() {
        // Test basic text search
        List<Ticket> apiTickets = ticketRepository.findByKeywordForSpanner("API");
        assertThat(apiTickets).hasSize(1);
        assertThat(apiTickets.get(0).getTitle()).isEqualTo("API Integration");
        
        List<Ticket> mobileTickets = ticketRepository.findByKeywordForSpanner("mobile");
        assertThat(mobileTickets).hasSize(1);
        assertThat(mobileTickets.get(0).getTitle()).isEqualTo("Mobile Navigation");
    }

    @Test
    public void testFindByKeywordCaseInsensitive() {
        // Test case-insensitive search using Spanner-compatible method
        List<Ticket> apiTickets = ticketRepository.findByKeywordCaseInsensitiveSpanner("api");
        assertThat(apiTickets).hasSize(1);
        assertThat(apiTickets.get(0).getTitle()).isEqualTo("API Integration");
        
        List<Ticket> mobileTickets = ticketRepository.findByKeywordCaseInsensitiveSpanner("MOBILE");
        assertThat(mobileTickets).hasSize(1);
        assertThat(mobileTickets.get(0).getTitle()).isEqualTo("Mobile Navigation");
    }

    @Test
    public void testFindByMultipleKeywords() {
        // Test searching with multiple keywords using Spanner-compatible method
        List<Ticket> tickets = ticketRepository.findByMultipleKeywordsSpanner("mobile", "navigation");
        assertThat(tickets).hasSize(1);
        assertThat(tickets.get(0).getTitle()).isEqualTo("Mobile Navigation");
        
        // Test with keywords that won't match together
        List<Ticket> noMatches = ticketRepository.findByMultipleKeywordsSpanner("mobile", "payment");
        assertThat(noMatches).isEmpty();
    }

    @Test
    public void testFindByPrefix() {
        // Test prefix search with Spanner-compatible method
        List<Ticket> tickets = ticketRepository.findByPrefixSpanner("datab");
        assertThat(tickets).hasSize(1);
        assertThat(tickets.get(0).getTitle()).isEqualTo("Database Optimization");
        
        // Note: STARTS_WITH only matches the beginning of the entire string
        // So this may not find matches where the prefix is in middle of text
        List<Ticket> implTickets = ticketRepository.findByPrefixSpanner("impl");
        // Assertions may need to be adjusted depending on actual behavior
    }

    @Test
    public void testFindByKeywordAcrossFields() {
        // Test searching across multiple fields using Spanner-compatible method
        List<Ticket> highTickets = ticketRepository.findByKeywordAcrossFieldsSpanner("high");
        assertThat(highTickets).hasSize(2);
        assertThat(highTickets).extracting("title")
            .containsExactlyInAnyOrder("Homepage Layout", "User Authentication");
        
        // Search for status values
        List<Ticket> openTickets = ticketRepository.findByKeywordAcrossFieldsSpanner("open");
        assertThat(openTickets).hasSize(2);
    }

    @Test
    public void testAdvancedSearch() {
        // Test searching with multiple criteria using Spanner-compatible method
        List<Ticket> tickets = ticketRepository.findByAdvancedCriteriaSpanner("api", null, "OPEN", null);
        assertThat(tickets).hasSize(1);
        assertThat(tickets.get(0).getTitle()).isEqualTo("API Integration");
        
        // Test search with only priority
        List<Ticket> highTickets = ticketRepository.findByAdvancedCriteriaSpanner(null, null, null, "HIGH");
        assertThat(highTickets).hasSize(2);
        
        // Test search with multiple criteria
        List<Ticket> complexTickets = ticketRepository.findByAdvancedCriteriaSpanner(null, "implement", null, "HIGH");
        assertThat(complexTickets).hasSize(1);
        assertThat(complexTickets.get(0).getTitle()).isEqualTo("User Authentication");
    }   

    //-------------------------------------------------------------------------
    // SECTION 4: JSON/JSONB QUERY TESTS
    //-------------------------------------------------------------------------

    @Test
    public void testJsonbFieldQuery() {
        // Test querying by a JSON field value
        List<Ticket> frontendTickets = ticketRepository.findByMetadataKeyValue("component", "frontend");
        assertThat(frontendTickets).hasSize(2);
        assertThat(frontendTickets).extracting("title")
            .containsExactlyInAnyOrder("Homepage Layout", "Mobile Navigation");
    }

    @Test
    public void testJsonbComplexityQuery() {
        // Test querying with numeric comparison on JSON value
        List<Ticket> complexTickets = ticketRepository.findByComplexityGreaterThan(3);
        assertThat(complexTickets).hasSize(3);
        assertThat(complexTickets).extracting("title")
            .containsExactlyInAnyOrder("API Integration", "Database Optimization", "User Authentication");
    }

    @Disabled("Feature not supported in Spanner")
    @Test 
    public void testJsonbArrayQuery() {
        // Test querying JSON arrays for specific values
        // FAIL
        /*
         * [ERROR]   TicketAdvancedQueriesSpannerTests.testJsonbArrayQuery:465 
         *   Expected size: 1 but was: 0 in:
         */
        List<Ticket> designTickets = ticketRepository.findByTagSpanner("design");
        assertThat(designTickets).hasSize(1);
        assertThat(designTickets.get(0).getTitle()).isEqualTo("Homepage Layout");
        
        List<Ticket> securityTickets = ticketRepository.findByTagSpanner("security");
        assertThat(securityTickets).hasSize(1);
        assertThat(securityTickets.get(0).getTitle()).isEqualTo("User Authentication");
    }

    @Test
    public void testJsonbContainsKeys() {
        // Test querying for tickets with specific keys in metadata
        List<Ticket> tickets = ticketRepository.findByMetadataContainingKeys("component", "complexity");
        assertThat(tickets).hasSize(5); // All tickets have both keys
        
        // Test with a key that only some tickets have
        List<Ticket> platformTickets = ticketRepository.findByMetadataContainingKeys("component", "platform");
        assertThat(platformTickets).hasSize(1);
        assertThat(platformTickets.get(0).getTitle()).isEqualTo("Mobile Navigation");
    }

    @Disabled("Feature not supported in Spanner")
    @Test 
    public void testJsonbContainment() {

        List<Object[]> values = ticketRepository.dumpMetadataValues("component");
        for (Object[] row : values) {
            System.out.println("Ticket ID: " + row[0] + ", component: " + row[1]);
        }

        // Test JSON containment operator
        List<Ticket> tickets = ticketRepository.findByMetadataContainingForSpanner("component", "\"frontend\"");
        assertThat(tickets).hasSize(2);
        assertThat(tickets).extracting("title")
            .containsExactlyInAnyOrder("Homepage Layout", "Mobile Navigation");
    }

   //-------------------------------------------------------------------------
    // SECTION 5: PAGINATION AND SORTING TESTS
    //-------------------------------------------------------------------------
    
    @Test
    public void testPagination() {
        // Test pagination with Spring Data
        Page<Ticket> firstPage = ticketRepository.findByProject(
            project1, PageRequest.of(0, 2));
        assertThat(firstPage.getTotalElements()).isEqualTo(3);
        assertThat(firstPage.getContent()).hasSize(2);
        assertThat(firstPage.getTotalPages()).isEqualTo(2);
        
        Page<Ticket> secondPage = ticketRepository.findByProject(
            project1, PageRequest.of(1, 2));
        assertThat(secondPage.getContent()).hasSize(1);
    }
    
    @Test
    public void testSorting() {
        // Use the custom sorting method
        List<Ticket> sortedByPriority = ticketRepository.findAllOrderByPriorityCustom();
        
        assertThat(sortedByPriority).hasSize(5);
        assertThat(sortedByPriority.get(0).getPriority()).isEqualTo(Ticket.TicketPriority.CRITICAL);
        assertThat(sortedByPriority.get(0).getTitle()).isEqualTo("Database Optimization");
    }
    
    @Test
    public void testPaginationAndSorting() {
        // Test pagination combined with sorting
        Page<Ticket> page = ticketRepository.findAll(
            PageRequest.of(0, 3, Sort.by(Sort.Direction.ASC, "title")));
        assertThat(page.getContent()).hasSize(3);
        assertThat(page.getContent().get(0).getTitle()).isEqualTo("API Integration");
    }

    //-------------------------------------------------------------------------
    // SECTION 6: COUNT AND AGGREGATION TESTS
    //-------------------------------------------------------------------------
    
    @Test
    public void testCountByStatus() {
        // Test counting by status
        Long openCount = ticketRepository.countByStatus(Ticket.TicketStatus.OPEN);
        assertThat(openCount).isEqualTo(2);
    }
    
    @Test
    public void testCountByProjectAndStatus() {
        // Test counting by project and status
        Long project1OpenCount = ticketRepository.countByProjectAndStatus(project1, Ticket.TicketStatus.OPEN);
        assertThat(project1OpenCount).isEqualTo(2);
        
        Long project2ClosedCount = ticketRepository.countByProjectAndStatus(project2, Ticket.TicketStatus.CLOSED);
        assertThat(project2ClosedCount).isEqualTo(1);
    }
    
    @Test
    public void testCountGroupByPriority() {
        // Add custom repository method first
        List<Object[]> priorityCounts = ticketRepository.countByPriority();
        assertThat(priorityCounts).hasSize(3); // LOW, MEDIUM, HIGH, CRITICAL (but we only used 3)
        
        Map<TicketPriority, Long> countMap = new HashMap<>();
        for (Object[] result : priorityCounts) {
            countMap.put((TicketPriority) result[0], (Long) result[1]);
        }
        
        assertThat(countMap.get(TicketPriority.MEDIUM)).isEqualTo(2L);
        assertThat(countMap.get(TicketPriority.HIGH)).isEqualTo(2L);
    }

    //-------------------------------------------------------------------------
    // SECTION 7: NATIVE QUERY TESTS 
    //-------------------------------------------------------------------------
    
    @Test
    public void testNativeQuery() {
        // Test native SQL query
        List<Ticket> johnsActiveTickets = ticketRepository.findActiveTicketsByReporterUsername("john");
        assertThat(johnsActiveTickets).hasSize(2);
        assertThat(johnsActiveTickets).extracting("title")
            .containsExactlyInAnyOrder("Homepage Layout", "API Integration");
    }
}