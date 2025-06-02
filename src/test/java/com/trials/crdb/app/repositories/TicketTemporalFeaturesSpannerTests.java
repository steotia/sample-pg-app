package com.trials.crdb.app.repositories;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
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

import com.trials.crdb.app.model.*;
import com.trials.crdb.app.test.TimeBasedTest;
import com.trials.crdb.app.utils.DateTimeProvider;
import com.trials.crdb.app.utils.PostgresCompatibilityInspector;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
public class TicketTemporalFeaturesSpannerTests extends TimeBasedTest {

    private static final String PROJECT_ID = "emulator-project";
    private static final String INSTANCE_ID = "test-instance";
    private static final String DATABASE_ID = "test-database";

    private PostgresCompatibilityInspector schemaInspector;
    
    // Test data references
    private User user1;
    private User user2;
    private Project project1;
    private Project project2;
    
    // Create empty credentials file for test
    @BeforeAll
    public static void setupCredentials() throws IOException {
        Files.writeString(Path.of("/tmp/empty-credentials.json"), "{}");
    }

    @BeforeEach
    void setUp() {
        super.setupTime(); // Set up our base time
        
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
                
                // TOIL - Missing columns in original CREATE TABLE statement caused insert failures
                // WORKAROUND - Add all columns that exist in Ticket entity
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
     * Create basic test data for users and projects
     */
    private void createTestData() {
        // Create users
        user1 = new User("john", "john@example.com", "John Smith");
        user2 = new User("jane", "jane@example.com", "Jane Doe");
        entityManager.persist(user1);
        entityManager.persist(user2);
        
        // Create projects
        project1 = new Project("Website Redesign", "Redesign the company website");
        project2 = new Project("Mobile App", "Develop a new mobile app");
        entityManager.persist(project1);
        entityManager.persist(project2);
        
        entityManager.flush();
    }

    @Test
    public void testOverdueTickets() {
        // Create a ticket that will become overdue
        Ticket overdueTicket = new Ticket("Overdue Ticket", "Past due date", user1, project1);
        overdueTicket.setDueDate(baseTime.plusDays(1)); // Set future date initially to satisfy constraint
        entityManager.persist(overdueTicket);
        entityManager.flush();
        
        // Update due_date via JDBC to make ticket overdue (due_date < current time)
        jdbcTemplate.update(
            "UPDATE tickets SET due_date = ? WHERE id = ?",
            java.sql.Timestamp.from(baseTime.minusDays(1).toInstant()), // Due date in the past
            overdueTicket.getId()
        );
        
        // Create a ticket due tomorrow (not overdue)
        Ticket upcomingTicket = new Ticket("Future Ticket", "Not yet due", user2, project2);
        upcomingTicket.setDueDate(baseTime.plusDays(1));
        entityManager.persist(upcomingTicket);
        
        // Create a resolved ticket that was due yesterday (not overdue because resolved)
        Ticket resolvedTicket = new Ticket("Resolved Ticket", "Resolved but was overdue", user2, project1);
        resolvedTicket.setDueDate(baseTime.plusDays(1)); // Set future initially
        resolvedTicket.setStatus(Ticket.TicketStatus.RESOLVED);
        entityManager.persist(resolvedTicket);
        entityManager.flush();
        
        // Update due_date for resolved ticket to past as well
        jdbcTemplate.update(
            "UPDATE tickets SET due_date = ? WHERE id = ?",
            java.sql.Timestamp.from(baseTime.minusDays(1).toInstant()),
            resolvedTicket.getId()
        );
        
        // Clear entity manager to ensure fresh data from database
        entityManager.clear();
        
        // Test the repository method with our baseTime
        List<Ticket> overdueTickets = ticketRepository.findOverdueTickets(baseTime);
        
        assertThat(overdueTickets).hasSize(1);
        assertThat(overdueTickets.get(0).getTitle()).isEqualTo("Overdue Ticket");
        
        // Test the isOverdue helper method - reload entities first
        Ticket reloadedOverdue = ticketRepository.findById(overdueTicket.getId()).orElseThrow();
        Ticket reloadedUpcoming = ticketRepository.findById(upcomingTicket.getId()).orElseThrow();
        Ticket reloadedResolved = ticketRepository.findById(resolvedTicket.getId()).orElseThrow();
        
        assertThat(reloadedOverdue.isOverdue()).isTrue();
        assertThat(reloadedUpcoming.isOverdue()).isFalse();
        assertThat(reloadedResolved.isOverdue()).isFalse();
    }

    @Test
    public void testTicketsDueInRange() {
        // Create tickets with different due dates - all must be > baseTime
        Ticket dueTomorrow = new Ticket("Due Tomorrow", "Due soon", user1, project1);
        dueTomorrow.setDueDate(baseTime.plusDays(1));
        entityManager.persist(dueTomorrow);
        
        Ticket dueNextWeek = new Ticket("Due Next Week", "Due later", user1, project1);
        dueNextWeek.setDueDate(baseTime.plusDays(7));
        entityManager.persist(dueNextWeek);
        
        Ticket dueNextMonth = new Ticket("Due Next Month", "Due much later", user2, project2);
        dueNextMonth.setDueDate(baseTime.plusDays(30));
        entityManager.persist(dueNextMonth);
        
        entityManager.flush();
        
        // Test finding tickets due in the next 3 days (from baseTime to baseTime+3)
        List<Ticket> ticketsDueSoon = ticketRepository.findTicketsDueInRange(
            baseTime, baseTime.plusDays(3));
            
        assertThat(ticketsDueSoon).hasSize(1);
        assertThat(ticketsDueSoon.get(0).getTitle()).isEqualTo("Due Tomorrow");
        
        // Test finding tickets due in the next 10 days
        List<Ticket> ticketsDueThisWeek = ticketRepository.findTicketsDueInRange(
            baseTime, baseTime.plusDays(10));
        assertThat(ticketsDueThisWeek).hasSize(2);
        assertThat(ticketsDueThisWeek).extracting("title")
            .containsExactlyInAnyOrder("Due Tomorrow", "Due Next Week");
    }

    @Test
    public void testResolveTicket() {
        // Create a ticket
        Ticket ticket = new Ticket("Resolution Test", "Testing resolution process", user1, project1);
        entityManager.persist(ticket);
        entityManager.flush();
        
        // Capture current state
        assertThat(ticket.getStatus()).isEqualTo(Ticket.TicketStatus.OPEN);
        assertThat(ticket.getResolvedDate()).isNull();
        
        // Resolve the ticket
        ticket.resolve();
        entityManager.persist(ticket);
        entityManager.flush();
        
        // Verify status and resolved date
        assertThat(ticket.getStatus()).isEqualTo(Ticket.TicketStatus.RESOLVED);
        assertThat(ticket.getResolvedDate()).isNotNull();
        
        // The resolved date should match our DateTimeProvider's current time
        assertThat(ticket.getResolvedDate()).isEqualTo(baseTime);
    }

    @Test
    public void testResolutionTimeCalculation() {
        // Create tickets with known resolution times using JDBC for precise control
        jdbcTemplate.execute("DELETE FROM tickets");
        
        // Insert a ticket that was created 5 days ago and resolved today
        ZonedDateTime createTime = baseTime.minusDays(5);
        ZonedDateTime resolveTime = baseTime;
        
        jdbcTemplate.update(
            "INSERT INTO tickets (title, description, status, priority, reporter_id, project_id, create_time, resolved_date, version) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            "Resolution Test", "Testing resolution time", "RESOLVED", "MEDIUM", user1.getId(), project1.getId(),
            java.sql.Timestamp.from(createTime.toInstant()),
            java.sql.Timestamp.from(resolveTime.toInstant()),
            0L // WORKAROUND - Explicitly set version to avoid NULL constraint violation
        );
        
        // Insert another ticket resolved after 1 day
        ZonedDateTime quickCreateTime = baseTime.minusDays(1);
        jdbcTemplate.update(
            "INSERT INTO tickets (title, description, status, priority, reporter_id, project_id, create_time, resolved_date, version) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            "Quick Fix", "Resolved quickly", "RESOLVED", "MEDIUM", user2.getId(), project2.getId(),
            java.sql.Timestamp.from(quickCreateTime.toInstant()),
            java.sql.Timestamp.from(resolveTime.toInstant()),
            0L // WORKAROUND - Explicitly set version
        );
        
        // Get the entities from the database
        Ticket ticket = ticketRepository.findAll().stream()
            .filter(t -> "Resolution Test".equals(t.getTitle()))
            .findFirst().orElseThrow();
            
        Ticket quickTicket = ticketRepository.findAll().stream()
            .filter(t -> "Quick Fix".equals(t.getTitle()))
            .findFirst().orElseThrow();
        
        // Test resolution times
        Duration resolution = ticket.getResolutionTime();
        assertThat(resolution).isNotNull();
        assertThat(resolution.toDays()).isEqualTo(5);
        
        Duration quickResolution = quickTicket.getResolutionTime();
        assertThat(quickResolution).isNotNull();
        assertThat(quickResolution.toDays()).isEqualTo(1);
        
        // Test average resolution time calculation
        Double avgTimeSeconds = ticketRepository.calculateAverageResolutionTimeInSeconds();
        assertThat(avgTimeSeconds).isNotNull();
        
        // Convert to days for easier comparison
        Double avgTimeDays = avgTimeSeconds / (60 * 60 * 24);
        
        // Average of 5 days and 1 day should be 3 days
        assertThat(avgTimeDays).isCloseTo(3.0, org.assertj.core.data.Offset.offset(0.2));
        
        // Test the native PostgreSQL interval approach (CockroachDB should support this too)
        String intervalResult = ticketRepository.calculateAverageResolutionTimeInterval();
        assertThat(intervalResult).isNotNull();
        System.out.println("Average resolution time (interval): " + intervalResult);
    }

    @Test
    public void testFilterByDateRange() {
        // Clean the tickets table
        jdbcTemplate.execute("DELETE FROM tickets");
        
        // Create time references
        ZonedDateTime now = baseTime;
        ZonedDateTime fiveDaysAgo = now.minusDays(5);
        ZonedDateTime tenDaysAgo = now.minusDays(10);
        
        // Insert test data with specific timestamps using JDBC
        jdbcTemplate.update(
            "INSERT INTO tickets (title, description, status, priority, reporter_id, project_id, create_time, version) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            "Old Ticket", 
            "Created 10 days ago", 
            "OPEN", 
            "MEDIUM", 
            user1.getId(),
            project1.getId(),
            java.sql.Timestamp.from(tenDaysAgo.toInstant()),
            0L // WORKAROUND - Explicitly set version
        );
        
        jdbcTemplate.update(
            "INSERT INTO tickets (title, description, status, priority, reporter_id, project_id, create_time, version) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            "Recent Ticket", 
            "Created 5 days ago", 
            "OPEN", 
            "MEDIUM", 
            user1.getId(),
            project1.getId(),
            java.sql.Timestamp.from(fiveDaysAgo.toInstant()),
            0L // WORKAROUND - Explicitly set version
        );
        
        // Test our JPA query using the repository
        List<Ticket> olderTickets = ticketRepository.findTicketsCreatedBetween(
            now.minusDays(15), now.minusDays(7)
        );
        
        assertThat(olderTickets).hasSize(1);
        assertThat(olderTickets.get(0).getTitle()).isEqualTo("Old Ticket");
    }

    @Test
    public void testTimeZoneHandling() {
        // Create tickets with due dates in different timezones
        // Use baseTime + 1 day to ensure constraint satisfaction
        ZonedDateTime utcTime = baseTime.plusDays(1).withZoneSameInstant(ZoneId.of("UTC"));
        ZonedDateTime nyTime = baseTime.plusDays(1).withZoneSameInstant(ZoneId.of("America/New_York"));
        
        Ticket utcTicket = new Ticket("UTC Ticket", "Using UTC timezone", user1, project1);
        utcTicket.setDueDate(utcTime);
        entityManager.persist(utcTicket);
        
        Ticket nyTicket = new Ticket("NY Ticket", "Using NY timezone", user2, project2);
        nyTicket.setDueDate(nyTime);
        entityManager.persist(nyTicket);
        
        entityManager.flush();
        
        // Verify the due dates are stored correctly
        Ticket foundUtcTicket = ticketRepository.findById(utcTicket.getId()).orElseThrow();
        Ticket foundNyTicket = ticketRepository.findById(nyTicket.getId()).orElseThrow();
        
        // Both should have the same instant value
        assertThat(foundUtcTicket.getDueDate().toInstant()).isEqualTo(foundNyTicket.getDueDate().toInstant());
        
        System.out.println("UTC ticket due date: " + foundUtcTicket.getDueDate());
        System.out.println("NY ticket due date: " + foundNyTicket.getDueDate());
        
        // Test timezone-aware queries (both PostgreSQL and CockroachDB should support this)
        try {
            List<Ticket> ticketsDueTodayUTC = ticketRepository.findTicketsDueTodayInTimezone("UTC");
            System.out.println("Tickets due today in UTC: " + ticketsDueTodayUTC.size());
            
            List<Ticket> ticketsDueTodayNY = ticketRepository.findTicketsDueTodayInTimezone("America/New_York");
            System.out.println("Tickets due today in NY: " + ticketsDueTodayNY.size());
        } catch (Exception e) {
            // Log the exception - some features might not be supported
            System.out.println("Timezone-aware query not supported: " + e.getMessage());
        }
    }

    @Test
    public void inspectTemporalSchema() {
        // Inspect the schema to verify temporal column types
        schemaInspector.inspectTableSchema("tickets");
    }
}