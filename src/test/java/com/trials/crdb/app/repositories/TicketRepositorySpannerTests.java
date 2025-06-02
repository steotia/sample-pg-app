package com.trials.crdb.app.repositories;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.ZonedDateTime;
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
import com.trials.crdb.app.utils.PostgresCompatibilityInspector;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
public class TicketRepositorySpannerTests {

    private static final String PROJECT_ID = "emulator-project";
    private static final String INSTANCE_ID = "test-instance";
    private static final String DATABASE_ID = "test-database";

    private PostgresCompatibilityInspector schemaInspector;
    
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
    }

    // Implement the same tests as in PostgreSQL test class
    @Test
    public void whenCreateTicket_thenTicketIsPersistedWithDefaultValues() {
        // Create users and project
        User reporter = new User("reporter", "reporter@example.com", "Test Reporter");
        User assignee = new User("assignee", "assignee@example.com", "Test Assignee");
        Project project = new Project("Ticket Test Project", "Project for testing tickets");
        
        entityManager.persist(reporter);
        entityManager.persist(assignee);
        entityManager.persist(project);
        
        // Create ticket
        Ticket ticket = new Ticket("Test Ticket", "This is a test ticket", reporter, project);
        ticket.assignTo(assignee);
        
        Ticket savedTicket = ticketRepository.save(ticket);
        
        // Verify ticket was saved
        assertThat(savedTicket).isNotNull();
        assertThat(savedTicket.getId()).isNotNull();
        assertThat(savedTicket.getStatus()).isEqualTo(Ticket.TicketStatus.OPEN);
        assertThat(savedTicket.getPriority()).isEqualTo(Ticket.TicketPriority.MEDIUM);
        assertThat(savedTicket.getReporter()).isEqualTo(reporter);
        assertThat(savedTicket.getAssignee()).isEqualTo(assignee);
        assertThat(savedTicket.getProject()).isEqualTo(project);
        assertThat(savedTicket.getCreateTime()).isNotNull();
        
        // Inspect the table schema
        schemaInspector.inspectTableSchema("tickets");
    }

    @Test
    public void whenSaveTicketWithMetadata_thenMetadataIsPersisted() {
        // Create users and project
        User reporter = new User("reporter2", "reporter2@example.com", "Metadata Reporter");
        Project project = new Project("Metadata Project", "Project for testing metadata");
        
        entityManager.persist(reporter);
        entityManager.persist(project);
        
        // Create ticket with metadata
        Ticket ticket = new Ticket("Metadata Ticket", "This is a ticket with metadata", reporter, project);
        ticket.setMetadataValue("category", "bug");
        ticket.setMetadataValue("browser", "Chrome");
        ticket.setMetadataValue("version", "1.0.0");
        
        Ticket savedTicket = ticketRepository.save(ticket);
        entityManager.flush();
        entityManager.clear();
        
        // Retrieve the ticket
        Ticket foundTicket = ticketRepository.findById(savedTicket.getId()).orElse(null);
        
        // Verify metadata was saved
        assertThat(foundTicket).isNotNull();
        assertThat(foundTicket.getMetadata()).containsEntry("category", "bug");
        assertThat(foundTicket.getMetadata()).containsEntry("browser", "Chrome");
        assertThat(foundTicket.getMetadata()).containsEntry("version", "1.0.0");
        
        // Test metadata query
        List<Ticket> bugsTickets = ticketRepository.findByMetadataKeyValue("category", "bug");
        assertThat(bugsTickets).contains(foundTicket);
    }
    
    @Test
    public void whenFindByStatusAndPriority_thenReturnMatchingTickets() {
        // Create users and project
        User reporter = new User("reporter3", "reporter3@example.com", "Filter Reporter");
        Project project = new Project("Filter Project", "Project for testing filters");
        
        entityManager.persist(reporter);
        entityManager.persist(project);
        
        // Create tickets with different statuses and priorities
        Ticket ticket1 = new Ticket("High Priority", "This is a high priority ticket", reporter, project);
        ticket1.setPriority(Ticket.TicketPriority.HIGH);
        ticket1.setStatus(Ticket.TicketStatus.OPEN);
        
        Ticket ticket2 = new Ticket("Medium Priority", "This is a medium priority ticket", reporter, project);
        ticket2.setPriority(Ticket.TicketPriority.MEDIUM);
        ticket2.setStatus(Ticket.TicketStatus.OPEN);
        
        Ticket ticket3 = new Ticket("Closed Ticket", "This is a closed ticket", reporter, project);
        ticket3.setStatus(Ticket.TicketStatus.CLOSED);
        
        entityManager.persist(ticket1);
        entityManager.persist(ticket2);
        entityManager.persist(ticket3);
        entityManager.flush();
        
        // Test filtering
        List<Ticket> openTickets = ticketRepository.findByStatus(Ticket.TicketStatus.OPEN);
        assertThat(openTickets).hasSize(2);
        assertThat(openTickets).extracting("title").containsExactlyInAnyOrder("High Priority", "Medium Priority");
        
        List<Ticket> highPriorityTickets = ticketRepository.findByPriority(Ticket.TicketPriority.HIGH);
        assertThat(highPriorityTickets).hasSize(1);
        assertThat(highPriorityTickets.get(0).getTitle()).isEqualTo("High Priority");
        
        List<Ticket> openHighPriorityTickets = ticketRepository.findByStatusAndPriority(Ticket.TicketStatus.OPEN, Ticket.TicketPriority.HIGH);
        assertThat(openHighPriorityTickets).hasSize(1);
        assertThat(openHighPriorityTickets.get(0).getTitle()).isEqualTo("High Priority");
    }
    
    @Test
    public void whenUpdateTicketStatus_thenStatusAndUpdateTimeChange() {
        // Create ticket
        User reporter = new User("reporter4", "reporter4@example.com", "Update Reporter");
        Project project = new Project("Update Project", "Project for testing updates");
        
        entityManager.persist(reporter);
        entityManager.persist(project);
        
        Ticket ticket = new Ticket("Status Update", "This is a ticket for testing status updates", reporter, project);
        entityManager.persist(ticket);
        entityManager.flush();
        
        // Capture initial update time
        ZonedDateTime initialUpdateTime = ticket.getUpdateTime();
        
        // Update status
        ticket.setStatus(Ticket.TicketStatus.IN_PROGRESS);
        entityManager.persist(ticket);
        entityManager.flush();
        entityManager.clear();
        
        // Verify status and update time changed
        Ticket updatedTicket = entityManager.find(Ticket.class, ticket.getId());
        assertThat(updatedTicket.getStatus()).isEqualTo(Ticket.TicketStatus.IN_PROGRESS);
        assertThat(updatedTicket.getUpdateTime()).isNotEqualTo(initialUpdateTime);
    }
}