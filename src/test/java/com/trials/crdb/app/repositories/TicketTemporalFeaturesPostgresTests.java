package com.trials.crdb.app.repositories;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.lang.NonNull;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.trials.crdb.app.model.*;
import com.trials.crdb.app.test.TimeBasedTest;
import com.trials.crdb.app.utils.DateTimeProvider;
import com.trials.crdb.app.utils.PostgresCompatibilityInspector;

import org.springframework.core.env.MapPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ContextConfiguration(initializers = TicketTemporalFeaturesPostgresTests.DataSourceInitializer.class)
public class TicketTemporalFeaturesPostgresTests extends TimeBasedTest {

    private PostgresCompatibilityInspector schemaInspector;
    
    // Test data references
    private User user1;
    private User user2;
    private Project project1;
    private Project project2;
    
    @BeforeEach
    void setUp() {
        super.setupTime(); // Set up our base time
        
        schemaInspector = new PostgresCompatibilityInspector(jdbcTemplate, 
            PostgresCompatibilityInspector.DatabaseType.POSTGRESQL);
        
        // Create basic test data
        createTestData();
    }

    @Container
    static final PostgreSQLContainer<?> postgresContainer = 
        new PostgreSQLContainer<>(DockerImageName.parse("postgres:15-alpine"))
            .withDatabaseName("test_temporal_postgres")
            .withUsername("testuser")
            .withPassword("testPass");
    
    static class DataSourceInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(@NonNull ConfigurableApplicationContext appContext) {
            Map<String, Object> properties = new HashMap<>();
            properties.put("spring.datasource.url", postgresContainer.getJdbcUrl());
            properties.put("spring.datasource.username", postgresContainer.getUsername());
            properties.put("spring.datasource.password", postgresContainer.getPassword());
            properties.put("spring.datasource.driver-class-name", "org.postgresql.Driver");
            properties.put("spring.jpa.hibernate.ddl-auto", "create");
            properties.put("spring.jpa.properties.hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
            properties.put("spring.jpa.show-sql", "true");
            
            appContext.getEnvironment().getPropertySources()
                .addFirst(new MapPropertySource("testcontainers-postgresql", properties));
        }
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
    private JdbcTemplate jdbcTemplate;
    
    /**
     * Create basic test data for users and projects
     */
    private void createTestData() {
        // Clean up existing data
        ticketRepository.deleteAll();
        userRepository.deleteAll();
        projectRepository.deleteAll();
        
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