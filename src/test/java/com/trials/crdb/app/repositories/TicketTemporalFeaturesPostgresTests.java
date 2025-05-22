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
    public void inspectTemporalSchema() {
        // Inspect the schema to verify temporal column types
        schemaInspector.inspectTableSchema("tickets");
    }

    @Test
    public void testOverdueTickets() {
        // Create a ticket due yesterday (overdue)
        Ticket overdueTicket = new Ticket("Overdue Ticket", "Past due date", user1, project1);
        overdueTicket.setDueDate(baseTime.minusDays(1));
        entityManager.persist(overdueTicket);
        
        // Create a ticket due tomorrow (not overdue)
        Ticket upcomingTicket = new Ticket("Future Ticket", "Not yet due", user2, project2);
        upcomingTicket.setDueDate(baseTime.plusDays(1));
        entityManager.persist(upcomingTicket);
        
        // Create a resolved ticket that was due yesterday (not overdue because resolved)
        Ticket resolvedTicket = new Ticket("Resolved Ticket", "Resolved but was overdue", user2, project1);
        resolvedTicket.setDueDate(baseTime.minusDays(1));
        resolvedTicket.setStatus(Ticket.TicketStatus.RESOLVED);
        entityManager.persist(resolvedTicket);
        
        entityManager.flush();
        
        // Test the repository method with our baseTime
        List<Ticket> overdueTickets = ticketRepository.findOverdueTickets(baseTime);
        
        assertThat(overdueTickets).hasSize(1);
        assertThat(overdueTickets.get(0).getTitle()).isEqualTo("Overdue Ticket");
        
        // Test the isOverdue helper method
        assertThat(overdueTicket.isOverdue()).isTrue();
        assertThat(upcomingTicket.isOverdue()).isFalse();
        assertThat(resolvedTicket.isOverdue()).isFalse();
    }

    @Test
    public void testTicketsDueInRange() {
        // Create tickets with different due dates
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
    }

    /**
     * Test the resolution process and updating the resolved date
     */
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

    /**
     * Test timezone handling with due dates
     */
    @Test
    public void testTimeZoneHandling() {
        // Create tickets with due dates in different timezones
        ZonedDateTime utcTime = baseTime.withZoneSameInstant(ZoneId.of("UTC"));
        ZonedDateTime nyTime = baseTime.withZoneSameInstant(ZoneId.of("America/New_York"));
        
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
        
        // Both should have the same instant value, even if they were created from different timezone representations
        assertThat(foundUtcTicket.getDueDate().toInstant()).isEqualTo(foundNyTicket.getDueDate().toInstant());
        
        // When accessed in Java, the timezone information might be normalized
        System.out.println("UTC ticket due date: " + foundUtcTicket.getDueDate());
        System.out.println("NY ticket due date: " + foundNyTicket.getDueDate());
    }



}