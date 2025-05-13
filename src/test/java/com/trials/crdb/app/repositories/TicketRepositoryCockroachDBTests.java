package com.trials.crdb.app.repositories;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.lang.NonNull;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.trials.crdb.app.model.*;
import com.trials.crdb.app.utils.PostgresCompatibilityInspector;

import org.springframework.core.env.MapPropertySource;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ContextConfiguration(initializers = TicketRepositoryCockroachDBTests.DataSourceInitializer.class)
public class TicketRepositoryCockroachDBTests {

    private PostgresCompatibilityInspector schemaInspector;

    @BeforeEach
    void setUp() {
        schemaInspector = new PostgresCompatibilityInspector(jdbcTemplate, 
            PostgresCompatibilityInspector.DatabaseType.COCKROACHDB);
    }

    // CockroachDB container
    @Container
    static final CockroachContainer cockroachContainer = 
        new CockroachContainer(DockerImageName.parse("cockroachdb/cockroach:latest"))
            .withCommand("start-single-node --insecure");

    static class DataSourceInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(@NonNull ConfigurableApplicationContext appContext) {
            Map<String, Object> properties = new HashMap<>();
            properties.put("spring.datasource.url", cockroachContainer.getJdbcUrl());
            properties.put("spring.datasource.username", cockroachContainer.getUsername());
            properties.put("spring.datasource.password", cockroachContainer.getPassword());
            properties.put("spring.datasource.driver-class-name", "org.postgresql.Driver");
            properties.put("spring.jpa.hibernate.ddl-auto", "create-drop");
            properties.put("spring.jpa.properties.hibernate.dialect", "org.hibernate.dialect.CockroachDialect");
            properties.put("spring.jpa.show-sql", "true");
            
            appContext.getEnvironment().getPropertySources()
                .addFirst(new MapPropertySource("testcontainers-cockroachdb", properties));
        }
    }

    @Autowired
    private TestEntityManager entityManager;

    @Autowired
    private TicketRepository ticketRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    // Copy the same tests from PostgreSQL test class
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