package com.trials.crdb.app.repositories;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.ZonedDateTime;
import java.util.ArrayList;
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
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.lang.NonNull;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.trials.crdb.app.model.*;
import com.trials.crdb.app.test.TimeBasedTest;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

import org.springframework.core.env.MapPropertySource;

@Testcontainers
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ContextConfiguration(initializers = ConstraintPostgresTests.DataSourceInitializer.class)
public class ConstraintPostgresTests extends TimeBasedTest {

    @Container
    static final PostgreSQLContainer<?> postgresContainer = 
        new PostgreSQLContainer<>(DockerImageName.parse("postgres:15-alpine"))
            .withDatabaseName("test_constraints")
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

    @PersistenceContext
    private EntityManager entityManager;

    @Autowired
    private TestEntityManager testEntityManager;

    @Autowired
    private SprintRepository sprintRepository;
    
    @Autowired
    private WorkLogRepository workLogRepository;
    
    @Autowired
    private TicketRepository ticketRepository;
    
    @Autowired
    private UserRepository userRepository;
    
    @Autowired
    private ProjectRepository projectRepository;
    
    // Test data
    private User user1, user2;
    private Project project1;
    private Ticket ticket1, ticket2;
    private Sprint sprint1;
    private WorkLog workLog1;
    
    @BeforeEach
    void setUp() {
        super.setupTime(); // Set up fixed time from TimeBasedTest
        
        // Create test data
        createTestData();
    }
    
    private void createTestData() {
        // Clean up existing data
        workLogRepository.deleteAll();
        ticketRepository.deleteAll();
        sprintRepository.deleteAll();
        userRepository.deleteAll();
        projectRepository.deleteAll();
        
        // Create users
        user1 = new User("john", "john@example.com", "John Smith");
        user2 = new User("jane", "jane@example.com", "Jane Doe");
        userRepository.save(user1);
        userRepository.save(user2);
        
        // Create project
        project1 = new Project("Constraint Test Project", "Project for testing constraints");
        projectRepository.save(project1);
        
        // Create tickets
        ticket1 = new Ticket("Test Ticket 1", "First test ticket", user1, project1);
        ticket1.setStatus(Ticket.TicketStatus.OPEN);
        ticket1.setPriority(Ticket.TicketPriority.HIGH);
        
        ticket2 = new Ticket("Test Ticket 2", "Second test ticket", user2, project1);
        ticket2.setStatus(Ticket.TicketStatus.IN_PROGRESS);
        ticket2.setPriority(Ticket.TicketPriority.MEDIUM);
        
        ticketRepository.save(ticket1);
        ticketRepository.save(ticket2);
        
        // Create sprint
        sprint1 = new Sprint(
            "Sprint 1", 
            "First test sprint", 
            baseTime, 
            baseTime.plusDays(14), 
            project1
        );
        sprintRepository.save(sprint1);
        
        // Add tickets to sprint
        sprint1.addTicket(ticket1);
        sprint1.addTicket(ticket2);
        sprintRepository.save(sprint1);
        
        // Create work log
        workLog1 = new WorkLog(
            ticket1,
            user1,
            baseTime.minusHours(2),
            baseTime.minusHours(1),
            "Initial setup",
            1.0
        );
        workLogRepository.save(workLog1);
        
        entityManager.flush();
    }
    
    //-------------------------------------------------------------------------
    // SECTION 1: FOREIGN KEY CASCADE BEHAVIORS
    //-------------------------------------------------------------------------
    
    @Test
    public void testCascadeDeleteFromTicketToWorkLog() {
        // Get the current count of work logs
        long initialWorkLogCount = workLogRepository.count();
        
        // Delete a ticket with work logs
        ticketRepository.delete(ticket1);
        entityManager.flush();
        
        // Verify that the associated work logs are also deleted
        long finalWorkLogCount = workLogRepository.count();
        assertThat(finalWorkLogCount).isEqualTo(initialWorkLogCount - 1);
    }
    
    @Test
    public void testCascadeUpdateFromTicketToWorkLog() {
        // Create a new work log for the ticket
        WorkLog newWorkLog = new WorkLog(
            ticket1,
            user1,
            baseTime.plusHours(1),
            baseTime.plusHours(2),
            "Additional work",
            1.0
        );
        workLogRepository.save(newWorkLog);
        
        // Verify the work log is associated with the ticket
        List<WorkLog> workLogs = workLogRepository.findByTicket(ticket1);
        assertThat(workLogs).hasSize(2);
        
        // Update the ticket
        ticket1.setTitle("Updated Ticket Title");
        ticketRepository.save(ticket1);
        entityManager.flush();
        
        // Verify the work logs are still associated with the updated ticket
        List<WorkLog> updatedWorkLogs = workLogRepository.findByTicket(ticket1);
        assertThat(updatedWorkLogs).hasSize(2);
        assertThat(updatedWorkLogs.get(0).getTicket().getTitle()).isEqualTo("Updated Ticket Title");
    }
    
    @Test
    public void testManyToManyRelationshipBetweenSprintAndTicket() {
        // Verify the tickets are associated with the sprint
        assertThat(sprint1.getTickets()).hasSize(2);
        assertThat(sprint1.getTickets()).extracting("title")
            .containsExactlyInAnyOrder("Test Ticket 1", "Test Ticket 2");
        
        // Verify the sprint is associated with the tickets
        assertThat(ticket1.getSprints()).hasSize(1);
        assertThat(ticket1.getSprints().iterator().next().getName()).isEqualTo("Sprint 1");
    }
    
    @Test
    public void testRemoveTicketFromSprint() {
        // Remove a ticket from the sprint
        sprint1.removeTicket(ticket1);
        sprintRepository.save(sprint1);
        entityManager.flush();
        
        // Verify the ticket is no longer associated with the sprint
        Sprint updatedSprint = sprintRepository.findById(sprint1.getId()).orElseThrow();
        assertThat(updatedSprint.getTickets()).hasSize(1);
        assertThat(updatedSprint.getTickets().iterator().next().getTitle()).isEqualTo("Test Ticket 2");
        
        // Verify the sprint is no longer associated with the ticket
        Ticket updatedTicket = ticketRepository.findById(ticket1.getId()).orElseThrow();
        assertThat(updatedTicket.getSprints()).isEmpty();
    }
    
    //-------------------------------------------------------------------------
    // SECTION 2: CHECK CONSTRAINTS WITH COMPLEX EXPRESSIONS
    //-------------------------------------------------------------------------
    
    @Test
    public void testDueDateConstraint() {
        // Set a due date before create time (should fail)
        ticket1.setDueDate(baseTime.minusDays(1));
        
        // With a check constraint, this would throw a DataIntegrityViolationException
        assertThrows(DataIntegrityViolationException.class, () -> {
            ticketRepository.save(ticket1);
            entityManager.flush();
        });
        
        // Set a valid due date (should succeed)
        ticket1.setDueDate(baseTime.plusDays(1));
        ticketRepository.save(ticket1);
        entityManager.flush();
        
        // Verify the due date was saved
        Ticket updatedTicket = ticketRepository.findById(ticket1.getId()).orElseThrow();
        assertThat(updatedTicket.getDueDate()).isEqualTo(baseTime.plusDays(1));
    }
    
    @Test
    public void testEstimatedHoursConstraint() {
        // Set negative estimated hours (should fail)
        ticket1.setEstimatedHours(-5.0);
        
        // With a check constraint, this would throw a DataIntegrityViolationException
        assertThrows(DataIntegrityViolationException.class, () -> {
            ticketRepository.save(ticket1);
            entityManager.flush();
        });
        
        // Set valid estimated hours (should succeed)
        ticket1.setEstimatedHours(5.0);
        ticketRepository.save(ticket1);
        entityManager.flush();
        
        // Verify the estimated hours were saved
        Ticket updatedTicket = ticketRepository.findById(ticket1.getId()).orElseThrow();
        assertThat(updatedTicket.getEstimatedHours()).isEqualTo(5.0);
    }
    
    //-------------------------------------------------------------------------
    // SECTION 3: UNIQUE CONSTRAINTS
    //-------------------------------------------------------------------------
    
    @Test
    public void testUniqueUsernameConstraint() {
        // Try to create a user with the same username (should fail)
        User duplicateUser = new User("john", "different@example.com", "Different Name");
        
        // This should throw a constraint violation
        assertThrows(DataIntegrityViolationException.class, () -> {
            userRepository.save(duplicateUser);
            entityManager.flush();
        });
        
        // Create a user with a different username (should succeed)
        User uniqueUser = new User("newuser", "new@example.com", "New User");
        userRepository.save(uniqueUser);
        entityManager.flush();
        
        // Verify the user was saved
        User savedUser = userRepository.findByUsername("newuser").orElseThrow();
        assertThat(savedUser.getEmail()).isEqualTo("new@example.com");
    }
    
    @Test
    public void testUniqueProjectNameConstraint() {
        // Try to create a project with the same name (should fail)
        Project duplicateProject = new Project("Constraint Test Project", "Different description");
        
        // This should throw a constraint violation
        assertThrows(DataIntegrityViolationException.class, () -> {
            projectRepository.save(duplicateProject);
            entityManager.flush();
        });
        
        // Create a project with a different name (should succeed)
        Project uniqueProject = new Project("Unique Project", "New project");
        projectRepository.save(uniqueProject);
        entityManager.flush();
        
        // Verify the project was saved
        List<Project> projects = (List<Project>) projectRepository.findAll();
        assertThat(projects).hasSize(2);
    }
    
    //-------------------------------------------------------------------------
    // SECTION 4: FOREIGN KEY CONSTRAINTS
    //-------------------------------------------------------------------------
    
    @Test
    public void testForeignKeyConstraintTicketProject() {
        // Try to create a ticket with a non-existent project (should fail)
        Project nonExistentProject = new Project();
        nonExistentProject.setId(999L); // Non-existent ID
        
        Ticket invalidTicket = new Ticket("Invalid Ticket", "Ticket with invalid project", user1, nonExistentProject);
        
        // This should throw a constraint violation
        assertThrows(DataIntegrityViolationException.class, () -> {
            ticketRepository.save(invalidTicket);
            entityManager.flush();
        });
    }
    
    @Test
    public void testForeignKeyConstraintWorkLogTicket() {
        // Try to create a work log with a non-existent ticket (should fail)
        Ticket nonExistentTicket = new Ticket();
        nonExistentTicket.setId(999L); // Non-existent ID
        
        WorkLog invalidWorkLog = new WorkLog(
            nonExistentTicket,
            user1,
            baseTime,
            baseTime.plusHours(1),
            "Invalid work log",
            1.0
        );
        
        // This should throw a constraint violation
        assertThrows(DataIntegrityViolationException.class, () -> {
            workLogRepository.save(invalidWorkLog);
            entityManager.flush();
        });
    }
    
    //-------------------------------------------------------------------------
    // SECTION 5: EXCLUSION CONSTRAINTS (via application logic)
    //-------------------------------------------------------------------------
    
    @Test
    public void testOverlappingSprintsAreDetected() {
        // Create a sprint that overlaps with sprint1
        Sprint overlappingSprint = new Sprint(
            "Overlapping Sprint",
            "This sprint overlaps with sprint1",
            baseTime.plusDays(7), // Starts during sprint1
            baseTime.plusDays(21), // Ends after sprint1
            project1
        );
        
        // Save the overlapping sprint
        sprintRepository.save(overlappingSprint);
        entityManager.flush();
        
        // Verify the overlapping sprint is detected by our repository method
        List<Sprint> overlappingSprints = sprintRepository.findOverlappingSprints(
            project1,
            overlappingSprint.getStartDate(),
            overlappingSprint.getEndDate()
        );
        
        assertThat(overlappingSprints).hasSize(1);
        assertThat(overlappingSprints.get(0).getName()).isEqualTo("Sprint 1");
    }
    
    @Test
    public void testOverlappingWorkLogsAreDetected() {
        // Create a work log that overlaps with workLog1
        WorkLog overlappingWorkLog = new WorkLog(
            ticket2, // Different ticket, same user
            user1,
            baseTime.minusHours((long) 1.5), // Starts during workLog1
            baseTime.minusHours((long) 0.5), // Ends during workLog1
            "Overlapping work",
            1.0
        );
        
        // Save the overlapping work log
        workLogRepository.save(overlappingWorkLog);
        entityManager.flush();
        
        // Verify the overlapping work log is detected by our repository method
        List<WorkLog> overlappingWorkLogs = workLogRepository.findOverlappingWorkLogs(
            user1,
            overlappingWorkLog.getStartTime(),
            overlappingWorkLog.getEndTime()
        );
        
        assertThat(overlappingWorkLogs).hasSize(1);
        assertThat(overlappingWorkLogs.get(0).getDescription()).isEqualTo("Initial setup");
    }
    
    //-------------------------------------------------------------------------
    // SECTION 6: COMPLEX RELATIONSHIPS AND AGGREGATIONS
    //-------------------------------------------------------------------------
    
    @Test
    public void testTicketWithMultipleWorkLogs() {
        // Create multiple work logs for the same ticket
        WorkLog workLog2 = new WorkLog(
            ticket1,
            user1,
            baseTime.plusHours(1),
            baseTime.plusHours(2),
            "Additional work",
            1.0
        );
        
        WorkLog workLog3 = new WorkLog(
            ticket1,
            user2, // Different user
            baseTime.plusHours(2),
            baseTime.plusHours(3),
            "More work",
            1.0
        );
        
        workLogRepository.saveAll(List.of(workLog2, workLog3));
        entityManager.flush();
        
        // Verify all work logs are associated with the ticket
        List<WorkLog> ticketWorkLogs = workLogRepository.findByTicket(ticket1);
        assertThat(ticketWorkLogs).hasSize(3);
        
        // Verify the total hours calculation
        Double totalHours = workLogRepository.getTotalHoursForTicket(ticket1);
        assertThat(totalHours).isEqualTo(3.0); // 1.0 + 1.0 + 1.0
    }
    
    @Test
    public void testSprintWithMultipleTickets() {
        // Create additional tickets
        Ticket ticket3 = new Ticket("Test Ticket 3", "Third test ticket", user1, project1);
        Ticket ticket4 = new Ticket("Test Ticket 4", "Fourth test ticket", user2, project1);
        
        ticketRepository.saveAll(List.of(ticket3, ticket4));
        
        // Add tickets to sprint
        sprint1.addTicket(ticket3);
        sprint1.addTicket(ticket4);
        
        sprintRepository.save(sprint1);
        entityManager.flush();
        
        // Verify all tickets are associated with the sprint
        Sprint updatedSprint = sprintRepository.findById(sprint1.getId()).orElseThrow();
        assertThat(updatedSprint.getTickets()).hasSize(4);
    }
    
    @Test
    public void testWorkLogTimeRange() {
        // Create work logs for a specific time range
        ZonedDateTime start = baseTime.withHour(9).withMinute(0);
        
        WorkLog morningWorkLog = new WorkLog(
            ticket1,
            user1,
            start,
            start.plusHours(2),
            "Morning work",
            2.0
        );
        
        WorkLog afternoonWorkLog = new WorkLog(
            ticket1,
            user1,
            start.plusHours(4),
            start.plusHours(6),
            "Afternoon work",
            2.0
        );
        
        workLogRepository.saveAll(List.of(morningWorkLog, afternoonWorkLog));
        entityManager.flush();
        
        // Verify work logs can be found by time range
        List<WorkLog> dayWorkLogs = workLogRepository.findWorkLogsByUserAndTimeRange(
            user1,
            start,
            start.plusHours(8)
        );
        
        assertThat(dayWorkLogs).hasSize(2);
        
        // Verify specific time range filter
        List<WorkLog> morningLogs = workLogRepository.findWorkLogsByUserAndTimeRange(
            user1,
            start,
            start.plusHours(3)
        );
        
        assertThat(morningLogs).hasSize(1);
        assertThat(morningLogs.get(0).getDescription()).isEqualTo("Morning work");
    }
}