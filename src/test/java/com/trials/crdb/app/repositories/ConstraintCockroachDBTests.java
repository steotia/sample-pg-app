package com.trials.crdb.app.repositories;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.exception.ConstraintViolationException;
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
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.trials.crdb.app.model.*;
import com.trials.crdb.app.test.TimeBasedTest;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

import org.springframework.core.env.MapPropertySource;

import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.annotation.Propagation;
import org.junit.jupiter.api.TestInstance;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.function.Supplier;

@Testcontainers
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ContextConfiguration(initializers = ConstraintCockroachDBTests.DataSourceInitializer.class)
public class ConstraintCockroachDBTests extends TimeBasedTest {

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
            properties.put("spring.jpa.hibernate.ddl-auto", "create");
            properties.put("spring.jpa.properties.hibernate.dialect", "org.hibernate.dialect.CockroachDialect");
            properties.put("spring.jpa.show-sql", "true");
            
            appContext.getEnvironment().getPropertySources()
                .addFirst(new MapPropertySource("testcontainers-cockroachdb", properties));
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

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private UserProjectRoleRepository userProjectRoleRepository;
    
    // Test data
    private User user1, user2;
    private Project project1;
    private Ticket ticket1, ticket2;
    private Sprint sprint1;
    private WorkLog workLog1;
    
    @BeforeEach
    void setUp() {
        super.setupTime(); // Set up fixed time from TimeBasedTest
        
        TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
        txTemplate.execute(status -> {
            createTestData();
            return null;
        });
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
        
        // Save tickets and log IDs
        ticket1 = ticketRepository.save(ticket1);
        ticket2 = ticketRepository.save(ticket2);
        
        System.out.println("Ticket1 ID after save: " + ticket1.getId());
        System.out.println("Ticket2 ID after save: " + ticket2.getId());
        
        // Create sprint
        sprint1 = new Sprint(
            "Sprint 1", 
            "First test sprint", 
            baseTime, 
            baseTime.plusDays(14), 
            project1
        );
        sprintRepository.save(sprint1);
        
        // Create work log
        System.out.println("Creating worklog with ticket ID: " + ticket1.getId());
        workLog1 = new WorkLog(
            ticket1,
            user1,
            baseTime.minusHours(2),
            baseTime.minusHours(1),
            "Initial setup",
            1.0
        );
        workLogRepository.save(workLog1);
        
        // Now add tickets to sprint
        sprint1.addTicket(ticket1);
        sprint1.addTicket(ticket2);
        sprintRepository.save(sprint1);
    }
    
    //-------------------------------------------------------------------------
    // SECTION 1: FOREIGN KEY CASCADE BEHAVIORS
    //-------------------------------------------------------------------------
    
    @Test
    public void testCreateWorkLog() {
        // Create a new ticket that won't be involved in any other relationships
        Ticket simpleTicket = new Ticket("Simple Ticket", "For testing worklogs", user1, project1);
        simpleTicket = ticketRepository.saveAndFlush(simpleTicket);
        
        // Manually detach all entities to start with a clean state
        entityManager.clear();
        
        // Reload the ticket to ensure it's in a managed state
        Ticket managedTicket = ticketRepository.findById(simpleTicket.getId()).orElseThrow();
        User managedUser = userRepository.findById(user1.getId()).orElseThrow();
        
        // Create a worklog using the helper method in Ticket
        WorkLog testLog = new WorkLog();
        testLog.setDescription("Test worklog");
        testLog.setStartTime(baseTime);
        testLog.setEndTime(baseTime.plusHours(1));
        testLog.setHoursSpent(1.0);
        testLog.setUser(managedUser);
        testLog.setTicket(managedTicket);
        
        // Save the worklog directly
        WorkLog savedLog = workLogRepository.saveAndFlush(testLog);
        
        // Verify it was saved correctly
        assertThat(savedLog.getId()).isNotNull();
        
        // Verify the relationship works in both directions
        Ticket fetchedTicket = ticketRepository.findById(managedTicket.getId()).orElseThrow();
        assertThat(fetchedTicket.getWorkLogs()).hasSize(1);
    }

    @Test
    public void testCascadeDeleteFromTicketToWorkLog() {
        // Create a dedicated ticket just for this test
        Ticket testTicket = new Ticket("Cascade Test Ticket", "For testing cascade delete", user1, project1);
        testTicket = ticketRepository.save(testTicket);
        Long ticketId = testTicket.getId();
        
        // Create a work log
        WorkLog testLog = new WorkLog();
        testLog.setDescription("Test log for cascade delete");
        testLog.setStartTime(baseTime);
        testLog.setEndTime(baseTime.plusHours(1));
        testLog.setHoursSpent(1.0);
        testLog.setUser(user1);
        testLog.setTicket(testTicket);
        workLogRepository.save(testLog);
        Long workLogId = testLog.getId();
        
        // Clear persistence context to ensure we're starting fresh
        entityManager.flush();
        entityManager.clear();
        
        // Verify the work log exists
        assertThat(workLogRepository.existsById(workLogId)).isTrue();
        
        // Delete the ticket by ID - avoiding entity references completely
        ticketRepository.deleteById(ticketId);
        
        // Verify the work log was deleted
        assertThat(workLogRepository.existsById(workLogId)).isFalse();
    }
    
    //-------------------------------------------------------------------------
    // SECTION 2: CHECK CONSTRAINTS WITH COMPLEX EXPRESSIONS
    //-------------------------------------------------------------------------
    
    @Test
    public void testInvalidDueDateShouldFailConstraint() {
        // Create a valid ticket
        User testUser = userRepository.findById(user1.getId()).orElseThrow();
        Project testProject = projectRepository.findById(project1.getId()).orElseThrow();
        
        Ticket testTicket = new Ticket("Invalid Due Date Test", "Testing due date constraint", testUser, testProject);
        testTicket.setStatus(Ticket.TicketStatus.OPEN);
        testTicket.setPriority(Ticket.TicketPriority.MEDIUM);
        ticketRepository.saveAndFlush(testTicket);
        
        // Set invalid due date
        testTicket.setDueDate(baseTime.minusDays(1)); // Before create time
        
        // This should fail due to check constraint
        assertThrows(Exception.class, () -> {
            ticketRepository.saveAndFlush(testTicket);
        });
    }

    @Test
    public void testValidDueDateShouldPass() {
        // Create a valid ticket
        User testUser = userRepository.findById(user1.getId()).orElseThrow();
        Project testProject = projectRepository.findById(project1.getId()).orElseThrow();
        
        Ticket testTicket = new Ticket("Valid Due Date Test", "Testing due date constraint", testUser, testProject);
        testTicket.setStatus(Ticket.TicketStatus.OPEN);
        testTicket.setPriority(Ticket.TicketPriority.MEDIUM);
        
        // Save ticket without a due date first
        testTicket = ticketRepository.saveAndFlush(testTicket);
        
        // Get the actual create time from the saved ticket
        ZonedDateTime actualCreateTime = testTicket.getCreateTime();
        
        // Set valid due date based on the actual create time
        testTicket.setDueDate(actualCreateTime.plusDays(1));
        testTicket = ticketRepository.saveAndFlush(testTicket);
        
        // Verify due date was saved
        entityManager.clear();
        Ticket updatedTicket = ticketRepository.findById(testTicket.getId()).orElseThrow();
        assertThat(updatedTicket.getDueDate()).isEqualTo(actualCreateTime.plusDays(1));
    }

    @Test
    public void testInvalidEstimatedHoursShouldFailConstraint() {
        // Create a valid ticket
        User testUser = userRepository.findById(user1.getId()).orElseThrow();
        Project testProject = projectRepository.findById(project1.getId()).orElseThrow();
        
        Ticket testTicket = new Ticket("Invalid Hours Test", "Testing hours constraint", testUser, testProject);
        testTicket.setStatus(Ticket.TicketStatus.OPEN);
        testTicket.setPriority(Ticket.TicketPriority.MEDIUM);
        ticketRepository.saveAndFlush(testTicket);
        
        // Set invalid negative hours
        testTicket.setEstimatedHours(-5.0);
        
        // This should fail due to check constraint
        assertThrows(Exception.class, () -> {
            ticketRepository.saveAndFlush(testTicket);
        });
    }

    @Test
    public void testValidEstimatedHoursShouldPass() {
        // Create a valid ticket
        User testUser = userRepository.findById(user1.getId()).orElseThrow();
        Project testProject = projectRepository.findById(project1.getId()).orElseThrow();
        
        Ticket testTicket = new Ticket("Valid Hours Test", "Testing hours constraint", testUser, testProject);
        testTicket.setStatus(Ticket.TicketStatus.OPEN);
        testTicket.setPriority(Ticket.TicketPriority.MEDIUM);
        testTicket = ticketRepository.saveAndFlush(testTicket);
        
        // Set valid positive hours
        testTicket.setEstimatedHours(5.0);
        testTicket = ticketRepository.saveAndFlush(testTicket);
        
        // Verify hours were saved
        entityManager.clear(); // Ensure we get a fresh entity
        Ticket updatedTicket = ticketRepository.findById(testTicket.getId()).orElseThrow();
        assertThat(updatedTicket.getEstimatedHours()).isEqualTo(5.0);
    }
    
    //-------------------------------------------------------------------------
    // SECTION 3: UNIQUE CONSTRAINTS DataIntegrityViolationException
    //-------------------------------------------------------------------------
    
    @Test
    public void testUniqueUsernameConstraintViolation() {
        // Try to create a user with the same username as existing user1 ("john")
        User duplicateUser = new User("john", "different@example.com", "Different Name");
        
        assertThrows(DataIntegrityViolationException.class, () -> {
            userRepository.save(duplicateUser);
            entityManager.flush();
        });
    }

    @Test 
    public void testUniqueUsernameSuccess() {
        // Create a user with a unique username (should succeed)
        User uniqueUser = new User("newuser", "new@example.com", "New User");
        User savedUser = userRepository.save(uniqueUser);
        entityManager.flush();
        
        assertThat(savedUser.getUsername()).isEqualTo("newuser");
        assertThat(savedUser.getEmail()).isEqualTo("new@example.com");
        
        // Verify it can be found
        User foundUser = userRepository.findByUsername("newuser").orElseThrow();
        assertThat(foundUser.getEmail()).isEqualTo("new@example.com");
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
    
    //-------------------------------------------------------------------------
    // SECTION 5: ADVANCED FOREIGN KEY BEHAVIORS
    //-------------------------------------------------------------------------

    @Test
    public void testCascadeOnUpdate() {
        // Create a project with a manual ID
        entityManager.createNativeQuery(
            "INSERT INTO projects (id, name, description, create_time) VALUES (100, 'Update Cascade Test Project', 'Testing ON UPDATE CASCADE', CURRENT_TIMESTAMP)"
        ).executeUpdate();
        
        Project cascadeProject = entityManager.find(Project.class, 100L);
        
        // Create sprints for this project
        Sprint sprint1 = new Sprint("Sprint 1", "First sprint", baseTime, baseTime.plusDays(14), cascadeProject);
        Sprint sprint2 = new Sprint("Sprint 2", "Second sprint", baseTime.plusDays(15), baseTime.plusDays(29), cascadeProject);
        sprintRepository.saveAll(List.of(sprint1, sprint2));
        
        // Verify sprints are associated with the project
        List<Sprint> projectSprints = sprintRepository.findByProject(cascadeProject);
        assertThat(projectSprints).hasSize(2);
        
        // Change the project ID using native SQL
        entityManager.createNativeQuery(
            "UPDATE projects SET id = 200 WHERE id = 100"
        ).executeUpdate();
        
        // Clear persistence context to get fresh data
        entityManager.clear();
        
        // Verify the project was updated
        Project updatedProject = entityManager.find(Project.class, 200L);
        assertThat(updatedProject).isNotNull();
        assertThat(updatedProject.getName()).isEqualTo("Update Cascade Test Project");
        
        // Verify sprints were also updated with the new FK
        List<Sprint> updatedSprints = sprintRepository.findByProject(updatedProject);
        assertThat(updatedSprints).hasSize(2);
        
        // The old project ID should no longer exist
        assertThat(entityManager.find(Project.class, 100L)).isNull();
    }

    @Test
    public void testSelfReferentialForeignKey() {
        // Create parent ticket
        Ticket parentTicket = new Ticket("Parent Task", "Parent", user1, project1);
        parentTicket = ticketRepository.saveAndFlush(parentTicket);
        Long parentId = parentTicket.getId();
        System.out.println("Created parent ticket ID: " + parentId);
        
        // Create child ticket that depends on parent
        Ticket childTicket = new Ticket("Child Task", "Depends on parent", user1, project1);
        childTicket.setDependentOn(parentTicket);
        childTicket = ticketRepository.saveAndFlush(childTicket);
        Long childId = childTicket.getId();
        System.out.println("Created child ticket ID: " + childId);
        
        // Verify the relationship was saved
        Ticket reloadedChild = ticketRepository.findById(childId).orElseThrow();
        System.out.println("Child dependent_on_id: " + (reloadedChild.getDependentOn() != null ? reloadedChild.getDependentOn().getId() : "NULL"));
        
        // Check constraint exists in database
        List<Object[]> constraints = entityManager.createNativeQuery(
            "SELECT constraint_name, constraint_type FROM information_schema.table_constraints " +
            "WHERE table_name = 'tickets' AND constraint_type = 'FOREIGN KEY'"
        ).getResultList();
        
        System.out.println("Foreign key constraints on tickets table:");
        for (Object[] row : constraints) {
            System.out.println("  " + row[0] + " - " + row[1]);
        }
        
        // Test constraint using native SQL
        try {
            int rows = entityManager.createNativeQuery("DELETE FROM tickets WHERE id = ?1")
                .setParameter(1, parentId)
                .executeUpdate();
            System.out.println("Deleted rows: " + rows + " (this should have failed!)");
            fail("Expected constraint violation but delete succeeded");
        } catch (Exception e) {
            System.out.println("Got expected exception: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        
        // Clean up
        ticketRepository.deleteById(childId);
        ticketRepository.deleteById(parentId);
    }

    @Test
    public void testSetNullOnDelete() {
        // Step 1: Create and persist entities in clean state
        User assignee = new User("assignee_test", "assignee@example.com", "Test Assignee");
        assignee = userRepository.saveAndFlush(assignee);
        Long assigneeId = assignee.getId();
        
        Ticket ticket = new Ticket("SET NULL Test", "Testing SET NULL constraint", user1, project1);
        ticket.setAssignee(assignee);
        ticket = ticketRepository.saveAndFlush(ticket);
        Long ticketId = ticket.getId();
        
        // Step 2: Verify the relationship exists
        assertThat(ticket.getAssignee()).isNotNull();
        assertThat(ticket.getAssignee().getId()).isEqualTo(assigneeId);
        
        // Step 3: Clear persistence context to avoid entity state conflicts
        entityManager.clear();
        
        // Step 4: Delete user by ID (not by entity reference)
        userRepository.deleteById(assigneeId);
        
        // Step 5: Flush changes and clear context again
        entityManager.flush();
        entityManager.clear();
        
        // Step 6: Load fresh ticket from database and verify SET NULL worked
        Ticket freshTicket = ticketRepository.findById(ticketId).orElseThrow();
        assertThat(freshTicket.getAssignee()).isNull();
        
        System.out.println("SUCCESS: JPA/Hibernate + ON DELETE SET NULL constraint worked!");
    }
}