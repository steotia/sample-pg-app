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
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.trials.crdb.app.model.*;
import com.trials.crdb.app.model.UserProjectRole.UserProjectRoleId;
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
        
        // Add tickets to sprint - save this until after worklog creation
        // sprint1.addTicket(ticket1);
        // sprint1.addTicket(ticket2);
        // sprintRepository.save(sprint1);
        
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
        
        // entityManager.flush();
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
    
    // @Test
    // public void testEstimatedHoursConstraint() {
    //     // Set negative estimated hours (should fail)
    //     ticket1.setEstimatedHours(-5.0);
        
    //     // With a check constraint, this would throw a DataIntegrityViolationException
    //     assertThrows(DataIntegrityViolationException.class, () -> {
    //         ticketRepository.save(ticket1);
    //         entityManager.flush();
    //     });
        
    //     // Set valid estimated hours (should succeed)
    //     ticket1.setEstimatedHours(5.0);
    //     ticketRepository.save(ticket1);
    //     entityManager.flush();
        
    //     // Verify the estimated hours were saved
    //     Ticket updatedTicket = ticketRepository.findById(ticket1.getId()).orElseThrow();
    //     assertThat(updatedTicket.getEstimatedHours()).isEqualTo(5.0);
    // }
    
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
    // SECTION 5: ADVANCED FOREIGN KEY BEHAVIORS
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
    public void testCascadeOnUpdate() {
        // Create a project with a manual ID
        entityManager.createNativeQuery(
            "INSERT INTO projects (id, name, description, create_time) VALUES (100, 'Update Test Project', 'Testing ON UPDATE CASCADE', CURRENT_TIMESTAMP)"
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
        assertThat(updatedProject.getName()).isEqualTo("Update Test Project");
        
        // Verify sprints were also updated with the new FK
        List<Sprint> updatedSprints = sprintRepository.findByProject(updatedProject);
        assertThat(updatedSprints).hasSize(2);
        
        // The old project ID should no longer exist
        assertThat(entityManager.find(Project.class, 100L)).isNull();
    }


    /**
     * Tests composite foreign keys with UserProjectRole entity.
     * This tests that:
     * 1. Composite primary keys work correctly
     * 2. Foreign key constraints on multiple columns work
     * 3. Cascading operations work with composite keys
     */
    @Test
    public void testCompositeForeignKey() {
        // Create a role assignment
        UserProjectRole role = new UserProjectRole(user1, project1, "ADMIN");
        userProjectRoleRepository.save(role);
        entityManager.flush();
        
        // Verify it was saved
        UserProjectRole.UserProjectRoleId roleId = new UserProjectRoleId(user1.getId(), project1.getId());
        UserProjectRole savedRole = userProjectRoleRepository.findById(roleId).orElseThrow();
        assertThat(savedRole.getRoleName()).isEqualTo("ADMIN");
        
        // Test composite primary key constraint via update
        UserProjectRole updatedRole = new UserProjectRole(user1, project1, "DEVELOPER");
        userProjectRoleRepository.save(updatedRole);
        entityManager.flush();
        
        // Should still be only one record, but with updated role name
        UserProjectRole refreshedRole = userProjectRoleRepository.findById(roleId).orElseThrow();
        assertThat(refreshedRole.getRoleName()).isEqualTo("DEVELOPER");
        
        // Try to delete using direct SQL to bypass any JPA/Hibernate handling
        assertThrows(Exception.class, () -> {
            entityManager.createNativeQuery("DELETE FROM users WHERE id = ?")
                .setParameter(1, user1.getId())
                .executeUpdate();
        });
        
        // Verify the role still exists
        assertThat(userProjectRoleRepository.existsById(roleId)).isTrue();
        
        // Clean up properly - delete role first, then user
        userProjectRoleRepository.delete(refreshedRole);
        entityManager.flush();
        
        userRepository.delete(user1);
        entityManager.flush();
        
        // Verify both are gone
        assertThat(userProjectRoleRepository.existsById(roleId)).isFalse();
        assertThat(userRepository.existsById(user1.getId())).isFalse();
    }

    /**
     * Tests self-referential foreign keys with ticket dependencies.
     * This tests:
     * 1. Self-referential relationships work correctly
     * 2. Multi-level dependency chains can be created and traversed
     * 3. Circular dependencies are prevented
     */
    /**
     * Tests self-referential foreign keys with ticket dependencies.
     */
    @Test
    public void testSelfReferentialForeignKey() {
        // Create a chain of ticket dependencies
        Ticket parentTicket = new Ticket("Parent Ticket", "Parent level", user1, project1);
        parentTicket.setStatus(Ticket.TicketStatus.OPEN);
        parentTicket.setPriority(Ticket.TicketPriority.HIGH);
        parentTicket = ticketRepository.save(parentTicket);
        
        Ticket childTicket = new Ticket("Child Ticket", "Child level", user1, project1);
        childTicket.setStatus(Ticket.TicketStatus.OPEN);
        childTicket.setPriority(Ticket.TicketPriority.MEDIUM);
        childTicket.setDependentOn(parentTicket);
        ticketRepository.save(childTicket);
        
        Ticket grandchildTicket = new Ticket("Grandchild Ticket", "Grandchild level", user1, project1);
        grandchildTicket.setStatus(Ticket.TicketStatus.OPEN);
        grandchildTicket.setPriority(Ticket.TicketPriority.LOW);
        grandchildTicket.setDependentOn(childTicket);
        grandchildTicket = ticketRepository.save(grandchildTicket);
        
        // Verify dependency relationships
        entityManager.flush();
        entityManager.clear();
        
        Ticket fetchedGrandchild = ticketRepository.findById(grandchildTicket.getId()).orElseThrow();
        Ticket fetchedChild = fetchedGrandchild.getDependentOn();
        Ticket fetchedParent = fetchedChild.getDependentOn();
        
        assertThat(fetchedChild.getId()).isEqualTo(childTicket.getId());
        assertThat(fetchedParent.getId()).isEqualTo(parentTicket.getId());
        assertThat(fetchedParent.getDependentOn()).isNull();
        
        // Verify we can traverse the dependency chain in both directions
        List<Ticket> childDependencies = ticketRepository.findByDependentOn(childTicket);
        assertThat(childDependencies).hasSize(1);
        assertThat(childDependencies.get(0).getId()).isEqualTo(grandchildTicket.getId());
        
        // Test deleting a ticket that has dependents
        // This should fail due to foreign key constraint (default behavior is RESTRICT/NO ACTION)
        assertThrows(Exception.class, () -> {
            ticketRepository.delete(childTicket);
            entityManager.flush();
        });
        
        // Delete in correct order (grandchild first, then child, then parent)
        ticketRepository.delete(grandchildTicket);
        entityManager.flush();
        
        // Now we should be able to delete the child
        ticketRepository.delete(childTicket);
        entityManager.flush();
        
        // And finally delete the parent
        ticketRepository.delete(parentTicket);
        entityManager.flush();
        
        // Verify all tickets are deleted
        assertThat(ticketRepository.findById(grandchildTicket.getId())).isEmpty();
        assertThat(ticketRepository.findById(childTicket.getId())).isEmpty();
        assertThat(ticketRepository.findById(parentTicket.getId())).isEmpty();
    }

    /**
     * Tests ON DELETE SET NULL foreign key action.
     * When a user is deleted, tickets assigned to them will have their assignee set to NULL.
     */
    @Test
    public void testSetNullOnDelete() {
        // Create a new user
        User assignee = new User("assignee", "assignee@example.com", "Test Assignee");
        userRepository.save(assignee);
        
        // Create a ticket assigned to this user
        Ticket ticket = new Ticket("Assigned Ticket", "Testing SET NULL", user1, project1);
        ticket.setStatus(Ticket.TicketStatus.OPEN);
        ticket.setPriority(Ticket.TicketPriority.MEDIUM);
        ticket.setAssignee(assignee); // Assign to the user we'll delete
        ticketRepository.save(ticket);
        
        // Verify the assignee is set
        entityManager.flush();
        entityManager.clear();
        
        ticket = ticketRepository.findById(ticket.getId()).orElseThrow();
        assertThat(ticket.getAssignee()).isNotNull();
        assertThat(ticket.getAssignee().getId()).isEqualTo(assignee.getId());
        
        // Delete the user
        userRepository.delete(assignee);
        entityManager.flush();
        entityManager.clear();
        
        // Verify the ticket's assignee is now NULL
        ticket = ticketRepository.findById(ticket.getId()).orElseThrow();
        assertThat(ticket.getAssignee()).isNull();
    }

    /**
     * Tests ON DELETE RESTRICT foreign key action.
     * Cannot delete a project if it has tickets.
     */
    @Test
    public void testRestrictOnDelete() {
        // Create a project
        Project restrictProject = new Project("Restrict Test", "Testing RESTRICT");
        projectRepository.save(restrictProject);
        
        // Create a ticket for this project
        Ticket ticket = new Ticket("Restrict Test Ticket", "Testing RESTRICT constraint", user1, restrictProject);
        ticket.setStatus(Ticket.TicketStatus.OPEN);
        ticket.setPriority(Ticket.TicketPriority.MEDIUM);
        ticketRepository.save(ticket);
        
        entityManager.flush();
        
        // Try to delete the project - should fail due to RESTRICT constraint
        assertThrows(Exception.class, () -> {
            projectRepository.delete(restrictProject);
            entityManager.flush();
        });
        
        // Delete the ticket first, then the project should be deletable
        ticketRepository.delete(ticket);
        projectRepository.delete(restrictProject);
        entityManager.flush();
        
        // Verify the project was deleted
        assertThat(projectRepository.findById(restrictProject.getId())).isEmpty();
    }

    /**
     * Tests basic composite primary key functionality.
     * This test verifies that composite primary keys work correctly.
     */
    @Test
    public void testCompositePrimaryKey() {
        // Create a new role with a composite PK
        UserProjectRole role = new UserProjectRole(user1, project1, "ADMIN");
        userProjectRoleRepository.save(role);
        
        // Verify it exists
        UserProjectRole.UserProjectRoleId roleId = new UserProjectRoleId(user1.getId(), project1.getId());
        assertThat(userProjectRoleRepository.findById(roleId)).isPresent();
        
        // Update the role
        role.setRoleName("DEVELOPER");
        userProjectRoleRepository.save(role);
        
        // Verify update worked
        UserProjectRole updated = userProjectRoleRepository.findById(roleId).orElseThrow();
        assertThat(updated.getRoleName()).isEqualTo("DEVELOPER");
        
        // Clean up
        userProjectRoleRepository.delete(role);
    }

    /**
     * Tests that proper deletion order works with composite foreign keys.
     * This test demonstrates that you must delete the role before deleting
     * the entities it references.
     */
    @Test
    public void testProperDeletionOrder() {
        // Create a new user and project for this test only
        User roleUser = new User("roleuser", "role@example.com", "Role User");
        userRepository.save(roleUser);
        
        Project roleProject = new Project("Role Project", "For testing roles");
        projectRepository.save(roleProject);
        
        // Create a role linking them
        UserProjectRole role = new UserProjectRole(roleUser, roleProject, "TESTER");
        userProjectRoleRepository.save(role);
        entityManager.flush();
        
        // Delete in correct order - role first
        userProjectRoleRepository.delete(role);
        entityManager.flush();
        
        // Now we can delete the user and project
        userRepository.delete(roleUser);
        projectRepository.delete(roleProject);
        entityManager.flush();
        
        // Verify all are gone
        UserProjectRole.UserProjectRoleId roleId = new UserProjectRoleId(roleUser.getId(), roleProject.getId());
        assertThat(userProjectRoleRepository.findById(roleId)).isEmpty();
        assertThat(userRepository.findById(roleUser.getId())).isEmpty();
        assertThat(projectRepository.findById(roleProject.getId())).isEmpty();
    }

    /**
     * Tests that foreign key constraints prevent deleting referenced entities.
     * This test verifies that you cannot delete a user that is referenced by a role.
     */
    @Test
    public void testForeignKeyConstraintViolation() {
        TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
        
        // Step 1: Create test data in a transaction
        final Long[] ids = new Long[2]; // To hold IDs for later use
        
        txTemplate.execute(status -> {
            // Create new entities for this test only
            User testUser = new User("constraintuser", "constraint@example.com", "Constraint Test User");
            userRepository.save(testUser);
            
            Project testProject = new Project("Constraint Test Project", "For constraint test");
            projectRepository.save(testProject);
            
            // Store IDs for later use
            ids[0] = testUser.getId();
            ids[1] = testProject.getId();
            
            UserProjectRole testRole = new UserProjectRole(testUser, testProject, "CONSTRAINT_TEST");
            userProjectRoleRepository.save(testRole);
            
            return null;
        });
        
        // Step 2: Verify constraint violation in a new transaction
        try {
            txTemplate.execute(status -> {
                // Try to delete the user directly - should fail with constraint violation
                User user = userRepository.findById(ids[0]).orElseThrow();
                userRepository.delete(user);
                return null;
            });
            fail("Expected a constraint violation");
        } catch (Exception e) {
            // Verify it's a constraint violation
            assertThat(e.getMessage()).contains("constraint");
        }
        
        // Step 3: Clean up properly in another transaction
        txTemplate.execute(status -> {
            User user = userRepository.findById(ids[0]).orElseThrow();
            Project project = projectRepository.findById(ids[1]).orElseThrow();
            UserProjectRole.UserProjectRoleId roleId = new UserProjectRoleId(user.getId(), project.getId());
            
            // Delete in correct order
            if (userProjectRoleRepository.existsById(roleId)) {
                UserProjectRole role = userProjectRoleRepository.findById(roleId).orElseThrow();
                userProjectRoleRepository.delete(role);
            }
            
            userRepository.delete(user);
            projectRepository.delete(project);
            
            return null;
        });
    }

}