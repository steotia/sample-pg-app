package com.trials.crdb.app.transaction;

import java.util.ArrayList;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.ArrayList;

import com.trials.crdb.app.model.*;
import com.trials.crdb.app.repositories.*;

public abstract class TransactionTestBase {
    
    @Autowired
    protected TestEntityManager entityManager;
    
    @Autowired
    protected TicketRepository ticketRepository;
    
    @Autowired
    protected UserRepository userRepository;
    
    @Autowired
    protected ProjectRepository projectRepository;
    
    @Autowired
    protected JdbcTemplate jdbcTemplate;
    
    @Autowired
    protected TransactionTemplate transactionTemplate;
    
    // Test data
    protected User user1;
    protected User user2;
    protected Project project1;
    protected Ticket testTicket;
    
    @BeforeEach
    void setupBaseData() {
        // Clean up
        // ticketRepository.deleteAll();
        // userRepository.deleteAll();
        // projectRepository.deleteAll();
        cleanDatabase();
        
        // Create test data
        user1 = new User("john", "john@example.com", "John Doe");
        user2 = new User("jane", "jane@example.com", "Jane Doe");
        project1 = new Project("Test Project", "Transaction Testing");
        
        entityManager.persist(user1);
        entityManager.persist(user2);
        entityManager.persist(project1);
        
        testTicket = new Ticket("Test Ticket", "For transaction testing", user1, project1);
        testTicket = ticketRepository.save(testTicket);
        
        entityManager.flush();
        entityManager.clear(); // Clear persistence context
    }

    // @BeforeEach
    // void setUp() {
    //     // Clean up any existing data
    //     cleanDatabase();
    // }

    @AfterEach
    void tearDown() {
        // Clean up after each test
        cleanDatabase();
    }

    private void cleanDatabase() {
        // Delete in correct order to avoid FK constraints
        ticketRepository.deleteAll();
        userRepository.deleteAll(); 
        projectRepository.deleteAll();
        entityManager.flush();
    }

    protected Ticket createTestTicket() {
        // Create a user first (ticket needs reporter)
        User reporter = new User(
            "testuser_" + System.currentTimeMillis(), 
            "test@example.com", 
            "Test User"
        );
        entityManager.persist(reporter);
        
        // Create a project (ticket needs project)
        Project project = new Project(
            "Test Project " + System.currentTimeMillis(), 
            "Test project for transaction tests"
        );
        entityManager.persist(project);
        
        // Flush to ensure IDs are generated
        entityManager.flush();
        
        // Create the ticket
        Ticket ticket = new Ticket(
            "Test Ticket", 
            "Test ticket for optimistic locking", 
            reporter, 
            project
        );
        
        // Set some additional fields to test updates
        ticket.setPriority(Ticket.TicketPriority.MEDIUM);
        ticket.setStatus(Ticket.TicketStatus.OPEN);
        
        return ticket;
    }

    // Also add a helper to create and save a ticket
    protected Ticket createAndSaveTicket() {
        Ticket ticket = createTestTicket();
        Ticket saved = ticketRepository.save(ticket);
        entityManager.flush();
        return saved;
    }

    // Add a helper to create multiple tickets
    protected List<Ticket> createMultipleTickets(int count) {
        List<Ticket> tickets = new ArrayList<>();
        User reporter = new User("bulk_user", "bulk@example.com", "Bulk User");
        entityManager.persist(reporter);
        
        Project project = new Project("Bulk Project", "Project for bulk tests");
        entityManager.persist(project);
        
        entityManager.flush();
        
        for (int i = 0; i < count; i++) {
            Ticket ticket = new Ticket(
                "Ticket " + i,
                "Description " + i,
                reporter,
                project
            );
            tickets.add(ticket);
        }
        
        return tickets;
    }
    
    protected void logTestContext(String testName) {
        System.out.println("\n=== " + testName + " ===");
        System.out.println("Database: " + getDatabaseType());
        System.out.println("Initial ticket version: " + testTicket.getVersion());
    }
    
    protected abstract String getDatabaseType();
}