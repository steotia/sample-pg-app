package com.trials.crdb.app.repositories;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.assertj.core.data.Offset;
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

import com.trials.crdb.app.model.Project;
import com.trials.crdb.app.model.Ticket;
import com.trials.crdb.app.model.User;

import org.springframework.core.env.MapPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ContextConfiguration(initializers = AdvancedSqlPostgresTests.DataSourceInitializer.class)
public class AdvancedSqlPostgresTests {

    @Container
    static final PostgreSQLContainer<?> postgresContainer = 
        new PostgreSQLContainer<>(DockerImageName.parse("postgres:15-alpine"))
            .withDatabaseName("test_advanced_sql")
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
    
    // Test data references
    private User user1, user2;
    private Project project1, project2;
    private Ticket ticket1, ticket2, ticket3, ticket4, ticket5;
    
    @BeforeEach
    void setUp() {
        // Clean up existing data
        ticketRepository.deleteAll();
        userRepository.deleteAll();
        projectRepository.deleteAll();
        
        // Create test data
        createTestData();
    }
    
    private void createTestData() {
        // Create users
        user1 = new User("john", "john@example.com", "John Smith");
        user2 = new User("jane", "jane@example.com", "Jane Doe");
        entityManager.persist(user1);
        entityManager.persist(user2);
        
        // Create projects
        project1 = new Project("Advanced SQL Project", "Testing advanced SQL features");
        project2 = new Project("Another Project", "Second test project");
        entityManager.persist(project1);
        entityManager.persist(project2);
        
        // Create tickets with various properties for testing
        ticket1 = new Ticket("Base Ticket", "First ticket", user1, project1);
        ticket1.setEstimatedHours(10.0);
        ticket1.setTags(new String[]{"sql", "test", "important"});
        entityManager.persist(ticket1);
        
        ticket2 = new Ticket("Dependent Ticket", "Depends on ticket1", user2, project1);
        ticket2.setEstimatedHours(5.0);
        ticket2.setTags(new String[]{"sql", "dependent"});
        ticket2.setDependentOn(ticket1);
        entityManager.persist(ticket2);
        
        ticket3 = new Ticket("Critical Task", "High priority task", user1, project1);
        ticket3.setPriority(Ticket.TicketPriority.CRITICAL);
        ticket3.setEstimatedHours(8.0);
        ticket3.setTags(new String[]{"critical", "urgent"});
        entityManager.persist(ticket3);
        
        ticket4 = new Ticket("Project 2 Task", "Task for second project", user2, project2);
        ticket4.setEstimatedHours(12.0);
        ticket4.setTags(new String[]{"project2", "test"});
        entityManager.persist(ticket4);
        
        ticket5 = new Ticket("Deep Dependency", "Depends on ticket2", user1, project1);
        ticket5.setDependentOn(ticket2);
        ticket5.setEstimatedHours(3.0);
        entityManager.persist(ticket5);
        
        entityManager.flush();
        
        // Print dependencies to verify
        System.out.println("Ticket5 depends on: " + (ticket5.getDependentOn() != null ? ticket5.getDependentOn().getId() : "null"));
        System.out.println("Ticket2 depends on: " + (ticket2.getDependentOn() != null ? ticket2.getDependentOn().getId() : "null"));
    }
    
    @Test
    public void testArithmeticAndAggregation() {
        Map<String, Object> stats = ticketRepository.calculateProjectEstimationStatistics(project1.getId());
        
        assertThat(stats).isNotNull();
        assertThat(((Number) stats.get("total")).doubleValue()).isEqualTo(26.0); // 10 + 5 + 8 + 3
        assertThat(((Number) stats.get("average")).doubleValue()).isEqualTo(6.5); // (10 + 5 + 8 + 3) / 4
        assertThat(((Number) stats.get("maximum")).doubleValue()).isEqualTo(10.0);
        assertThat(((Number) stats.get("rms")).doubleValue()).isCloseTo(14.071, Offset.offset(0.1)); // sqrt(10² + 5² + 8² + 3²)
    }
    
    @Test
    public void testWindowFunctions() {
        List<Object[]> rankedTickets = ticketRepository.findTicketsWithPriorityRanking();
        
        assertThat(rankedTickets).isNotNull();
        assertThat(rankedTickets).hasSize(5);
        
        // Verify ranking - critical ticket should be first
        Object[] criticalTicket = rankedTickets.stream()
            .filter(row -> row[0].equals(ticket3.getId()))
            .findFirst()
            .orElse(null);
            
        assertThat(criticalTicket).isNotNull();
        assertThat(criticalTicket[3]).isEqualTo(1L); // Rank 1
        
        // Test LAG/LEAD functions
        List<Object[]> timeGapResults = ticketRepository.findTicketSequenceWithTimeGaps(project1.getId());
        assertThat(timeGapResults).hasSize(4); // 4 tickets in project1
        
        // First ticket should have null as previous ticket
        assertThat(timeGapResults.get(0)[3]).isNull();
    }
    
    @Test
    public void testArrayFunctions() {
        List<Object[]> sqlTaggedTickets = ticketRepository.findTicketsByTagWithArrayFunctions("sql");
        
        assertThat(sqlTaggedTickets).hasSize(2);
        
        // Handle potential type differences between databases
        Object tagCount1 = sqlTaggedTickets.get(0)[3];
        Object tagCount2 = sqlTaggedTickets.get(1)[3];
        
        if (tagCount1 instanceof Long) {
            assertThat(((Long)tagCount1).intValue()).isEqualTo(3);
            assertThat(((Long)tagCount2).intValue()).isEqualTo(2);
        } else {
            assertThat(((Number)tagCount1).intValue()).isEqualTo(3);
            assertThat(((Number)tagCount2).intValue()).isEqualTo(2);
        }
    }
    
    @Test
    public void testCommonTableExpressions() {
        // Test basic CTE
        List<Object[]> projectTickets = ticketRepository.findTicketsWithCTEByProjectName("Advanced SQL Project");
        assertThat(projectTickets).hasSize(4); // 4 tickets in project1
        
        // Test recursive CTE
        List<Object[]> dependencyChain = ticketRepository.findTicketDependencyChain(ticket5.getId());
        
        // Should have 3 levels: ticket5 -> ticket2 -> ticket1
        assertThat(dependencyChain).hasSize(3);
        
        // Dump the results for debugging
        for (Object[] row : dependencyChain) {
            System.out.println("ID: " + row[0] + ", Title: " + row[1] + ", Depth: " + row[2]);
        }
        
        // Sort by depth to ensure consistent ordering
        dependencyChain.sort(Comparator.comparing(row -> ((Number)row[2]).intValue()));
        
        // Verify each level in the chain
        assertThat(dependencyChain.get(0)[0]).isEqualTo(ticket5.getId());
        assertThat(((Number)dependencyChain.get(0)[2]).intValue()).isEqualTo(1); // Depth 1
        
        assertThat(dependencyChain.get(1)[0]).isEqualTo(ticket2.getId());
        assertThat(((Number)dependencyChain.get(1)[2]).intValue()).isEqualTo(2); // Depth 2
        
        assertThat(dependencyChain.get(2)[0]).isEqualTo(ticket1.getId());
        assertThat(((Number)dependencyChain.get(2)[2]).intValue()).isEqualTo(3); // Depth 3
    }
}