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
import org.testcontainers.containers.CockroachContainer;
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
@ContextConfiguration(initializers = AdvancedSqlCockroachDBTests.DataSourceInitializer.class)
public class AdvancedSqlCockroachDBTests {

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
    public void diagnosticArrayTest() {
        // Create a ticket with a known array
        Ticket testTicket = new Ticket("Array Test", "Testing arrays", user1, project1);
        testTicket.setTags(new String[]{"test", "array", "diagnostic"});
        entityManager.persist(testTicket);
        entityManager.flush();
        
        Long ticketId = testTicket.getId();
        
        // Test 1: Simple array selection
        try {
            List<Object[]> result1 = jdbcTemplate.query(
                "SELECT id, tags FROM tickets WHERE id = ?",
                (rs, rowNum) -> {
                    Object id = rs.getObject(1);
                    Object tags = rs.getArray(2);
                    return new Object[]{id, tags};
                },
                ticketId
            );
            System.out.println("Test 1 - Raw array: " + (result1.get(0)[1] != null ? 
                result1.get(0)[1].getClass().getName() : "null"));
        } catch (Exception e) {
            System.out.println("Test 1 failed: " + e.getClass().getName() + ": " + e.getMessage());
        }
        
        // Test 2: Array with ANY operator
        try {
            List<Long> result2 = jdbcTemplate.queryForList(
                "SELECT id FROM tickets WHERE 'test' = ANY(tags)",
                Long.class
            );
            System.out.println("Test 2 - ANY operator: Found " + result2.size() + " results");
        } catch (Exception e) {
            System.out.println("Test 2 failed: " + e.getClass().getName() + ": " + e.getMessage());
        }
        
        // Test 3: Array length function
        try {
            Integer length = jdbcTemplate.queryForObject(
                "SELECT array_length(tags, 1) FROM tickets WHERE id = ?",
                Integer.class,
                ticketId
            );
            System.out.println("Test 3 - array_length: " + length);
        } catch (Exception e) {
            System.out.println("Test 3 failed: " + e.getClass().getName() + ": " + e.getMessage());
        }
        
        // Test 4: Combination that fails
        try {
            List<Object[]> result4 = jdbcTemplate.query(
                "SELECT id, tags, array_length(tags, 1) FROM tickets WHERE 'test' = ANY(tags)",
                (rs, rowNum) -> {
                    Object id = rs.getObject(1);
                    Object tags = rs.getArray(2);
                    Object length = rs.getObject(3);
                    return new Object[]{id, tags, length};
                }
            );
            System.out.println("Test 4 - Combined query: " + result4.size() + " results");
        } catch (Exception e) {
            System.out.println("Test 4 failed: " + e.getClass().getName() + ": " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    // TOIL - did not work - serialization error
    // @Test
    // public void testArrayFunctions() {
    //     List<Object[]> sqlTaggedTickets = ticketRepository.findTicketsByTagWithArrayFunctions("sql");
        
    //     assertThat(sqlTaggedTickets).hasSize(2);
        
    //     // Handle potential type differences between databases
    //     Object tagCount1 = sqlTaggedTickets.get(0)[3];
    //     Object tagCount2 = sqlTaggedTickets.get(1)[3];
        
    //     if (tagCount1 instanceof Long) {
    //         assertThat(((Long)tagCount1).intValue()).isEqualTo(3);
    //         assertThat(((Long)tagCount2).intValue()).isEqualTo(2);
    //     } else {
    //         assertThat(((Number)tagCount1).intValue()).isEqualTo(3);
    //         assertThat(((Number)tagCount2).intValue()).isEqualTo(2);
    //     }
    // }

    @Test
    public void testArrayFunctions() {
        // Step 1: Find tickets with "sql" tag
        List<Object[]> sqlTaggedTickets = ticketRepository.findTicketIdsByTag("sql");
        assertThat(sqlTaggedTickets).hasSize(2);
        
        // Step 2: Verify basic ticket properties
        String firstTicketTitle = (String) sqlTaggedTickets.get(0)[1];
        String secondTicketTitle = (String) sqlTaggedTickets.get(1)[1];
        
        assertThat(firstTicketTitle).isEqualTo("Base Ticket");
        assertThat(secondTicketTitle).isEqualTo("Dependent Ticket");
        
        // Step 3: Get tag counts for each ticket
        Long ticket1Id = (Long) sqlTaggedTickets.get(0)[0];
        Long ticket2Id = (Long) sqlTaggedTickets.get(1)[0];
        
        Integer tagCount1 = ticketRepository.getTagCountForTicket(ticket1Id);
        Integer tagCount2 = ticketRepository.getTagCountForTicket(ticket2Id);
        
        // Step 4: Verify counts
        assertThat(tagCount1).isEqualTo(3); // First ticket has 3 tags
        assertThat(tagCount2).isEqualTo(2); // Second ticket has 2 tags
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