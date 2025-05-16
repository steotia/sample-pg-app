package com.trials.crdb.app.repositories;

import java.util.*;
import java.time.ZonedDateTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.lang.NonNull;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.trials.crdb.app.model.*;
import com.trials.crdb.app.model.Ticket.TicketPriority;
import com.trials.crdb.app.model.Ticket.TicketStatus;
import com.trials.crdb.app.utils.PostgresCompatibilityInspector;

import org.springframework.core.env.MapPropertySource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ContextConfiguration(initializers = TicketAdvancedQueriesCockroachDBTests.DataSourceInitializer.class)
public class TicketAdvancedQueriesCockroachDBTests {

    private PostgresCompatibilityInspector schemaInspector;
    
    // Test data references
    private User user1, user2, user3;
    private Project project1, project2;
    private Ticket ticket1, ticket2, ticket3, ticket4, ticket5;

    @BeforeEach
    void setUp() {
        schemaInspector = new PostgresCompatibilityInspector(jdbcTemplate, 
            PostgresCompatibilityInspector.DatabaseType.COCKROACHDB);
        
        // Set up test data
        createTestData();
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
    private UserRepository userRepository;
    
    @Autowired
    private ProjectRepository projectRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;
    
    /**
     * Set up common test data for all tests
     */
    private void createTestData() {
        // Clean up existing data if needed
        ticketRepository.deleteAll();
        userRepository.deleteAll();
        projectRepository.deleteAll();
        
        // Create users
        user1 = new User("john", "john@example.com", "John Smith");
        user2 = new User("jane", "jane@example.com", "Jane Doe");
        user3 = new User("bob", "bob@example.com", "Bob Johnson");
        entityManager.persist(user1);
        entityManager.persist(user2);
        entityManager.persist(user3);
        
        // Create projects
        project1 = new Project("Website Redesign", "Redesign the company website");
        project2 = new Project("Mobile App", "Develop a new mobile app");
        entityManager.persist(project1);
        entityManager.persist(project2);
        
        // Create tickets with different statuses, priorities, and metadata
        ticket1 = new Ticket("Homepage Layout", "Redesign the homepage layout", user1, project1);
        ticket1.setStatus(Ticket.TicketStatus.IN_PROGRESS);
        ticket1.setPriority(Ticket.TicketPriority.HIGH);
        ticket1.assignTo(user2);
        Map<String, Object> metadata1 = new HashMap<>();
        metadata1.put("component", "frontend");
        metadata1.put("complexity", 3);
        metadata1.put("tags", Arrays.asList("design", "ui"));
        ticket1.setMetadata(metadata1);
        
        ticket2 = new Ticket("API Integration", "Integrate with payment API", user1, project1);
        ticket2.setStatus(Ticket.TicketStatus.OPEN);
        ticket2.setPriority(Ticket.TicketPriority.MEDIUM);
        Map<String, Object> metadata2 = new HashMap<>();
        metadata2.put("component", "backend");
        metadata2.put("complexity", 4);
        metadata2.put("tags", Arrays.asList("api", "payment"));
        ticket2.setMetadata(metadata2);
        
        ticket3 = new Ticket("Mobile Navigation", "Implement mobile navigation menu", user2, project2);
        ticket3.setStatus(Ticket.TicketStatus.REVIEW);
        ticket3.setPriority(Ticket.TicketPriority.MEDIUM);
        ticket3.assignTo(user3);
        Map<String, Object> metadata3 = new HashMap<>();
        metadata3.put("component", "frontend");
        metadata3.put("complexity", 2);
        metadata3.put("platform", "iOS");
        metadata3.put("tags", Arrays.asList("navigation", "mobile"));
        ticket3.setMetadata(metadata3);
        
        ticket4 = new Ticket("Database Optimization", "Optimize database queries", user3, project1);
        ticket4.setStatus(Ticket.TicketStatus.OPEN);
        ticket4.setPriority(Ticket.TicketPriority.CRITICAL);
        Map<String, Object> metadata4 = new HashMap<>();
        metadata4.put("component", "backend");
        metadata4.put("complexity", 5);
        metadata4.put("tags", Arrays.asList("database", "performance"));
        ticket4.setMetadata(metadata4);
        
        ticket5 = new Ticket("User Authentication", "Implement user authentication", user1, project2);
        ticket5.setStatus(Ticket.TicketStatus.CLOSED);
        ticket5.setPriority(Ticket.TicketPriority.HIGH);
        ticket5.assignTo(user1);
        Map<String, Object> metadata5 = new HashMap<>();
        metadata5.put("component", "backend");
        metadata5.put("complexity", 4);
        metadata5.put("tags", Arrays.asList("security", "auth"));
        ticket5.setMetadata(metadata5);
        
        // Persist all tickets
        entityManager.persist(ticket1);
        entityManager.persist(ticket2);
        entityManager.persist(ticket3);
        entityManager.persist(ticket4);
        entityManager.persist(ticket5);
        
        // Flush to ensure all entities are saved
        entityManager.flush();
    }

    //-------------------------------------------------------------------------
    // SECTION 1: BASIC FILTERING TESTS
    //-------------------------------------------------------------------------
    
    @Test
    public void testFilterByStatus() {
        // Test finding tickets by status
        List<Ticket> openTickets = ticketRepository.findByStatus(Ticket.TicketStatus.OPEN);
        assertThat(openTickets).hasSize(2);
        assertThat(openTickets).extracting("title")
            .containsExactlyInAnyOrder("API Integration", "Database Optimization");
    }
    
    @Test
    public void testFilterByPriority() {
        // Test finding tickets by priority
        List<Ticket> highPriorityTickets = ticketRepository.findByPriority(Ticket.TicketPriority.HIGH);
        assertThat(highPriorityTickets).hasSize(2);
        assertThat(highPriorityTickets).extracting("title")
            .containsExactlyInAnyOrder("Homepage Layout", "User Authentication");
    }
    
    @Test
    public void testFilterByStatusAndPriority() {
        // Test combined filters
        List<Ticket> openCriticalTickets = ticketRepository.findByStatusAndPriority(
            Ticket.TicketStatus.OPEN, Ticket.TicketPriority.CRITICAL);
        assertThat(openCriticalTickets).hasSize(1);
        assertThat(openCriticalTickets.get(0).getTitle()).isEqualTo("Database Optimization");
    }

    //-------------------------------------------------------------------------
    // SECTION 2: RELATIONSHIP QUERY TESTS
    //-------------------------------------------------------------------------
    
    @Test
    public void testFindByAssignee() {
        // Test finding tickets assigned to a specific user
        List<Ticket> user2Tickets = ticketRepository.findByAssignee(user2);
        assertThat(user2Tickets).hasSize(1);
        assertThat(user2Tickets.get(0).getTitle()).isEqualTo("Homepage Layout");
    }
    
    @Test
    public void testFindByReporter() {
        // Test finding tickets reported by a specific user
        List<Ticket> user1ReportedTickets = ticketRepository.findByReporter(user1);
        assertThat(user1ReportedTickets).hasSize(3);
        assertThat(user1ReportedTickets).extracting("title")
            .containsExactlyInAnyOrder("Homepage Layout", "API Integration", "User Authentication");
    }
    
    @Test
    public void testFindByProject() {
        // Test finding tickets for a specific project
        List<Ticket> project2Tickets = ticketRepository.findByProject(project2);
        assertThat(project2Tickets).hasSize(2);
        assertThat(project2Tickets).extracting("title")
            .containsExactlyInAnyOrder("Mobile Navigation", "User Authentication");
    }
    
    @Test
    public void testFindUnassignedTickets() {
        // Test finding tickets without an assignee
        List<Ticket> unassignedTickets = ticketRepository.findByAssigneeIsNull();
        assertThat(unassignedTickets).hasSize(2);
        assertThat(unassignedTickets).extracting("title")
            .containsExactlyInAnyOrder("API Integration", "Database Optimization");
    }

    //-------------------------------------------------------------------------
    // SECTION 3: TEXT SEARCH TESTS 
    //-------------------------------------------------------------------------

    @Test
    public void testFindByKeyword() {
        // Test basic text search
        List<Ticket> apiTickets = ticketRepository.findByKeyword("API");
        assertThat(apiTickets).hasSize(1);
        assertThat(apiTickets.get(0).getTitle()).isEqualTo("API Integration");
        
        List<Ticket> mobileTickets = ticketRepository.findByKeyword("mobile");
        assertThat(mobileTickets).hasSize(1);
        assertThat(mobileTickets.get(0).getTitle()).isEqualTo("Mobile Navigation");
    }

    @Test
    public void testFindByKeywordCaseInsensitive() {
        // Test case-insensitive search using PostgreSQL ILIKE
        List<Ticket> apiTickets = ticketRepository.findByKeywordCaseInsensitive("api");
        assertThat(apiTickets).hasSize(1);
        assertThat(apiTickets.get(0).getTitle()).isEqualTo("API Integration");
        
        List<Ticket> mobileTickets = ticketRepository.findByKeywordCaseInsensitive("MOBILE");
        assertThat(mobileTickets).hasSize(1);
        assertThat(mobileTickets.get(0).getTitle()).isEqualTo("Mobile Navigation");
    }

    @Test
    public void testFindByMultipleKeywords() {
        // Test searching with multiple keywords using PostgreSQL ARRAY and ALL construct
        List<Ticket> tickets = ticketRepository.findByMultipleKeywords("mobile", "navigation");
        assertThat(tickets).hasSize(1);
        assertThat(tickets.get(0).getTitle()).isEqualTo("Mobile Navigation");
        
        // Test with keywords that won't match together
        List<Ticket> noMatches = ticketRepository.findByMultipleKeywords("mobile", "payment");
        assertThat(noMatches).isEmpty();
    }

    @Test
    public void testFindByPrefix() {
        // Test prefix search with PostgreSQL ILIKE
        List<Ticket> tickets = ticketRepository.findByPrefix("datab");
        assertThat(tickets).hasSize(1);
        assertThat(tickets.get(0).getTitle()).isEqualTo("Database Optimization");
        
        // Test case-insensitive prefix
        List<Ticket> implTickets = ticketRepository.findByPrefix("impl");
        assertThat(implTickets).hasSize(2);
        assertThat(implTickets).extracting("title")
            .containsExactlyInAnyOrder("Mobile Navigation", "User Authentication");
    }

    @Test
    public void testFindByKeywordAcrossFields() {
        // Test searching across multiple fields including enum values
        List<Ticket> highTickets = ticketRepository.findByKeywordAcrossFields("high");
        assertThat(highTickets).hasSize(2);
        assertThat(highTickets).extracting("title")
            .containsExactlyInAnyOrder("Homepage Layout", "User Authentication");
        
        // Search for status values
        List<Ticket> openTickets = ticketRepository.findByKeywordAcrossFields("open");
        assertThat(openTickets).hasSize(2);
    }

    @Test
    public void testAdvancedSearch() {
        // Test searching with multiple criteria
        List<Ticket> tickets = ticketRepository.findByAdvancedCriteria("api", null, TicketStatus.OPEN, null);
        assertThat(tickets).hasSize(1);
        assertThat(tickets.get(0).getTitle()).isEqualTo("API Integration");
        
        // Test search with only priority
        List<Ticket> highTickets = ticketRepository.findByAdvancedCriteria(null, null, null, TicketPriority.HIGH);
        assertThat(highTickets).hasSize(2);
        
        // Test search with multiple criteria
        List<Ticket> complexTickets = ticketRepository.findByAdvancedCriteria(null, "implement", null, TicketPriority.HIGH);
        assertThat(complexTickets).hasSize(1);
        assertThat(complexTickets.get(0).getTitle()).isEqualTo("User Authentication");
    }

   //-------------------------------------------------------------------------
    // SECTION 4: JSON/JSONB QUERY TESTS
    //-------------------------------------------------------------------------

    @Test
    public void testJsonbFieldQuery() {
        // Test querying by a JSON field value
        List<Ticket> frontendTickets = ticketRepository.findByMetadataKeyValue("component", "frontend");
        assertThat(frontendTickets).hasSize(2);
        assertThat(frontendTickets).extracting("title")
            .containsExactlyInAnyOrder("Homepage Layout", "Mobile Navigation");
    }

    @Test
    public void testJsonbComplexityQuery() {
        // Test querying with numeric comparison on JSON value
        List<Ticket> complexTickets = ticketRepository.findByComplexityGreaterThan(3);
        assertThat(complexTickets).hasSize(3);
        assertThat(complexTickets).extracting("title")
            .containsExactlyInAnyOrder("API Integration", "Database Optimization", "User Authentication");
    }

    @Test
    public void testJsonbArrayQuery() {
        // Test querying JSON arrays for specific values
        List<Ticket> designTickets = ticketRepository.findByTag("design");
        assertThat(designTickets).hasSize(1);
        assertThat(designTickets.get(0).getTitle()).isEqualTo("Homepage Layout");
        
        List<Ticket> securityTickets = ticketRepository.findByTag("security");
        assertThat(securityTickets).hasSize(1);
        assertThat(securityTickets.get(0).getTitle()).isEqualTo("User Authentication");
    }

    @Test
    public void testJsonbContainsKeys() {
        // Test querying for tickets with specific keys in metadata
        List<Ticket> tickets = ticketRepository.findByMetadataContainingKeys("component", "complexity");
        assertThat(tickets).hasSize(5); // All tickets have both keys
        
        // Test with a key that only some tickets have
        List<Ticket> platformTickets = ticketRepository.findByMetadataContainingKeys("component", "platform");
        assertThat(platformTickets).hasSize(1);
        assertThat(platformTickets.get(0).getTitle()).isEqualTo("Mobile Navigation");
    }

    @Test
    public void testJsonbContainment() {

        List<Object[]> values = ticketRepository.dumpMetadataValues("component");
        for (Object[] row : values) {
            System.out.println("Ticket ID: " + row[0] + ", component: " + row[1]);
        }

        // Test JSON containment operator
        List<Ticket> tickets = ticketRepository.findByMetadataContaining("component", "\"frontend\"");
        assertThat(tickets).hasSize(2);
        assertThat(tickets).extracting("title")
            .containsExactlyInAnyOrder("Homepage Layout", "Mobile Navigation");
    }

    //-------------------------------------------------------------------------
    // SECTION 5: PAGINATION AND SORTING TESTS
    //-------------------------------------------------------------------------
    
    // Add this method to TicketRepository:
    // Page<Ticket> findByProject(Project project, Pageable pageable);
    
    /*
    @Test
    public void testPagination() {
        // Test pagination with Spring Data
        Page<Ticket> firstPage = ticketRepository.findByProject(
            project1, PageRequest.of(0, 2));
        assertThat(firstPage.getTotalElements()).isEqualTo(3);
        assertThat(firstPage.getContent()).hasSize(2);
        assertThat(firstPage.getTotalPages()).isEqualTo(2);
        
        Page<Ticket> secondPage = ticketRepository.findByProject(
            project1, PageRequest.of(1, 2));
        assertThat(secondPage.getContent()).hasSize(1);
    }
    
    @Test
    public void testSorting() {
        // Test sorting with Spring Data
        List<Ticket> sortedByPriority = ticketRepository.findAll(
            Sort.by(Sort.Direction.DESC, "priority"));
        assertThat(sortedByPriority).hasSize(5);
        assertThat(sortedByPriority.get(0).getPriority()).isEqualTo(Ticket.TicketPriority.CRITICAL);
        assertThat(sortedByPriority.get(0).getTitle()).isEqualTo("Database Optimization");
    }
    
    @Test
    public void testPaginationAndSorting() {
        // Test pagination combined with sorting
        Page<Ticket> page = ticketRepository.findAll(
            PageRequest.of(0, 3, Sort.by(Sort.Direction.ASC, "title")));
        assertThat(page.getContent()).hasSize(3);
        assertThat(page.getContent().get(0).getTitle()).isEqualTo("API Integration");
    }
    */

    //-------------------------------------------------------------------------
    // SECTION 6: COUNT AND AGGREGATION TESTS
    //-------------------------------------------------------------------------
    
    @Test
    public void testCountByStatus() {
        // Test counting by status
        Long openCount = ticketRepository.countByStatus(Ticket.TicketStatus.OPEN);
        assertThat(openCount).isEqualTo(2);
    }
    
    @Test
    public void testCountByProjectAndStatus() {
        // Test counting by project and status
        Long project1OpenCount = ticketRepository.countByProjectAndStatus(project1, Ticket.TicketStatus.OPEN);
        assertThat(project1OpenCount).isEqualTo(2);
        
        Long project2ClosedCount = ticketRepository.countByProjectAndStatus(project2, Ticket.TicketStatus.CLOSED);
        assertThat(project2ClosedCount).isEqualTo(1);
    }
    
    // Add this method to TicketRepository:
    // @Query("SELECT t.priority, COUNT(t) FROM Ticket t GROUP BY t.priority")
    // List<Object[]> countByPriority();
    
    /*
    @Test
    public void testCountGroupByPriority() {
        // Add custom repository method first
        List<Object[]> priorityCounts = ticketRepository.countByPriority();
        assertThat(priorityCounts).hasSize(3); // LOW, MEDIUM, HIGH, CRITICAL (but we only used 3)
        
        Map<Ticket.TicketPriority, Long> countMap = new HashMap<>();
        for (Object[] result : priorityCounts) {
            countMap.put((Ticket.TicketPriority) result[0], (Long) result[1]);
        }
        
        assertThat(countMap.get(Ticket.TicketPriority.MEDIUM)).isEqualTo(2L);
        assertThat(countMap.get(Ticket.TicketPriority.HIGH)).isEqualTo(2L);
    }
    */

    //-------------------------------------------------------------------------
    // SECTION 7: NATIVE QUERY TESTS 
    //-------------------------------------------------------------------------
    
    // Add this method to TicketRepository:
    // @Query(value = "SELECT t.* FROM tickets t " +
    //                "JOIN users u ON t.reporter_id = u.id " +
    //                "WHERE u.username = :username AND " +
    //                "t.status NOT IN ('CLOSED', 'RESOLVED')", 
    //        nativeQuery = true)
    // List<Ticket> findActiveTicketsByReporterUsername(@Param("username") String username);
    
    /*
    @Test
    public void testNativeQuery() {
        // Test native SQL query
        List<Ticket> johnsActiveTickets = ticketRepository.findActiveTicketsByReporterUsername("john");
        assertThat(johnsActiveTickets).hasSize(2);
        assertThat(johnsActiveTickets).extracting("title")
            .containsExactlyInAnyOrder("Homepage Layout", "API Integration");
    }
    */
}