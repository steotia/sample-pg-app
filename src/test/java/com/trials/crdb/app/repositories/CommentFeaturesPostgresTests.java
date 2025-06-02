package com.trials.crdb.app.repositories;

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
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
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
@ContextConfiguration(initializers = CommentFeaturesPostgresTests.DataSourceInitializer.class)
public class CommentFeaturesPostgresTests extends TimeBasedTest {

    private PostgresCompatibilityInspector schemaInspector;
    
    // Test data references
    private User user1, user2, user3;
    private Project project1;
    private Ticket ticket1, ticket2;

    @BeforeEach
    void setUp() {
        super.setupTime();
        
        schemaInspector = new PostgresCompatibilityInspector(jdbcTemplate, 
            PostgresCompatibilityInspector.DatabaseType.POSTGRESQL);
        
        createTestData();
    }

    @Container
    static final PostgreSQLContainer<?> postgresContainer = 
        new PostgreSQLContainer<>(DockerImageName.parse("postgres:15-alpine"))
            .withDatabaseName("test_comment_postgres")
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
    private CommentRepository commentRepository;
    
    @Autowired
    private TicketRepository ticketRepository;
    
    @Autowired
    private UserRepository userRepository;
    
    @Autowired
    private ProjectRepository projectRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;
    
    private void createTestData() {
        // Clean up existing data
        commentRepository.deleteAll();
        ticketRepository.deleteAll();
        userRepository.deleteAll();
        projectRepository.deleteAll();
        
        // Create users
        user1 = new User("alice", "alice@example.com", "Alice Cooper");
        user2 = new User("bob", "bob@example.com", "Bob Smith");
        user3 = new User("charlie", "charlie@example.com", "Charlie Brown");
        entityManager.persist(user1);
        entityManager.persist(user2);
        entityManager.persist(user3);
        
        // Create project
        project1 = new Project("Comment Test Project", "Testing comment functionality");
        entityManager.persist(project1);
        
        // Create tickets
        ticket1 = new Ticket("Bug Fix", "Fix critical bug in some system", user1, project1);
        ticket2 = new Ticket("Feature Request", "Add user profile management", user2, project1);
        entityManager.persist(ticket1);
        entityManager.persist(ticket2);
        
        entityManager.flush();
    }

    @Test
    public void inspectCommentSchema() {
        schemaInspector.inspectTableSchema("comments");
    }

    @Test
    public void testCreateAndRetrieveComments() {
        // Create comments
        Comment comment1 = new Comment("This is a critical issue that needs immediate attention.", ticket1, user2);
        Comment comment2 = new Comment("I agree, we should prioritize this bug fix.", ticket1, user3);
        Comment comment3 = new Comment("This feature would be really useful for our users.", ticket2, user1);
        
        commentRepository.save(comment1);
        commentRepository.save(comment2);
        commentRepository.save(comment3);
        
        // Test finding comments by ticket
        List<Comment> ticket1Comments = commentRepository.findByTicket(ticket1);
        assertThat(ticket1Comments).hasSize(2);
        assertThat(ticket1Comments).extracting("content")
            .containsExactlyInAnyOrder(
                "This is a critical issue that needs immediate attention.",
                "I agree, we should prioritize this bug fix."
            );
            
        // Test finding comments by commenter
        List<Comment> user2Comments = commentRepository.findByCommenter(user2);
        assertThat(user2Comments).hasSize(1);
        assertThat(user2Comments.get(0).getContent()).isEqualTo("This is a critical issue that needs immediate attention.");
    }

    @Test
    public void testTextSearchInComments() {
        // Create comments with different content
        Comment comment1 = new Comment("This bug is critical and affects payment processing.", ticket1, user1);
        Comment comment2 = new Comment("The user interface needs improvement for better usability.", ticket2, user2);
        Comment comment3 = new Comment("Critical security vulnerability found in authentication module.", ticket1, user3);
        Comment comment4 = new Comment("Great suggestion! This would improve user experience significantly.", ticket2, user1);
        
        commentRepository.save(comment1);
        commentRepository.save(comment2);
        commentRepository.save(comment3);
        commentRepository.save(comment4);
        
        // Test basic text search
        List<Comment> criticalComments = commentRepository.findByContentContaining("critical");
        assertThat(criticalComments).hasSize(1);
        
        // Test case-insensitive search - should find "user" in comments 2 and 4
        List<Comment> userComments = commentRepository.findByContentContainingIgnoreCase("USER");
        assertThat(userComments).hasSize(2); // Changed from 3 to 2
        
        // Test search across comment and ticket content
        List<Comment> paymentRelated = commentRepository.findByContentOrTicketContaining("payment");
        assertThat(paymentRelated).hasSize(1); // Should find comment mentioning payment processing
    }

    @Test
    public void testCommentCounts() {
        // Create multiple comments
        Comment comment1 = new Comment("First comment on ticket1", ticket1, user1);
        Comment comment2 = new Comment("Second comment on ticket1", ticket1, user2);
        Comment comment3 = new Comment("Third comment on ticket1", ticket1, user3);
        Comment comment4 = new Comment("First comment on ticket2", ticket2, user1);
        Comment comment5 = new Comment("Another comment from user1", ticket1, user1);
        
        commentRepository.saveAll(List.of(comment1, comment2, comment3, comment4, comment5));
        
        // Test count by ticket
        Long ticket1CommentCount = commentRepository.countByTicket(ticket1);
        assertThat(ticket1CommentCount).isEqualTo(4);
        
        Long ticket2CommentCount = commentRepository.countByTicket(ticket2);
        assertThat(ticket2CommentCount).isEqualTo(1);
        
        // Test count by commenter
        Long user1CommentCount = commentRepository.countByCommenter(user1);
        assertThat(user1CommentCount).isEqualTo(3);
    }

    @Test
    public void testRecentComments() {
        // Clean existing comments
        jdbcTemplate.execute("DELETE FROM comments");
        
        // Create comments with specific timestamps using JDBC
        ZonedDateTime twoDaysAgo = baseTime.minusDays(2);
        ZonedDateTime oneDayAgo = baseTime.minusDays(1);
        ZonedDateTime today = baseTime;
        
        jdbcTemplate.update(
            "INSERT INTO comments (content, ticket_id, commenter_id, create_time) VALUES (?, ?, ?, ?)",
            "Old comment", ticket1.getId(), user1.getId(),
            java.sql.Timestamp.from(twoDaysAgo.toInstant())
        );
        
        jdbcTemplate.update(
            "INSERT INTO comments (content, ticket_id, commenter_id, create_time) VALUES (?, ?, ?, ?)",
            "Yesterday's comment", ticket2.getId(), user2.getId(),
            java.sql.Timestamp.from(oneDayAgo.toInstant())
        );
        
        jdbcTemplate.update(
            "INSERT INTO comments (content, ticket_id, commenter_id, create_time) VALUES (?, ?, ?, ?)",
            "Today's comment", ticket1.getId(), user3.getId(),
            java.sql.Timestamp.from(today.toInstant())
        );
        
        // Test recent comments (ordered by creation time desc)
        Page<Comment> recentCommentsPage = commentRepository.findRecentComments(PageRequest.of(0, 2));
        List<Comment> recentComments = recentCommentsPage.getContent();
        
        assertThat(recentComments).hasSize(2);
        assertThat(recentComments.get(0).getContent()).isEqualTo("Today's comment");
        assertThat(recentComments.get(1).getContent()).isEqualTo("Yesterday's comment");
        
        // Test comments created after a specific date (should find only "Today's comment")
        List<Comment> commentsSinceYesterday = commentRepository.findCommentsCreatedAfter(baseTime.minusDays(1));
        assertThat(commentsSinceYesterday).hasSize(1);
        assertThat(commentsSinceYesterday.get(0).getContent()).isEqualTo("Today's comment");
    }

    @Test 
    public void testCommentAnalytics() {
        // Create comments of varying lengths
        Comment shortComment = new Comment("OK", ticket1, user1);
        Comment mediumComment = new Comment("This is a medium length comment with some detailed explanation.", ticket1, user2);
        Comment longComment = new Comment("This is a very long comment that contains a lot of detailed information about the issue, including background context, potential solutions, and implementation considerations that need to be taken into account.", ticket2, user3);
        
        commentRepository.saveAll(List.of(shortComment, mediumComment, longComment));
        
        // Test finding long comments
        List<Comment> longComments = commentRepository.findLongComments(50);
        assertThat(longComments).hasSize(2); // medium and long comments
        
        // Test average comment length
        Double avgLength = commentRepository.getAverageCommentLength();
        assertThat(avgLength).isNotNull();
        assertThat(avgLength).isGreaterThan(0);
        
        // Test top commenters
        // Add more comments to create different comment counts
        commentRepository.save(new Comment("Another comment from user1", ticket1, user1));
        commentRepository.save(new Comment("Yet another comment from user1", ticket2, user1));
        
        List<Object[]> topCommenters = commentRepository.findTopCommenters(PageRequest.of(0, 3));
        assertThat(topCommenters).hasSize(3);
        
        // user1 should be the top commenter with 3 comments
        Object[] topCommenter = topCommenters.get(0);
        assertThat(((User) topCommenter[0]).getUsername()).isEqualTo("alice");
        assertThat(((Long) topCommenter[1])).isEqualTo(3L);
    }

    @Test
    public void testPaginatedComments() {
        // Create many comments for pagination testing
        for (int i = 1; i <= 10; i++) {
            Comment comment = new Comment("Comment number " + i, ticket1, user1);
            commentRepository.save(comment);
        }
        
        // Test paginated retrieval
        Page<Comment> firstPage = commentRepository.findByTicket(ticket1, PageRequest.of(0, 3));
        assertThat(firstPage.getTotalElements()).isEqualTo(10);
        assertThat(firstPage.getTotalPages()).isEqualTo(4);
        assertThat(firstPage.getContent()).hasSize(3);
        
        Page<Comment> secondPage = commentRepository.findByTicket(ticket1, PageRequest.of(1, 3));
        assertThat(secondPage.getContent()).hasSize(3);
        assertThat(secondPage.isFirst()).isFalse();
        assertThat(secondPage.isLast()).isFalse();
    }
}