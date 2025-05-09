package com.trials.crdb.app.repositories;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.trials.crdb.app.model.Project;
import com.trials.crdb.app.model.User;
import com.trials.crdb.app.utils.PostgresCompatibilityInspector;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
public class UserProjectRelationshipSpannerTests {

    private static final String PROJECT_ID = "emulator-project";
    private static final String INSTANCE_ID = "test-instance";
    private static final String DATABASE_ID = "test-database";

    private PostgresCompatibilityInspector schemaInspector;
    
    // Create empty credentials file for test
    @BeforeAll
    public static void setupCredentials() throws IOException {
        Files.writeString(Path.of("/tmp/empty-credentials.json"), "{}");
    }

    @BeforeEach
    void setUp() {
        schemaInspector = new PostgresCompatibilityInspector(jdbcTemplate, 
            PostgresCompatibilityInspector.DatabaseType.SPANNER);
    }
    
    // Create a shared network for containers
    private static final Network NETWORK = Network.newNetwork();

    // Spanner emulator container
    @Container
    static final GenericContainer<?> spannerEmulator = 
        new GenericContainer<>("gcr.io/cloud-spanner-emulator/emulator")
            .withNetwork(NETWORK)
            .withNetworkAliases("spanner-emulator")
            .withExposedPorts(9010, 9020)
            .withStartupTimeout(Duration.ofMinutes(2));
    
    // PGAdapter container with matched configuration
    @Container
    static final GenericContainer<?> pgAdapter = 
        new GenericContainer<>("gcr.io/cloud-spanner-pg-adapter/pgadapter")
            .withNetwork(NETWORK)
            .dependsOn(spannerEmulator)
            .withExposedPorts(5432)
            .withFileSystemBind("/tmp/empty-credentials.json", "/credentials.json", BindMode.READ_ONLY)
            .withCommand(
                "-p", PROJECT_ID,
                "-i", INSTANCE_ID,
                "-d", DATABASE_ID,
                "-e", "spanner-emulator:9010",
                "-c", "/credentials.json",
                "-r", "autoConfigEmulator=true",
                "-x"
            )
            .withStartupTimeout(Duration.ofMinutes(2));

    // Use DynamicPropertySource for database configuration
    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
        registry.add("spring.datasource.url", () -> {
            String pgHost = pgAdapter.getHost();
            int pgPort = pgAdapter.getMappedPort(5432);
            return String.format("jdbc:postgresql://%s:%d/%s", pgHost, pgPort, DATABASE_ID);
        });
        registry.add("spring.datasource.username", () -> "postgres");
        registry.add("spring.datasource.password", () -> "");
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "none");
        registry.add("spring.datasource.hikari.connection-init-sql", 
            () -> "SET spanner.support_drop_cascade=true");
        registry.add("spring.jpa.properties.hibernate.dialect", 
            () -> "org.hibernate.dialect.PostgreSQLDialect");
        registry.add("spring.jpa.show-sql", () -> "true");
    }

    @Autowired
    private TestEntityManager entityManager;

    @Autowired
    private ProjectRepository projectRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setupSchema() throws SQLException {
        // Using raw JDBC connection to avoid Spring's transaction management
        try (Connection conn = dataSource.getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                // Enable Spanner specific settings
                stmt.execute("SET spanner.support_drop_cascade=true");
                
                // Drop tables if they exist
                stmt.execute("DROP TABLE IF EXISTS user_projects");
                stmt.execute("DROP TABLE IF EXISTS users");
                stmt.execute("DROP TABLE IF EXISTS projects");
                
                // Create projects table
                stmt.execute("CREATE TABLE projects (" +
                    "id BIGINT GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE)," +
                    "create_time TIMESTAMPTZ NOT NULL," +
                    "description TEXT," +
                    "name VARCHAR(255) NOT NULL," +
                    "PRIMARY KEY (id)" +
                    ")");
                
                // Create users table
                stmt.execute("CREATE TABLE users (" +
                    "id BIGINT GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE)," +
                    "create_time TIMESTAMPTZ NOT NULL," +
                    "username VARCHAR(255) NOT NULL," +
                    "email VARCHAR(255) NOT NULL," +
                    "full_name VARCHAR(255)," +
                    "PRIMARY KEY (id)" +
                    ")");
                
                // Create unique indexes (since Spanner doesn't support UNIQUE constraint)
                stmt.execute("CREATE UNIQUE INDEX uk_users_username ON users (username)");
                stmt.execute("CREATE UNIQUE INDEX uk_projects_name ON projects (name)");
                
                // Create join table
                stmt.execute("CREATE TABLE user_projects (" +
                    "user_id BIGINT NOT NULL," +
                    "project_id BIGINT NOT NULL," +
                    "PRIMARY KEY (user_id, project_id)" +
                    ")");
            }
        }
    }

    @Test
    public void whenAssignUserToProject_thenBidirectionalRelationshipIsEstablished() {
        // Create a project
        Project project = new Project("Spanner Project", "A project for testing user assignments in Spanner");
        entityManager.persist(project);
        
        // Create a user
        User user = new User("spanneruser", "spanner@example.com", "Spanner User");
        entityManager.persist(user);
        
        // Associate user with project
        user.addProject(project);
        entityManager.flush();
        
        // Verify relationship was established
        User foundUser = entityManager.find(User.class, user.getId());
        Project foundProject = entityManager.find(Project.class, project.getId());
        
        assertThat(foundUser.getProjects()).contains(foundProject);
        assertThat(foundProject.getUsers()).contains(foundUser);
        
        // Verify querying by project - using JPQL instead of native SQL for better compatibility
        List<User> usersInProject = userRepository.findByProjectId(project.getId());
        assertThat(usersInProject).contains(user);
        
        // Verify querying by project name
        List<User> usersByProjectName = userRepository.findByProjectName(project.getName());
        assertThat(usersByProjectName).contains(user);
        
        // Verify count
        Long userCount = userRepository.countUsersByProjectId(project.getId());
        assertThat(userCount).isEqualTo(1L);
        
        // Inspect the join table schema using our modified inspector
        schemaInspector.inspectTableSchema("user_projects");
    }

    @Test
    public void whenRemoveUserFromProject_thenRelationshipIsRemoved() {
        // Create a project
        Project project = new Project("Another Spanner Project", "A project for testing user removal");
        entityManager.persist(project);
        
        // Create a user
        User user = new User("removespanneruser", "removespanner@example.com", "Remove Spanner User");
        entityManager.persist(user);
        
        // Associate user with project
        user.addProject(project);
        entityManager.flush();
        
        // Verify relationship exists
        User foundUser = entityManager.find(User.class, user.getId());
        assertThat(foundUser.getProjects()).hasSize(1);
        
        // Remove the relationship
        foundUser.removeProject(project);
        entityManager.flush();
        
        // Verify relationship was removed
        User updatedUser = entityManager.find(User.class, user.getId());
        Project updatedProject = entityManager.find(Project.class, project.getId());
        
        assertThat(updatedUser.getProjects()).isEmpty();
        assertThat(updatedProject.getUsers()).isEmpty();
    }
    
    @Test
    public void testMultipleUsersAndProjects() {
        // Create projects
        Project project1 = new Project("Spanner Project 1", "First test project");
        Project project2 = new Project("Spanner Project 2", "Second test project");
        entityManager.persist(project1);
        entityManager.persist(project2);
        
        // Create users
        User user1 = new User("spanneruser1", "user1@example.com", "Spanner User 1");
        User user2 = new User("spanneruser2", "user2@example.com", "Spanner User 2");
        entityManager.persist(user1);
        entityManager.persist(user2);
        
        // Create relationships
        user1.addProject(project1);
        user1.addProject(project2);
        user2.addProject(project1);
        entityManager.flush();
        
        // Verify relationships
        assertThat(user1.getProjects()).hasSize(2);
        assertThat(user2.getProjects()).hasSize(1);
        assertThat(project1.getUsers()).hasSize(2);
        assertThat(project2.getUsers()).hasSize(1);
        
        // Test query by project
        List<User> usersInProject1 = userRepository.findByProjectId(project1.getId());
        assertThat(usersInProject1).hasSize(2);
        assertThat(usersInProject1).extracting("username").containsExactlyInAnyOrder("spanneruser1", "spanneruser2");
        
        // Use our PostgreSQL-compatible inspector to check the schema
        schemaInspector.inspectTableSchema("user_projects");
    }
}