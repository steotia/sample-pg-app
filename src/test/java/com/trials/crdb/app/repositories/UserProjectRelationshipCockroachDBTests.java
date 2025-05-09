package com.trials.crdb.app.repositories;

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

import com.trials.crdb.app.model.Project;
import com.trials.crdb.app.model.User;

import org.springframework.core.env.MapPropertySource;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

import com.trials.crdb.app.utils.PostgresCompatibilityInspector;

@Testcontainers
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ContextConfiguration(initializers = UserProjectRelationshipCockroachDBTests.DataSourceInitializer.class)
public class UserProjectRelationshipCockroachDBTests {

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
    private ProjectRepository projectRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    public void whenAssignUserToProject_thenBidirectionalRelationshipIsEstablished() {
        // Create a project
        Project project = new Project("CockroachDB Project", "A project for testing user assignments in CockroachDB");
        entityManager.persist(project);
        
        // Create a user
        User user = new User("crdbuser", "crdb@example.com", "CockroachDB User");
        entityManager.persist(user);
        
        // Associate user with project
        user.addProject(project);
        entityManager.flush();
        
        // Verify relationship was established
        User foundUser = entityManager.find(User.class, user.getId());
        Project foundProject = entityManager.find(Project.class, project.getId());
        
        assertThat(foundUser.getProjects()).contains(foundProject);
        assertThat(foundProject.getUsers()).contains(foundUser);
        
        // Verify querying by project
        List<User> usersInProject = userRepository.findByProjectId(project.getId());
        assertThat(usersInProject).contains(user);
        
        // Verify querying by project name
        List<User> usersByProjectName = userRepository.findByProjectName(project.getName());
        assertThat(usersByProjectName).contains(user);
        
        // Verify count
        Long userCount = userRepository.countUsersByProjectId(project.getId());
        assertThat(userCount).isEqualTo(1L);
        
        // Inspect the join table schema
        schemaInspector.inspectTableSchema("user_projects");
    }

    @Test
    public void whenRemoveUserFromProject_thenRelationshipIsRemoved() {
        // Create a project
        Project project = new Project("Another CockroachDB Project", "A project for testing user removal");
        entityManager.persist(project);
        
        // Create a user
        User user = new User("removecrdbuser", "removecrdb@example.com", "Remove CockroachDB User");
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
        Project project1 = new Project("CockroachDB Project 1", "First test project");
        Project project2 = new Project("CockroachDB Project 2", "Second test project");
        entityManager.persist(project1);
        entityManager.persist(project2);
        
        // Create users
        User user1 = new User("crdbuser1", "user1@example.com", "CockroachDB User 1");
        User user2 = new User("crdbuser2", "user2@example.com", "CockroachDB User 2");
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
        assertThat(usersInProject1).extracting("username").containsExactlyInAnyOrder("crdbuser1", "crdbuser2");
        
        // Inspect the join table to check for foreign keys and indexes
        schemaInspector.inspectTableSchema("user_projects");
    }
}