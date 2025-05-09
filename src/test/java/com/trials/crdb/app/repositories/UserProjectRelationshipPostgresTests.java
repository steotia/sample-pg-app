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
import org.testcontainers.containers.PostgreSQLContainer;
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
@ContextConfiguration(initializers = UserProjectRelationshipPostgresTests.DataSourceInitializer.class)
public class UserProjectRelationshipPostgresTests {

    private PostgresCompatibilityInspector schemaInspector;

    @BeforeEach
    void setUp() {
        schemaInspector = new PostgresCompatibilityInspector(jdbcTemplate, 
            PostgresCompatibilityInspector.DatabaseType.POSTGRESQL);
    }

    @Container
    static final PostgreSQLContainer<?> postgresContainer = 
        new PostgreSQLContainer<>(DockerImageName.parse("postgres:15-alpine"))
            .withDatabaseName("test_user_project_pg")
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
    private ProjectRepository projectRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    public void whenAssignUserToProject_thenBidirectionalRelationshipIsEstablished() {
        // Create a project
        Project project = new Project("Test Project", "A project for testing user assignments");
        entityManager.persist(project);
        
        // Create a user
        User user = new User("testuser", "test@example.com", "Test User");
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
        Project project = new Project("Another Project", "A project for testing user removal");
        entityManager.persist(project);
        
        // Create a user
        User user = new User("removeuser", "remove@example.com", "Remove User");
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
}