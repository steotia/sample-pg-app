package com.trials.crdb.app.repositories;

import java.util.ArrayList;
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
import org.springframework.lang.NonNull;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.trials.crdb.app.model.Project;

import org.springframework.core.env.MapPropertySource;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;
// import static org.junit.jupiter.api.Assertions.assertThrows;

import com.trials.crdb.app.utils.PostgresCompatibilityInspector;

@Testcontainers
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ContextConfiguration(initializers = ProjectRepositoryPostgresTests.DataSourceInitializer.class)
public class ProjectRepositoryPostgresTests {

    private PostgresCompatibilityInspector schemaInspector;

    @BeforeEach
    void setUp() {
        schemaInspector = new PostgresCompatibilityInspector(
            jdbcTemplate, 
            PostgresCompatibilityInspector.DatabaseType.SPANNER);
    }

    @Container
    static final PostgreSQLContainer<?> postgresContainer = 
        new PostgreSQLContainer<>(DockerImageName.parse("postgres:15-alpine"))
            .withDatabaseName("test_ticketing_pg_serial")
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
            properties.put("spring.jpa.hibernate.ddl-auto", "create");  // Try "create" instead of "create-drop"
            properties.put("spring.jpa.properties.hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
            properties.put("spring.jpa.show-sql", "true");
            properties.put("spring.jpa.generate-ddl", "true");  // Add this line
            
            appContext.getEnvironment().getPropertySources()
                .addFirst(new MapPropertySource("testcontainers-postgresql", properties));
        }
    }

    @Autowired
    private TestEntityManager entityManager;

    @Autowired
    private ProjectRepository projectRepository;

   @Autowired
    private JdbcTemplate jdbcTemplate;


    @Test
    public void whenSaveProject_withValidName_thenProjectIsPersistedWithGeneratedIntegerId(){
        String projectName = "A SERIAL project";
        Project project = new Project(projectName, "A very critical project with SERIAL");
        Project savedProject = projectRepository.save(project);

        assertThat(savedProject).isNotNull();
        assertThat(savedProject.getId()).isNotNull();
        assertThat(savedProject.getId()).isGreaterThan(0);
        assertThat(savedProject.getName()).isEqualTo(projectName);
        assertThat(savedProject.getCreateTime()).isNotNull();

        Project foundInDb = entityManager.find(Project.class, savedProject.getId());
        assertThat(foundInDb).isNotNull();
        assertThat(foundInDb.getName()).isEqualTo(projectName);

        schemaInspector.inspectTableSchema("projects");

    }

    @Test
    public void testSequentialIDs(){
        List<Long> generatedIDs = new ArrayList<>();
        
        for(int i = 0;i<5;i++){
            Project project = new Project("Sequential "+i, "Testing ID generation");
            entityManager.persist(project);
            entityManager.flush();
            generatedIDs.add(project.getId());

        }

        System.out.println("Generated IDs :"+generatedIDs);

    }

}
