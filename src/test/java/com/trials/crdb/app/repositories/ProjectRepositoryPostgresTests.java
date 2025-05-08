package com.trials.crdb.app.repositories;

import java.util.Map;

import org.junit.Test;
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

import static org.assertj.core.api.Assertions.assertThat;
// import static org.junit.jupiter.api.Assertions.assertThrows;

@Testcontainers
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ContextConfiguration(initializers = ProjectRepositoryPostgresTests.DataSourceInitializer.class)
public class ProjectRepositoryPostgresTests {

    @Container
    static final PostgreSQLContainer<?> postgresContainer = 
        new PostgreSQLContainer<>(DockerImageName.parse("postgres:15-alpine"))
            .withDatabaseName("test_ticketing_pg_serial")
            .withUsername("testuser")
            .withPassword("testPass");
    
    static class DataSourceInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(@NonNull ConfigurableApplicationContext appContext) {
            Map<String, Object> properties = Map.of(
                "spring.datasource.url",postgresContainer.getJdbcUrl(),
                "spring.datasource.username",postgresContainer.getUsername(),
                "spring.datasource.url",postgresContainer.getPassword(),
                "spring.datasource.driver-class-name", "org.postgresql.Driver",
                "spring.jpa.hibernate.ddl-auto", "create-drop",
                "spring.jpa.properties.hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect",
                "spring.jpa.show-sql", "true"
            );
            appContext.getEnvironment().getPropertySources()
                .addFirst(new MapPropertySource("testcontainers-postgresql", properties));
        }
    }

    @Autowired
    private TestEntityManager entityManager;

    @Autowired
    private ProjectRepository projectRepository;

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

    }

}
