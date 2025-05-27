package com.trials.crdb.app.transaction;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.lang.NonNull;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.trials.crdb.app.model.Ticket;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ContextConfiguration(initializers = OptimisticLockingPostgresTests.DataSourceInitializer.class)
public class OptimisticLockingPostgresTests extends TransactionTestBase {

    @Container
    static final PostgreSQLContainer<?> postgresContainer = 
        new PostgreSQLContainer<>(DockerImageName.parse("postgres:15-alpine"))
            .withDatabaseName("test_transactions")
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

    @PersistenceContext
    private EntityManager em;
    
    @Autowired
    private PlatformTransactionManager transactionManager;

    @Override
    protected String getDatabaseType() {
        return "PostgreSQL";
    }

    @Test
    public void testBasicOptimisticLocking() {
        logTestContext("Basic Optimistic Locking Test");
        
        // Load the same ticket in two separate transactions
        Ticket ticket1 = transactionTemplate.execute(status -> 
            ticketRepository.findById(testTicket.getId()).orElseThrow()
        );
        
        Ticket ticket2 = transactionTemplate.execute(status -> 
            ticketRepository.findById(testTicket.getId()).orElseThrow()
        );
        
        assertThat(ticket1.getVersion()).isEqualTo(0L);
        assertThat(ticket2.getVersion()).isEqualTo(0L);
        
        // Update and save first ticket
        transactionTemplate.execute(status -> {
            ticket1.setStatus(Ticket.TicketStatus.IN_PROGRESS);
            ticketRepository.save(ticket1);
            return null;
        });
        
        // Verify version was incremented
        Ticket updated = ticketRepository.findById(testTicket.getId()).orElseThrow();
        assertThat(updated.getVersion()).isEqualTo(1);
        System.out.println("First update succeeded, version now: " + updated.getVersion());
        
        // Try to update second ticket - should fail
        Exception exception = assertThrows(Exception.class, () -> {
            transactionTemplate.execute(status -> {
                ticket2.setStatus(Ticket.TicketStatus.CLOSED);
                ticketRepository.save(ticket2);
                return null;
            });
        });
        
        System.out.println("Second update failed with: " + exception.getClass().getName());
        System.out.println("Exception message: " + exception.getMessage());
        
        // Verify it's an optimistic lock exception
        assertThat(exception).isInstanceOf(ObjectOptimisticLockingFailureException.class);
    }

    @Test
    public void debugVersionField() {
        // Create and save a ticket
        Ticket ticket = createTestTicket();
        Ticket saved = ticketRepository.save(ticket);
        
        // Force flush to database
        entityManager.flush();
        
        System.out.println("After save - Version: " + saved.getVersion());
        
        // Clear persistence context
        entityManager.clear();
        
        // Reload from database
        Ticket reloaded = ticketRepository.findById(saved.getId()).orElseThrow();
        System.out.println("After reload - Version: " + reloaded.getVersion());
        
        // Update and save
        reloaded.setTitle("Updated title");
        ticketRepository.save(reloaded);
        entityManager.flush();
        
        System.out.println("After update - Version: " + reloaded.getVersion());
    }
    
    @Test
    public void testConcurrentUpdates() throws Exception {
        logTestContext("Concurrent Updates Test");
        
        int threadCount = 5;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        AtomicReference<String> failureType = new AtomicReference<>();
        
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        
        for (int i = 0; i < threadCount; i++) {
            final int threadNum = i;
            executor.submit(() -> {
                try {
                    startLatch.await(); // Wait for signal to start
                    
                    transactionTemplate.execute(status -> {
                        Ticket ticket = ticketRepository.findById(testTicket.getId()).orElseThrow();
                        ticket.setPriority(Ticket.TicketPriority.HIGH);
                        ticket.setDescription("Updated by thread " + threadNum);
                        ticketRepository.save(ticket);
                        return null;
                    });
                    
                    successCount.incrementAndGet();
                } catch (OptimisticLockingFailureException e) {
                    failureCount.incrementAndGet();
                    failureType.set(e.getClass().getSimpleName());
                } catch (Exception e) {
                    failureCount.incrementAndGet();
                    failureType.set(e.getClass().getSimpleName());
                } finally {
                    endLatch.countDown();
                }
            });
        }
        
        // Start all threads at once
        startLatch.countDown();
        
        // Wait for all to complete
        endLatch.await();
        executor.shutdown();
        
        // Results
        System.out.println("Concurrent update results:");
        System.out.println("- Success count: " + successCount.get());
        System.out.println("- Failure count: " + failureCount.get());
        System.out.println("- Failure type: " + failureType.get());
        
        // In PostgreSQL with optimistic locking:
        // - Only 1 should succeed
        // - Others should fail with OptimisticLockingFailureException
        assertThat(successCount.get()).isEqualTo(1);
        assertThat(failureCount.get()).isEqualTo(threadCount - 1);
        
        // Check final state
        Ticket finalTicket = ticketRepository.findById(testTicket.getId()).orElseThrow();
        System.out.println("Final ticket version: " + finalTicket.getVersion());
        assertThat(finalTicket.getVersion()).isEqualTo(1L);
    }

    @Test
    public void testReadThenWritePattern() {
        logTestContext("Read-Then-Write Pattern Test");
        
        // Common pattern: read, process, update
        transactionTemplate.execute(status -> {
            // Read
            Ticket ticket = ticketRepository.findById(testTicket.getId()).orElseThrow();
            System.out.println("Read ticket with version: " + ticket.getVersion());
            
            // Simulate processing time
            try { Thread.sleep(100); } catch (Exception e) {}
            
            // Update based on current state
            if (ticket.getStatus() == Ticket.TicketStatus.OPEN) {
                ticket.setStatus(Ticket.TicketStatus.IN_PROGRESS);
                ticket.setAssignee(user2);
            }
            
            // Write
            ticketRepository.save(ticket);
            System.out.println("Updated ticket, new version will be: " + (ticket.getVersion() + 1));
            
            return null;
        });
        
        // Verify
        Ticket updated = ticketRepository.findById(testTicket.getId()).orElseThrow();
        assertThat(updated.getVersion()).isEqualTo(1L);
        assertThat(updated.getStatus()).isEqualTo(Ticket.TicketStatus.IN_PROGRESS);
    }

    @Test
    public void testVersionFieldBehavior() {
        logTestContext("Version Field Behavior Test");
        
        // Test 1: Version starts at 0
        Ticket newTicket = new Ticket("Version Test", "Testing version field", user1, project1);
        assertThat(newTicket.getVersion()).isEqualTo(0L);
        
        Ticket saved = ticketRepository.save(newTicket);
        assertThat(saved.getVersion()).isEqualTo(0L);
        
        // Test 2: Version increments on update
        saved.setDescription("Updated description");
        Ticket updated = ticketRepository.save(saved);
        assertThat(updated.getVersion()).isEqualTo(1L);
        
        // Test 3: No version change on read
        Ticket read = ticketRepository.findById(updated.getId()).orElseThrow();
        assertThat(read.getVersion()).isEqualTo(1L);
        
        // Test 4: Version is checked on save
        saved.setVersion(0L); // Simulate stale version
        assertThrows(OptimisticLockingFailureException.class, () -> {
            ticketRepository.save(saved);
        });
        
        System.out.println("Version field behavior verified");
    }
}