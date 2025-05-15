package com.trials.crdb.app.repositories;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.trials.crdb.app.model.Ticket;
import com.trials.crdb.app.model.User;
import com.trials.crdb.app.model.Project;

import java.util.List;

@Repository
public interface TicketRepository extends JpaRepository<Ticket, Long> {

    // Basic finder methods
    List<Ticket> findByStatus(Ticket.TicketStatus status);
    List<Ticket> findByPriority(Ticket.TicketPriority priority);
    List<Ticket> findByAssignee(User assignee);
    List<Ticket> findByReporter(User reporter);
    List<Ticket> findByProject(Project project);
    
    // Combined finders
    List<Ticket> findByStatusAndPriority(Ticket.TicketStatus status, Ticket.TicketPriority priority);
    List<Ticket> findByProjectAndStatus(Project project, Ticket.TicketStatus status);
    List<Ticket> findByAssigneeAndStatus(User assignee, Ticket.TicketStatus status);
    
    // Text search
    @Query("SELECT t FROM Ticket t WHERE t.title LIKE %:keyword% OR t.description LIKE %:keyword%")
    List<Ticket> findByKeyword(@Param("keyword") String keyword);
    
    // TOIL
    // Spanner-compatible alternative - avoiding JPQL and using nativeQuery = true
    @Query(value = "SELECT * FROM tickets WHERE " +
                "title LIKE '%' || :keyword || '%' OR " +
                "description LIKE '%' || :keyword || '%'", 
        nativeQuery = true)
    List<Ticket> findByKeywordForSpanner(@Param("keyword") String keyword);

    // JSONB queries - using native queries for each database
    
    // PostgreSQL JSONB query
    @Query(value = "SELECT * FROM tickets WHERE metadata ->> :key = :value", nativeQuery = true)
    List<Ticket> findByMetadataKeyValue(@Param("key") String key, @Param("value") String value);
    
    // Query for tickets without an assignee
    List<Ticket> findByAssigneeIsNull();
    
    // Count tickets by status
    Long countByStatus(Ticket.TicketStatus status);
    
    // Count tickets by project and status
    Long countByProjectAndStatus(Project project, Ticket.TicketStatus status);
}