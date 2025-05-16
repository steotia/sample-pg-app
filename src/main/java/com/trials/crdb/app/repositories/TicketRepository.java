package com.trials.crdb.app.repositories;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.trials.crdb.app.model.Ticket;
import com.trials.crdb.app.model.Ticket.TicketPriority;
import com.trials.crdb.app.model.Ticket.TicketStatus;
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

    /* 
     * PostgreSQL-specific features like:

        Using ILIKE for case-insensitive matching
        Using PostgreSQL's ARRAY constructor and ALL predicate
        PostgreSQL-style casting with CAST(field AS string)
        Standard PostgreSQL string concatenation with CONCAT()
    */

    // Case insensitive search - using PostgreSQL ILIKE
    @Query("SELECT t FROM Ticket t WHERE t.title ILIKE CONCAT('%', :keyword, '%') OR t.description ILIKE CONCAT('%', :keyword, '%')")
    List<Ticket> findByKeywordCaseInsensitive(@Param("keyword") String keyword);

    // Multi-word search with array parameter and ALL construct
    @Query(value = "SELECT * FROM tickets WHERE " +
                "title ILIKE ALL(ARRAY[CONCAT('%', :keyword1, '%'), CONCAT('%', :keyword2, '%')]) OR " +
                "description ILIKE ALL(ARRAY[CONCAT('%', :keyword1, '%'), CONCAT('%', :keyword2, '%')])", 
        nativeQuery = true)
    List<Ticket> findByMultipleKeywords(@Param("keyword1") String keyword1, @Param("keyword2") String keyword2);

    // Prefix search with ILIKE
    @Query("SELECT t FROM Ticket t WHERE t.title ILIKE CONCAT(:prefix, '%') OR t.description ILIKE CONCAT(:prefix, '%')")
    List<Ticket> findByPrefix(@Param("prefix") String prefix);

    // Multiple field search including enums as strings
    @Query("SELECT t FROM Ticket t WHERE " +
        "t.title ILIKE CONCAT('%', :keyword, '%') OR " +
        "t.description ILIKE CONCAT('%', :keyword, '%') OR " +
        "CAST(t.status AS string) ILIKE CONCAT('%', :keyword, '%') OR " +
        "CAST(t.priority AS string) ILIKE CONCAT('%', :keyword, '%')")
    List<Ticket> findByKeywordAcrossFields(@Param("keyword") String keyword);

    // The ILIKE operator expects text values, but Hibernate is passing a bytea (binary) type for some parameters
    // Advanced search with multiple optional criteria
    @Query("SELECT t FROM Ticket t WHERE " +
        "(:title IS NULL OR t.title ILIKE CONCAT('%', CAST(:title AS text), '%')) AND " +
        "(:description IS NULL OR t.description ILIKE CONCAT('%', CAST(:description AS text), '%')) AND " +
        "(:status IS NULL OR t.status = :status) AND " +
        "(:priority IS NULL OR t.priority = :priority)")
    List<Ticket> findByAdvancedCriteria(
        @Param("title") String title,
        @Param("description") String description,
        @Param("status") TicketStatus status,
        @Param("priority") TicketPriority priority);

    // For Spanner - Advanced search using STRPOS instead of ILIKE
    @Query(value = "SELECT * FROM tickets t WHERE " +
                "(:title IS NULL OR STRPOS(LOWER(t.title), LOWER(:title)) > 0) AND " +
                "(:description IS NULL OR STRPOS(LOWER(t.description), LOWER(:description)) > 0) AND " +
                "(:status IS NULL OR t.status = :status) AND " +
                "(:priority IS NULL OR t.priority = :priority)",
        nativeQuery = true)
    List<Ticket> findByAdvancedCriteriaSpanner(
        @Param("title") String title,
        @Param("description") String description,
        @Param("status") String status,
        @Param("priority") String priority);

    // For Spanner - Case insensitive search using STRPOS
    @Query(value = "SELECT * FROM tickets WHERE " +
                "STRPOS(LOWER(title), LOWER(:keyword)) > 0 OR " +
                "STRPOS(LOWER(description), LOWER(:keyword)) > 0", 
        nativeQuery = true)
    List<Ticket> findByKeywordCaseInsensitiveSpanner(@Param("keyword") String keyword);

    // For Spanner - Multiple keyword search
    @Query(value = 
        "SELECT * FROM tickets t WHERE " +
        "(STRPOS(LOWER(t.title), LOWER(:keyword1)) > 0 AND STRPOS(LOWER(t.title), LOWER(:keyword2)) > 0) OR " +
        "(STRPOS(LOWER(t.description), LOWER(:keyword1)) > 0 AND STRPOS(LOWER(t.description), LOWER(:keyword2)) > 0)",
        nativeQuery = true)
    List<Ticket> findByMultipleKeywordsSpanner(@Param("keyword1") String keyword1, @Param("keyword2") String keyword2);

    // For Spanner - Prefix search using STARTS_WITH
    @Query(value = "SELECT * FROM tickets WHERE " +
                "STARTS_WITH(LOWER(title), LOWER(:prefix)) OR " +
                "STARTS_WITH(LOWER(description), LOWER(:prefix))", 
        nativeQuery = true)
    List<Ticket> findByPrefixSpanner(@Param("prefix") String prefix);

    // For Spanner - Search across fields
    @Query(value = "SELECT * FROM tickets WHERE " +
                "STRPOS(LOWER(title), LOWER(:keyword)) > 0 OR " +
                "STRPOS(LOWER(description), LOWER(:keyword)) > 0 OR " +
                "STRPOS(LOWER(status), LOWER(:keyword)) > 0 OR " +
                "STRPOS(LOWER(priority), LOWER(:keyword)) > 0", 
        nativeQuery = true)
    List<Ticket> findByKeywordAcrossFieldsSpanner(@Param("keyword") String keyword);

}