package com.trials.crdb.app.repositories;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.trials.crdb.app.model.Project;
import com.trials.crdb.app.model.Ticket;
import com.trials.crdb.app.model.Ticket.TicketPriority;
import com.trials.crdb.app.model.Ticket.TicketStatus;
import com.trials.crdb.app.model.User;

import java.time.ZonedDateTime;
import java.util.List;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

@Repository
public interface TicketRepository extends JpaRepository<Ticket, Long> {

    // Basic finder methods
    List<Ticket> findByStatus(Ticket.TicketStatus status);
    List<Ticket> findByPriority(Ticket.TicketPriority priority);
    List<Ticket> findByAssignee(User assignee);
    List<Ticket> findByReporter(User reporter);
    List<Ticket> findByProject(Project project);
    Page<Ticket> findByProject(Project project, Pageable pageable);
    
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

    // Basic JSON key-value query (existing method)
    @Query(value = "SELECT * FROM tickets WHERE metadata ->> :key = :value", nativeQuery = true)
    List<Ticket> findByMetadataKeyValue(@Param("key") String key, @Param("value") String value);

    // Query by numeric JSON value with comparison
    @Query(value = "SELECT * FROM tickets WHERE (metadata->>'complexity')::int > :value", nativeQuery = true)
    List<Ticket> findByComplexityGreaterThan(@Param("value") int value);

    // Query for tickets with a specific tag in the tags array
    // TOIL array containment ? operator did not work
    // @Query(value = "SELECT * FROM tickets WHERE metadata->'tags' ? :tag", nativeQuery = true)
    @Query(value = "SELECT * FROM tickets WHERE EXISTS (SELECT 1 FROM jsonb_array_elements_text(metadata->'tags') tag WHERE tag = ?1)", nativeQuery = true)
    List<Ticket> findByTag(@Param("tag") String tag);

    // Query for tickets with multiple specific keys in metadata
    // trying to avoid array containment ? operator
    // @Query(value = "SELECT * FROM tickets WHERE metadata ? :key1 AND metadata ? :key2", nativeQuery = true)
    // TOIL below didnt work
    // @Query(value = "SELECT * FROM tickets WHERE " +
    //            "metadata ?? ?1 AND metadata ?? ?2", nativeQuery = true)
    // Check if keys exist using the ->> operator (returns NULL if key doesn't exist)
    @Query(value = "SELECT * FROM tickets WHERE " +
               "metadata->>?1 IS NOT NULL AND metadata->>?2 IS NOT NULL", nativeQuery = true)
    List<Ticket> findByMetadataContainingKeys(@Param("key1") String key1, @Param("key2") String key2);

    @Query(value = "SELECT id, metadata->>?1 AS value FROM tickets", nativeQuery = true)
    List<Object[]> dumpMetadataValues(String key);

    // Query for tickets with metadata containing a JSON object with specific key-value
    // Below is not working
    // @Query(value = "SELECT * FROM tickets WHERE metadata @> jsonb_build_object(:key, :value::jsonb)", nativeQuery = true)
    // List<Ticket> findByMetadataContaining(@Param("key") String key, @Param("value") String value);
    // Containment using JSON function without problematic parameter casts
    @Query(value = "SELECT * FROM tickets WHERE metadata @> jsonb_build_object(?1, CAST(?2 AS JSONB))", nativeQuery = true)
    List<Ticket> findByMetadataContaining(String key, String value);

    // TOIL 
    // CAST(?2 AS JSONB) is not working
    // @Query(value = "SELECT * FROM tickets WHERE metadata @> jsonb_build_object(?1, CAST(?2 AS JSONB))", nativeQuery = true)
    // @Query(value = "SELECT * FROM tickets WHERE metadata @> jsonb_build_object(?1, to_jsonb(?2))", nativeQuery = true)
    // FAIL
    /* 
     * [ERROR]   TicketAdvancedQueriesSpannerTests.testJsonbContainment:494 » JpaSystem JDBC exception executing SQL [SELECT * FROM tickets 
     * WHERE metadata @> jsonb_build_object(?, ?::text)] [ERROR: Postgres function jsonb_build_object(text, text) is not supported - Statement: 
     * 'SELECT * FROM tickets WHERE metadata @> jsonb_build_object($1, $2::text)'] [n/a]
     */
    @Query(value = "SELECT * FROM tickets WHERE metadata @> jsonb_build_object(?, ?::text)", nativeQuery = true)
    List<Ticket> findByMetadataContainingForSpanner(String key, String value);

    // TOIL
    // Also this is sub-optimal
    // Spanner-compatible array containment check using JSON_EXTRACT_ARRAY
    // TOIL
    // json_extract_array does not exist
    // @Query(value = "SELECT * FROM tickets WHERE EXISTS(SELECT 1 FROM UNNEST(JSON_EXTRACT_ARRAY(metadata, '$.tags')) AS tag WHERE tag = ?1)", nativeQuery = true)
    // List<Ticket> findByTagSpanner(@Param("tag1") String tag); 
    // TOIL 
    // JSON_EXTRACT_ARRAY does not exist
    // @Query(value = "SELECT * FROM tickets t WHERE EXISTS (SELECT 1 FROM UNNEST(JSON_EXTRACT_ARRAY(t.metadata, '$.tags')) AS json_tag WHERE JSON_VALUE(json_tag, '$') = ?1)", nativeQuery = true)
    // TOIL JSON_QUERY does not exist
    // @Query(value = "SELECT * FROM tickets t WHERE EXISTS (SELECT 1 FROM UNNEST(JSON_QUERY(t.metadata, '$.tags')) AS tag WHERE JSON_VALUE(tag, '$') = ?1)", nativeQuery = true)
    // @Query(value = "SELECT * FROM tickets t WHERE EXISTS (SELECT 1 FROM jsonb_array_elements(t.metadata->'tags') tag WHERE tag::text = ?1)", nativeQuery = true)
    // ERROR this breaks Spanner
    // @Query(value = "SELECT t.* FROM tickets t CROSS JOIN LATERAL jsonb_array_elements(t.metadata->'tags') AS tag WHERE tag::text = ?1;", nativeQuery = true)
    @Query(value = "SELECT * FROM tickets t WHERE EXISTS (SELECT 1 FROM jsonb_array_elements(t.metadata->'tags') tag WHERE tag::text = ?1)", nativeQuery = true)
    List<Ticket> findByTagSpanner(@Param("tag") String tag);

    // @Query(value = "SELECT * FROM tickets ORDER BY CASE priority " +
    //               "WHEN 'CRITICAL' THEN 0 " +
    //               "WHEN 'HIGH' THEN 1 " +
    //               "WHEN 'MEDIUM' THEN 2 " +
    //               "WHEN 'LOW' THEN 3 END DESC", 
    //        nativeQuery = true)
    // List<Ticket> findAllOrderByPriorityCustom();

    @Query(value = "SELECT *, CASE priority " +
              "WHEN 'CRITICAL' THEN 0 " +
              "WHEN 'HIGH' THEN 1 " +
              "WHEN 'MEDIUM' THEN 2 " +
              "WHEN 'LOW' THEN 3 " +
              "ELSE 999 END AS priority_order " +
              "FROM tickets ORDER BY priority_order ASC", 
       nativeQuery = true)
    List<Ticket> findAllOrderByPriorityCustom();

    // Add this method to TicketRepository:
    @Query("SELECT t.priority, COUNT(t) FROM Ticket t GROUP BY t.priority")
    List<Object[]> countByPriority();

    @Query(value = "SELECT t.* FROM tickets t " +
                   "JOIN users u ON t.reporter_id = u.id " +
                   "WHERE u.username = :username AND " +
                   "t.status NOT IN ('CLOSED', 'RESOLVED')", 
           nativeQuery = true)
    List<Ticket> findActiveTicketsByReporterUsername(@Param("username") String username);

    // PHASE

    // Finding overdue tickets
    @Query("SELECT t FROM Ticket t WHERE t.dueDate < :referenceTime AND t.status NOT IN ('RESOLVED', 'CLOSED')")
    List<Ticket> findOverdueTickets(@Param("referenceTime") ZonedDateTime referenceTime);


    // // Tickets due in the next N days
    @Query("SELECT t FROM Ticket t WHERE t.dueDate BETWEEN :startDate AND :endDate AND t.status NOT IN ('RESOLVED', 'CLOSED')")
    List<Ticket> findTicketsDueInRange(
        @Param("startDate") ZonedDateTime startDate,
        @Param("endDate") ZonedDateTime endDate);    
        
    // Filtering by date range
    @Query("SELECT t FROM Ticket t WHERE t.createTime BETWEEN :startDate AND :endDate")
    List<Ticket> findTicketsCreatedBetween(
        @Param("startDate") ZonedDateTime startDate, 
        @Param("endDate") ZonedDateTime endDate);

    // Average resolution time query
    // didn't work
    // @Query("SELECT AVG(FUNCTION('EXTRACT', EPOCH FROM t.resolvedDate - t.createTime)) FROM Ticket t WHERE t.resolvedDate IS NOT NULL")
    @Query(value = "SELECT AVG(EXTRACT(EPOCH FROM (resolved_date - create_time))) FROM tickets WHERE resolved_date IS NOT NULL", nativeQuery = true)
    Double calculateAverageResolutionTimeInSeconds();

    // PostgreSQL-specific interval calculation
    @Query(value = "SELECT AVG(resolved_date - create_time) FROM tickets WHERE resolved_date IS NOT NULL", nativeQuery = true)
    String calculateAverageResolutionTimeInterval();

    // Timezone-aware query
    @Query(value = "SELECT * FROM tickets WHERE DATE(due_date AT TIME ZONE :timezone) = CURRENT_DATE AND status NOT IN ('RESOLVED', 'CLOSED')", nativeQuery = true)
    List<Ticket> findTicketsDueTodayInTimezone(@Param("timezone") String timezone);

    // Phase
    // Arithmetic and aggregation
    @Query(value = "SELECT SUM(estimated_hours) AS total, " +
                "AVG(estimated_hours) AS average, " +
                "MAX(estimated_hours) AS maximum, " +
                "SQRT(SUM(estimated_hours * estimated_hours)) AS rms " +
                "FROM tickets " +
                "WHERE project_id = ?1", nativeQuery = true)
    Map<String, Object> calculateProjectEstimationStatistics(Long projectId);

    // Window functions - priority ranking
    @Query(value = "SELECT id, title, priority, " +
                "ROW_NUMBER() OVER(PARTITION BY project_id ORDER BY " +
                "CASE priority " +
                "  WHEN 'CRITICAL' THEN 0 " +
                "  WHEN 'HIGH' THEN 1 " +
                "  WHEN 'MEDIUM' THEN 2 " +
                "  WHEN 'LOW' THEN 3 " +
                "  ELSE 4 END) AS priority_rank " +
                "FROM tickets " +
                "WHERE status IN ('OPEN', 'IN_PROGRESS') " +
                "ORDER BY project_id, priority_rank", nativeQuery = true)
    List<Object[]> findTicketsWithPriorityRanking();

    // Window functions - LAG/LEAD for time gap analysis
    @Query(value = "SELECT id, title, create_time, " +
                "LAG(title, 1) OVER(PARTITION BY project_id ORDER BY create_time) AS prev_ticket, " +
                "LEAD(title, 1) OVER(PARTITION BY project_id ORDER BY create_time) AS next_ticket, " +
                "EXTRACT(EPOCH FROM (create_time - LAG(create_time, 1) OVER(PARTITION BY project_id ORDER BY create_time)))/3600 AS hours_since_prev " +
                "FROM tickets " +
                "WHERE project_id = ?1 " +
                "ORDER BY create_time", nativeQuery = true)
    List<Object[]> findTicketSequenceWithTimeGaps(Long projectId);

    // Array functions - PostgreSQL style
    @Query(value = "SELECT id, title, tags, " +
                "array_length(tags, 1) AS tag_count " +
                "FROM tickets " +
                "WHERE ?1 = ANY(tags) " +
                "ORDER BY id", nativeQuery = true)
    List<Object[]> findTicketsByTagWithArrayFunctions(String tag);

    // Spanner-compatible version - avoiding arrays
    @Query(value = "SELECT id, title " +
                "FROM tickets " +
                "WHERE tags IS NOT NULL AND " +
                "STRPOS(ARRAY_TO_STRING(tags, ','), ?1) > 0 " +
                "ORDER BY id", nativeQuery = true)
    List<Object[]> findTicketsByTagSimplified(String tag);

    // Basic CTE example
    @Query(value = "WITH project_tickets AS (" +
                "  SELECT t.* FROM tickets t " +
                "  JOIN projects p ON t.project_id = p.id " +
                "  WHERE p.name = ?1 " +
                ") " +
                "SELECT pt.id, pt.title, pt.status, pt.priority " +
                "FROM project_tickets pt " +
                "ORDER BY pt.priority, pt.id", nativeQuery = true)
    List<Object[]> findTicketsWithCTEByProjectName(String projectName);

    // Recursive CTE for dependency chains
    @Query(value = "WITH RECURSIVE ticket_chain AS (" +
                "  SELECT id, title, dependent_on_id, 1 AS depth " +
                "  FROM tickets " +
                "  WHERE id = ?1 " +
                "  UNION ALL " +
                "  SELECT t.id, t.title, t.dependent_on_id, tc.depth + 1 " +
                "  FROM tickets t " +
                // "  JOIN ticket_chain tc ON t.dependent_on_id = tc.id " +
                "  JOIN ticket_chain tc ON t.id = tc.dependent_on_id " +
                "  WHERE tc.depth < 10 " +
                ") " +
                "SELECT id, title, depth FROM ticket_chain ORDER BY depth DESC", nativeQuery = true)
    List<Object[]> findTicketDependencyChain(Long ticketId);
    
    // Step 1: Find tickets with matching tag 
    @Query(value = "SELECT id, title FROM tickets WHERE ?1 = ANY(tags) ORDER BY id", nativeQuery = true)
    List<Object[]> findTicketIdsByTag(String tag);

    // Step 2: Get tag count for a specific ticket (separate query)
    @Query(value = "SELECT array_length(tags, 1) FROM tickets WHERE id = ?1", nativeQuery = true)
    Integer getTagCountForTicket(Long ticketId);

    List<Ticket> findByDependentOn(Ticket dependentOn);
}