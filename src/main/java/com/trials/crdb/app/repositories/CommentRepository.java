package com.trials.crdb.app.repositories;

import java.time.ZonedDateTime;
import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.trials.crdb.app.model.Comment;
import com.trials.crdb.app.model.Ticket;
import com.trials.crdb.app.model.User;

@Repository
public interface CommentRepository extends JpaRepository<Comment, Long> {

    // Basic finder methods
    List<Comment> findByTicket(Ticket ticket);
    List<Comment> findByCommenter(User commenter);
    
    // Paginated queries
    Page<Comment> findByTicket(Ticket ticket, Pageable pageable);
    Page<Comment> findByCommenter(User commenter, Pageable pageable);
    
    // TOIL - doesn't work in Spanner
    /*
     * ORM introduces ESCAPE clause
     * Missing from Main Documentation
        The following official docs do not mention LIKE ESCAPE limitations:

        ❌ Known Issues in PostgreSQL Interface
        ❌ Supported PostgreSQL Functions
        ❌ PostgreSQL Query Syntax
        ❌ String Functions Documentation
     */ 
    // Text search in comments
    @Query("SELECT c FROM Comment c WHERE c.content LIKE %:keyword%")
    List<Comment> findByContentContaining(@Param("keyword") String keyword);

    // WORKAROUND
    // TOIL - didnt work - casting issues
    // @Query(value = "SELECT * FROM comments WHERE content LIKE CONCAT('%', ?1, '%')", nativeQuery = true)
    // TOIL - didnt work - posibly Spanner can't determine the data type of the ?1 parameter when it's used inside CONCAT()
    // @Query(value = "SELECT * FROM comments WHERE content LIKE CONCAT('%', CAST(?1 AS STRING), '%')", nativeQuery = true)
    @Query(value = "SELECT * FROM comments WHERE content LIKE '%' || ?1 || '%'", nativeQuery = true)
    List<Comment> findByContentContainingSpanner(@Param("keyword") String keyword);

    
    // TOIL - doesnt work in Spanner
    // as mentioned above
    // Case-insensitive text search
    @Query(value = "SELECT * FROM comments WHERE LOWER(content) LIKE LOWER(CONCAT('%', ?1, '%'))", nativeQuery = true)
    List<Comment> findByContentContainingIgnoreCase(@Param("keyword") String keyword);

    // WORKAROUND
    // TOIL - CONCAT didn't work
    // @Query(value = "SELECT * FROM comments WHERE LOWER(content) LIKE LOWER(CONCAT('%', ?1, '%'))", nativeQuery = true)
    @Query(value = "SELECT * FROM comments WHERE LOWER(content) LIKE LOWER('%' || ?1 || '%')", nativeQuery = true)
    List<Comment> findByContentContainingIgnoreCaseSpanner(@Param("keyword") String keyword);
    
    // TOIL
    // Search across multiple fields
    @Query("SELECT c FROM Comment c JOIN c.ticket t WHERE " +
           "c.content LIKE %:keyword% OR t.title LIKE %:keyword% OR t.description LIKE %:keyword%")
    List<Comment> findByContentOrTicketContaining(@Param("keyword") String keyword);

    // WORKAROUND
    // TOIL - didnt work
    // @Query(value = "SELECT c.* FROM comments c " +
    //        "JOIN tickets t ON c.ticket_id = t.id " +
    //        "WHERE c.content LIKE CONCAT('%', ?1, '%') " +
    //        "   OR t.title LIKE CONCAT('%', ?1, '%') " +
    //        "   OR t.description LIKE CONCAT('%', ?1, '%')", nativeQuery = true)
    // List<Comment> findByContentOrTicketContainingSpanner(@Param("keyword") String keyword);
    @Query(value = "SELECT c.* FROM comments c " +
       "JOIN tickets t ON c.ticket_id = t.id " +
       "WHERE c.content LIKE '%' || ?1 || '%' " +
       "   OR t.title LIKE '%' || ?1 || '%' " +
       "   OR t.description LIKE '%' || ?1 || '%'", nativeQuery = true)
    List<Comment> findByContentOrTicketContainingSpanner(@Param("keyword") String keyword);
    
    // Count queries
    Long countByTicket(Ticket ticket);
    Long countByCommenter(User commenter);
    
    // Recent comments
    @Query("SELECT c FROM Comment c ORDER BY c.createTime DESC")
    Page<Comment> findRecentComments(Pageable pageable);
    
    // Comments created after a specific date
    @Query("SELECT c FROM Comment c WHERE c.createTime > :since ORDER BY c.createTime DESC")
    List<Comment> findCommentsCreatedAfter(@Param("since") ZonedDateTime since);
    
    // TOIL - needs change for Spanner
    /* When Hibernate translates this JPQL to SQL, it's selecting all fields from the User entity (commenter) 
    but only grouping by the user's ID. Spanner is stricter about SQL standards than PostgreSQL/CockroachDB and 
    requires all non-aggregated columns in the SELECT clause to be in the GROUP BY clause. 
    PostgreSQL uses functional dependency detection. 
    */
    // Top commenters (users with most comments)
    // @Query("SELECT c.commenter, COUNT(c) FROM Comment c GROUP BY c.commenter ORDER BY COUNT(c) DESC")
    @Query("SELECT u, COUNT(c) FROM Comment c JOIN c.commenter u GROUP BY u.id, u.username, u.email, u.fullName, u.createTime ORDER BY COUNT(c) DESC")
    List<Object[]> findTopCommenters(Pageable pageable);
    
    // TOIL - didnt work in Spanner
    /* Spanner doesn't support the character_length() function that Hibernate generates from the JPQL LENGTH() function */
    // Comments with content longer than specified length
    @Query("SELECT c FROM Comment c WHERE LENGTH(c.content) > :minLength")
    List<Comment> findLongComments(@Param("minLength") int minLength);

    // Spanner-compatible version using native SQL
    @Query(value = "SELECT * FROM comments WHERE LENGTH(content) > ?1", nativeQuery = true)
    List<Comment> findLongCommentsSpanner(@Param("minLength") int minLength);
    
    // TOIL - didnt work in Spanner
    // Average comment length
    @Query("SELECT AVG(LENGTH(c.content)) FROM Comment c")
    Double getAverageCommentLength();

    // Spanner-compatible average comment length
    @Query(value = "SELECT AVG(LENGTH(content)) FROM comments", nativeQuery = true)
    Double getAverageCommentLengthSpanner();
}