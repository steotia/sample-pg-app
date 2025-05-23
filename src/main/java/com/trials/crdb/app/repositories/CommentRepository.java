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
    
    // Text search in comments
    @Query("SELECT c FROM Comment c WHERE c.content LIKE %:keyword%")
    List<Comment> findByContentContaining(@Param("keyword") String keyword);
    
    // Case-insensitive text search
    @Query("SELECT c FROM Comment c WHERE c.content ILIKE %:keyword%")
    List<Comment> findByContentContainingIgnoreCase(@Param("keyword") String keyword);
    
    // Search across multiple fields
    @Query("SELECT c FROM Comment c JOIN c.ticket t WHERE " +
           "c.content LIKE %:keyword% OR t.title LIKE %:keyword% OR t.description LIKE %:keyword%")
    List<Comment> findByContentOrTicketContaining(@Param("keyword") String keyword);
    
    // Count queries
    Long countByTicket(Ticket ticket);
    Long countByCommenter(User commenter);
    
    // Recent comments
    @Query("SELECT c FROM Comment c ORDER BY c.createTime DESC")
    Page<Comment> findRecentComments(Pageable pageable);
    
    // Comments created after a specific date
    @Query("SELECT c FROM Comment c WHERE c.createTime > :since ORDER BY c.createTime DESC")
    List<Comment> findCommentsCreatedAfter(@Param("since") ZonedDateTime since);
    
    // Top commenters (users with most comments)
    @Query("SELECT c.commenter, COUNT(c) FROM Comment c GROUP BY c.commenter ORDER BY COUNT(c) DESC")
    List<Object[]> findTopCommenters(Pageable pageable);
    
    // Comments with content longer than specified length
    @Query("SELECT c FROM Comment c WHERE LENGTH(c.content) > :minLength")
    List<Comment> findLongComments(@Param("minLength") int minLength);
    
    // Average comment length
    @Query("SELECT AVG(LENGTH(c.content)) FROM Comment c")
    Double getAverageCommentLength();
}