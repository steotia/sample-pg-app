package com.trials.crdb.app.repositories;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.trials.crdb.app.model.WorkLog;
import com.trials.crdb.app.model.Ticket;
import com.trials.crdb.app.model.User;

import java.time.ZonedDateTime;
import java.util.List;

@Repository
public interface WorkLogRepository extends JpaRepository<WorkLog, Long> {
    
    List<WorkLog> findByTicket(Ticket ticket);
    
    List<WorkLog> findByUser(User user);
    
    @Query("SELECT SUM(w.hoursSpent) FROM WorkLog w WHERE w.ticket = :ticket")
    Double getTotalHoursForTicket(@Param("ticket") Ticket ticket);
    
    @Query("SELECT w FROM WorkLog w WHERE w.user = :user AND " +
           "w.startTime >= :start AND w.endTime <= :end")
    List<WorkLog> findWorkLogsByUserAndTimeRange(
        @Param("user") User user,
        @Param("start") ZonedDateTime start,
        @Param("end") ZonedDateTime end
    );
    
    @Query("SELECT w FROM WorkLog w WHERE w.user = :user AND " +
           "((w.startTime <= :endTime AND w.endTime >= :startTime) OR " +
           "(w.startTime >= :startTime AND w.startTime <= :endTime) OR " +
           "(w.endTime >= :startTime AND w.endTime <= :endTime))")
    List<WorkLog> findOverlappingWorkLogs(
        @Param("user") User user,
        @Param("startTime") ZonedDateTime startTime,
        @Param("endTime") ZonedDateTime endTime
    );
}