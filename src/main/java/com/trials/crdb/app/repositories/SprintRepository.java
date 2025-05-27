package com.trials.crdb.app.repositories;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.trials.crdb.app.model.Sprint;
import com.trials.crdb.app.model.Project;

import java.time.ZonedDateTime;
import java.util.List;

@Repository
public interface SprintRepository extends JpaRepository<Sprint, Long> {
    
    List<Sprint> findByProject(Project project);
    
    @Query("SELECT s FROM Sprint s WHERE s.project = :project AND s.endDate > CURRENT_TIMESTAMP")
    List<Sprint> findActiveSprintsByProject(@Param("project") Project project);
    
    @Query("SELECT s FROM Sprint s WHERE s.startDate <= :date AND s.endDate >= :date")
    List<Sprint> findSprintsByDate(@Param("date") ZonedDateTime date);
    
    @Query("SELECT s FROM Sprint s WHERE s.project = :project AND " +
           "((s.startDate <= :endDate AND s.endDate >= :startDate) OR " +
           "(s.startDate >= :startDate AND s.startDate <= :endDate) OR " +
           "(s.endDate >= :startDate AND s.endDate <= :endDate))")
    List<Sprint> findOverlappingSprints(
        @Param("project") Project project,
        @Param("startDate") ZonedDateTime startDate,
        @Param("endDate") ZonedDateTime endDate
    );
}