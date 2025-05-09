package com.trials.crdb.app.repositories;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.trials.crdb.app.model.User;
import com.trials.crdb.app.model.Project;

import java.util.List;
import java.util.Optional;

@Repository
public interface UserRepository extends CrudRepository<User, Long> {
    
    // Find by username
    Optional<User> findByUsername(String username);
    
    // Find by email
    Optional<User> findByEmail(String email);
    
    // Find users by project
    @Query("SELECT u FROM User u JOIN u.projects p WHERE p.id = :projectId")
    List<User> findByProjectId(@Param("projectId") Long projectId);
    
    // Find users with a specific project name
    @Query("SELECT u FROM User u JOIN u.projects p WHERE p.name = :projectName")
    List<User> findByProjectName(@Param("projectName") String projectName);
    
    // Count users in a specific project
    @Query("SELECT COUNT(u) FROM User u JOIN u.projects p WHERE p.id = :projectId")
    Long countUsersByProjectId(@Param("projectId") Long projectId);
}