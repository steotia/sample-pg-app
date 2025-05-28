package com.trials.crdb.app.repositories;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.trials.crdb.app.model.User;
import com.trials.crdb.app.model.Project;
import com.trials.crdb.app.model.UserProjectRole;

@Repository
public interface UserProjectRoleRepository extends JpaRepository<UserProjectRole, Long> {
    
    // Find by user and project (replaces composite key lookup)
    Optional<UserProjectRole> findByUserAndProject(User user, Project project);
    
    // Find roles by user
    List<UserProjectRole> findByUser(User user);
    
    // Find roles by project
    List<UserProjectRole> findByProject(Project project);
}