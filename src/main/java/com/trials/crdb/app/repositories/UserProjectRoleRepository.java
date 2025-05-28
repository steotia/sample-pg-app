// src/main/java/com/trials/crdb/app/repositories/UserProjectRoleRepository.java
package com.trials.crdb.app.repositories;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.trials.crdb.app.model.UserProjectRole;
import com.trials.crdb.app.model.UserProjectRole.UserProjectRoleId;

@Repository
public interface UserProjectRoleRepository extends JpaRepository<UserProjectRole, UserProjectRoleId> {
}