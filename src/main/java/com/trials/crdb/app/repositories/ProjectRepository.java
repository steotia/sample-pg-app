package com.trials.crdb.app.repositories;

import java.util.Optional;

import org.springframework.data.repository.CrudRepository;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Repository;

import com.trials.crdb.app.model.Project;

@Repository
public interface ProjectRepository extends CrudRepository<Project, Long> {

    Optional<Project> findByName(@NonNull String name);

}