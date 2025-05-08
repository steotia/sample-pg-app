package com.trials.crdb.app.repositories;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import com.trials.crdb.app.model.Project;

@Repository
public interface ProjectRepository extends CrudRepository<Project, Integer> {

}
