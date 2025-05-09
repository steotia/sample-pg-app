package com.trials.crdb.app.model;

import org.hibernate.annotations.CreationTimestamp;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

import java.util.HashSet;
import java.util.Set;

import java.time.ZonedDateTime;

@Getter
@Setter
@Entity
@Table(name = "projects")
public class Project {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    // This conflicts with Hibernate unless we do spring.jpa.properties.hibernate.id.new_generator_mappings=false
    // @Column(updatable = false, nullable = false, columnDefinition = "SERIAL")
    // using the modern approach
    @Column(updatable = false, nullable = false)
    // TOIL both
    // even CRDB returns large values > INTEGER
    private Long id;

    @Column(unique = true, nullable = false)
    private String name;

    @Column(columnDefinition = "text")
    private String description;

    @CreationTimestamp
    @Column(updatable = false, nullable = false)
    private ZonedDateTime createTime;

    @ManyToMany(mappedBy = "projects", fetch = FetchType.LAZY)
    private Set<User> users = new HashSet<>();

    public Project(){}

    public Project(String name, String description){
        this.name = name;
        this.description = description;
    }
}
