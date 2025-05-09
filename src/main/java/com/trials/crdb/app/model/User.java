package com.trials.crdb.app.model;

import org.hibernate.annotations.CreationTimestamp;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.Set;

@Entity
@Table(name = "users")
@Getter
@Setter
public class User {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(unique = true, nullable = false)
    private String username;

    @Column(nullable = false)
    private String email;

    @Column
    private String fullName;

    // Many-to-many relationship with Project
    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
        name = "user_projects", 
        joinColumns = @JoinColumn(name = "user_id"),
        inverseJoinColumns = @JoinColumn(name = "project_id")
    )
    private Set<Project> projects = new HashSet<>();

    @CreationTimestamp
    @Column(updatable = false, nullable = false)
    private ZonedDateTime createTime;

    // Constructors
    public User() {}

    public User(String username, String email, String fullName) {
        this.username = username;
        this.email = email;
        this.fullName = fullName;
    }

    // Helper methods
    public void addProject(Project project) {
        this.projects.add(project);
        project.getUsers().add(this);
    }

    public void removeProject(Project project) {
        this.projects.remove(project);
        project.getUsers().remove(this);
    }
}