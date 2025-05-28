package com.trials.crdb.app.model;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

@Entity
@Table(name = "user_project_roles",
       uniqueConstraints = @UniqueConstraint(columnNames = {"user_id", "project_id"}))
@Getter  
@Setter
public class UserProjectRole {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @ManyToOne
    @JoinColumn(name = "user_id", nullable = false)
    private User user;
    
    @ManyToOne
    @JoinColumn(name = "project_id", nullable = false)
    private Project project;
    
    @Column(nullable = false)
    private String roleName;
    
    // Default constructor
    public UserProjectRole() {}
    
    // Constructor
    public UserProjectRole(User user, Project project, String roleName) {
        this.user = user;
        this.project = project;
        this.roleName = roleName;
    }
}