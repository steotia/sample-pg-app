package com.trials.crdb.app.model;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import java.io.Serializable;
import java.util.Objects;

@Entity
@Table(name = "user_project_roles")
@Getter  
@Setter
public class UserProjectRole {
    
    @EmbeddedId
    private UserProjectRoleId id;
    
    @ManyToOne
    @MapsId("userId")
    @JoinColumn(name = "user_id", 
               foreignKey = @ForeignKey(name = "fk_user_role_user",
                          foreignKeyDefinition = "FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT"))
    private User user;
    
    @ManyToOne
    @MapsId("projectId")
    @JoinColumn(name = "project_id", 
               foreignKey = @ForeignKey(name = "fk_user_role_project",
                          foreignKeyDefinition = "FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE RESTRICT"))
    private Project project;
    
    @Column(nullable = false)
    private String roleName;
    
    // Default constructor
    public UserProjectRole() {}
    
    // Constructor
    public UserProjectRole(User user, Project project, String roleName) {
        this.id = new UserProjectRoleId(user.getId(), project.getId());
        this.user = user;
        this.project = project;
        this.roleName = roleName;
    }
    
    // Manual getters and setters if not using Lombok
    public String getRoleName() {
        return roleName;
    }
    
    public void setRoleName(String roleName) {
        this.roleName = roleName;
    }
    
    public UserProjectRoleId getId() {
        return id;
    }
    
    public void setId(UserProjectRoleId id) {
        this.id = id;
    }
    
    public User getUser() {
        return user;
    }
    
    public void setUser(User user) {
        this.user = user;
    }
    
    public Project getProject() {
        return project;
    }
    
    public void setProject(Project project) {
        this.project = project;
    }
    
    @Embeddable
    @Getter
    @Setter
    public static class UserProjectRoleId implements Serializable {
        private Long userId;
        private Long projectId;
        
        public UserProjectRoleId() {}
        
        public UserProjectRoleId(Long userId, Long projectId) {
            this.userId = userId;
            this.projectId = projectId;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            UserProjectRoleId that = (UserProjectRoleId) o;
            return Objects.equals(userId, that.userId) && 
                Objects.equals(projectId, that.projectId);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(userId, projectId);
        }
    }
}