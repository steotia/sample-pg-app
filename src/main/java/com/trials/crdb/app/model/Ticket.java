package com.trials.crdb.app.model;

import org.hibernate.annotations.Check;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.time.Duration;
import java.time.ZonedDateTime;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import com.trials.crdb.app.utils.DateTimeProvider;

@Entity
@Table(name = "tickets")
@Getter
@Setter
// @Check(constraints = "estimated_hours >= 0 AND (due_date IS NULL OR due_date > create_time)")
public class Ticket {

    public enum TicketStatus {
        OPEN, IN_PROGRESS, REVIEW, RESOLVED, CLOSED
    }

    public enum TicketPriority {
        LOW, MEDIUM, HIGH, CRITICAL
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String title;

    @Column(columnDefinition = "text")
    private String description;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private TicketStatus status = TicketStatus.OPEN;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private TicketPriority priority = TicketPriority.MEDIUM;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private Map<String, Object> metadata = new HashMap<>();

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "assignee_id", 
            foreignKey = @ForeignKey(name = "fk_ticket_assignee",
                       foreignKeyDefinition = "FOREIGN KEY (assignee_id) REFERENCES users(id) ON DELETE SET NULL"))
    private User assignee;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "reporter_id", nullable = false)
    private User reporter;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "project_id", nullable = false,
                foreignKey = @ForeignKey(name = "fk_ticket_project", 
                        foreignKeyDefinition = "FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE RESTRICT"))
    private Project project;

    @CreationTimestamp
    @Column(updatable = false, nullable = false)
    private ZonedDateTime createTime;

    @UpdateTimestamp
    @Column
    private ZonedDateTime updateTime;

    // Constructors
    public Ticket() {}

    public Ticket(String title, String description, User reporter, Project project) {
        this.title = title;
        this.description = description;
        this.reporter = reporter;
        this.project = project;
    }

    // Helper methods
    public void assignTo(User user) {
        this.assignee = user;
    }

    public void setMetadataValue(String key, Object value) {
        if (this.metadata == null) {
            this.metadata = new HashMap<>();
        }
        this.metadata.put(key, value);
    }

    public Object getMetadataValue(String key) {
        return this.metadata != null ? this.metadata.get(key) : null;
    }

    // PHASE - add temporal fields

    @Column
    @Check(constraints = "due_date IS NULL OR due_date > create_time")
    private ZonedDateTime dueDate;

    @Column
    @Check(constraints = "estimated_hours >= 0")
    private Double estimatedHours;

    @Column
    private ZonedDateTime resolvedDate;

    // Getters and setters
    public ZonedDateTime getDueDate() {
        return dueDate;
    }

    public void setDueDate(ZonedDateTime dueDate) {
        this.dueDate = dueDate;
    }

    public Double getEstimatedHours() {
        return estimatedHours;
    }

    public void setEstimatedHours(Double estimatedHours) {
        this.estimatedHours = estimatedHours;
    }

    public ZonedDateTime getResolvedDate() {
        return resolvedDate;
    }

    public void setResolvedDate(ZonedDateTime resolvedDate) {
        this.resolvedDate = resolvedDate;
    }

    public Duration getResolutionTime() {
        if (createTime == null || resolvedDate == null) {
            return null;
        }
        return Duration.between(createTime, resolvedDate);
    }

    // Then update the methods:
    public boolean isOverdue() {
        if (dueDate == null || status == TicketStatus.RESOLVED || status == TicketStatus.CLOSED) {
            return false;
        }
        return DateTimeProvider.now().isAfter(dueDate);
    }

    public void resolve() {
        this.status = TicketStatus.RESOLVED;
        this.resolvedDate = DateTimeProvider.now();
    }

    @OneToMany(mappedBy = "ticket", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    private Set<Comment> comments = new HashSet<>();

    // Add helper methods
    public void addComment(Comment comment) {
        this.comments.add(comment);
        comment.setTicket(this);
    }

    public void removeComment(Comment comment) {
        this.comments.remove(comment);
        comment.setTicket(null);
    }

    public int getCommentCount() {
        return comments != null ? comments.size() : 0;
    }

    // Phase

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "dependent_on_id")
    private Ticket dependentOn;

    @OneToMany(mappedBy = "dependentOn")
    private Set<Ticket> dependencies = new HashSet<>();

    // Native array storage for tags - PostgreSQL specific
    // This will need compatibility testing with other databases
    @Column(columnDefinition = "text[]")
    private String[] tags;

    // Getters and setters
    public Ticket getDependentOn() {
        return dependentOn;
    }

    public void setDependentOn(Ticket dependentOn) {
        this.dependentOn = dependentOn;
    }

    public Set<Ticket> getDependencies() {
        return dependencies;
    }

    public String[] getTags() {
        return tags;
    }

    public void setTags(String[] tags) {
        this.tags = tags;
    }

    // Helper method to add a dependency
    public void addDependency(Ticket dependency) {
        dependency.setDependentOn(this);
        this.dependencies.add(dependency);
    }
    // Transaction & Concurrency Control
    @Version
    @Column(name = "version", nullable = false)
    private Long version = 0L;

    // Add getter and setter
    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    @PreUpdate
    public void preUpdate() {
        System.out.println("About to update ticket " + id + ", current version: " + version);
    }

    @PostUpdate  
    public void postUpdate() {
        System.out.println("Updated ticket " + id + ", new version: " + version);
    }

    // Phase - FK - constraints

    // Add relationship with Sprint
    @ManyToMany(mappedBy = "tickets")
    private Set<Sprint> sprints = new HashSet<>();

    // Add relationship with WorkLog
    @OneToMany(mappedBy = "ticket", cascade = CascadeType.ALL, orphanRemoval = true)
    private Set<WorkLog> workLogs = new HashSet<>();

    // Add helper methods for WorkLog
    public void addWorkLog(WorkLog workLog) {
        this.workLogs.add(workLog);
        workLog.setTicket(this);
    }

    public void removeWorkLog(WorkLog workLog) {
        this.workLogs.remove(workLog);
        workLog.setTicket(null);
    }

    // Add getter/setter for sprints
    public Set<Sprint> getSprints() {
        return sprints;
    }

    public void setSprints(Set<Sprint> sprints) {
        this.sprints = sprints;
    }

    // Add getter/setter for workLogs
    public Set<WorkLog> getWorkLogs() {
        return workLogs;
    }

    public void setWorkLogs(Set<WorkLog> workLogs) {
        this.workLogs = workLogs;
    }

}