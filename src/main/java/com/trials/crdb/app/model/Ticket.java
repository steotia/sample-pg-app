package com.trials.crdb.app.model;

import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import java.time.Duration;
import java.time.ZonedDateTime;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import com.trials.crdb.app.utils.DateTimeProvider;

@Entity
@Table(name = "tickets")
@Getter
@Setter
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
    @JoinColumn(name = "assignee_id")
    private User assignee;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "reporter_id", nullable = false)
    private User reporter;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "project_id", nullable = false)
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

    // Add these fields to the Ticket class
    @Column
    private ZonedDateTime dueDate;

    @Column
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

}