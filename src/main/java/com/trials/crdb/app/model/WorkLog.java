package com.trials.crdb.app.model;

import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.ZonedDateTime;

@Entity
@Table(name = "work_logs")
@Getter
@Setter
public class WorkLog {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "ticket_id", nullable = false)
    private Ticket ticket;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id", nullable = false)
    private User user;
    
    @Column(nullable = false)
    private ZonedDateTime startTime;
    
    @Column(nullable = false)
    private ZonedDateTime endTime;
    
    @Column(columnDefinition = "text", nullable = false)
    private String description;
    
    @Column(nullable = false)
    private Double hoursSpent;
    
    @CreationTimestamp
    @Column(updatable = false, nullable = false)
    private ZonedDateTime createTime;
    
    @UpdateTimestamp
    @Column
    private ZonedDateTime updateTime;
    
    // Default constructor
    public WorkLog() {}
    
    // Constructor with essential fields
    public WorkLog(Ticket ticket, User user, ZonedDateTime startTime, ZonedDateTime endTime, String description, Double hoursSpent) {
        this.ticket = ticket;
        this.user = user;
        this.startTime = startTime;
        this.endTime = endTime;
        this.description = description;
        this.hoursSpent = hoursSpent;
    }
}