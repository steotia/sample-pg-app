package com.trials.crdb.app.model;

import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.Set;

@Entity
@Table(name = "sprints")
@Getter
@Setter
public class Sprint {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(nullable = false)
    private String name;
    
    @Column(columnDefinition = "text")
    private String description;
    
    @Column(nullable = false)
    private ZonedDateTime startDate;
    
    @Column(nullable = false)
    private ZonedDateTime endDate;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "project_id", nullable = false,
                foreignKey = @ForeignKey(name = "fk_sprint_project", 
                            foreignKeyDefinition = "FOREIGN KEY (project_id) REFERENCES projects(id) ON UPDATE CASCADE"))
    private Project project;
    
    @ManyToMany
    @JoinTable(
        name = "sprint_tickets",
        joinColumns = @JoinColumn(name = "sprint_id"),
        inverseJoinColumns = @JoinColumn(name = "ticket_id")
    )
    private Set<Ticket> tickets = new HashSet<>();
    
    @CreationTimestamp
    @Column(updatable = false, nullable = false)
    private ZonedDateTime createTime;
    
    @UpdateTimestamp
    @Column
    private ZonedDateTime updateTime;
    
    // Default constructor
    public Sprint() {}
    
    // Constructor with essential fields
    public Sprint(String name, String description, ZonedDateTime startDate, ZonedDateTime endDate, Project project) {
        this.name = name;
        this.description = description;
        this.startDate = startDate;
        this.endDate = endDate;
        this.project = project;
    }
    
    // Helper methods for ticket management
    public void addTicket(Ticket ticket) {
        this.tickets.add(ticket);
        ticket.getSprints().add(this);
    }
    
    public void removeTicket(Ticket ticket) {
        this.tickets.remove(ticket);
        ticket.getSprints().remove(this);
    }
}