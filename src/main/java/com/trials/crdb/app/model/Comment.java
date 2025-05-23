package com.trials.crdb.app.model;

import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.ZonedDateTime;

@Entity
@Table(name = "comments")
@Getter
@Setter
public class Comment {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(columnDefinition = "text", nullable = false)
    private String content;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "ticket_id", nullable = false)
    private Ticket ticket;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "commenter_id", nullable = false)
    private User commenter;

    @CreationTimestamp
    @Column(updatable = false, nullable = false)
    private ZonedDateTime createTime;

    @UpdateTimestamp
    @Column
    private ZonedDateTime updateTime;

    // Default constructor
    public Comment() {}

    // Constructor with essential fields
    public Comment(String content, Ticket ticket, User commenter) {
        this.content = content;
        this.ticket = ticket;
        this.commenter = commenter;
    }

    // Helper method to get content preview
    public String getContentPreview(int maxLength) {
        if (content == null) return null;
        return content.length() <= maxLength ? content : content.substring(0, maxLength) + "...";
    }
}