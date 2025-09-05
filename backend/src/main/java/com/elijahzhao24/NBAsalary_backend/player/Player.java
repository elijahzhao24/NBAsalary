package com.elijahzhao24.NBAsalary_backend.player;

import com.elijahzhao24.NBAsalary_backend.team.Team;
import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.persistence.*;
import java.time.OffsetDateTime;

@Entity
@Table(name = "players",
        uniqueConstraints = {
                @UniqueConstraint(columnNames = {"site_player_id", "year"})
        })

public class Player {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Column(name = "site_player_id")
    private Integer sitePlayerId;

    @Column(nullable = false)
    private String name;

    // three-letter code
    @Column(nullable = false)
    private String team;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "team_id", nullable = false) // names foreign key column in players
    @JsonIgnore
    private Team teamEntity;

    @Column(nullable = false)
    private Integer year;

    private Long salary;

    @Column(name = "row_hash")
    private String rowHash;

    @Column(name = "last_scrape")
    private OffsetDateTime lastScrape;

    @Column(name = "headshot_url")
    private String headshotUrl;

    public String getHeadshotUrl() {
        return headshotUrl;
    }

    public void setHeadshotUrl(String headshotUrl) {
        this.headshotUrl = headshotUrl;
    }

    public Integer getId() {
        return id;
    }

    public String getRowHash() {
        return rowHash;
    }

    public OffsetDateTime getLastScrape() {
        return lastScrape;
    }

    public Long getSalary() {
        return salary;
    }

    public Integer getYear() {
        return year;
    }

    public Team getTeamEntity() {
        return teamEntity;
    }

    public String getTeam() {
        return team;
    }

    public String getName() {
        return name;
    }

    public Integer getSitePlayerId() {
        return sitePlayerId;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public void setSitePlayerId(Integer sitePlayerId) {
        this.sitePlayerId = sitePlayerId;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setTeam(String team) {
        this.team = team;
    }

    public void setTeamEntity(Team teamEntity) {
        this.teamEntity = teamEntity;
    }

    public void setSalary(Long salary) {
        this.salary = salary;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public void setRowHash(String rowHash) {
        this.rowHash = rowHash;
    }

    public void setLastScrape(OffsetDateTime lastScrape) {
        this.lastScrape = lastScrape;
    }
}
