package com.elijahzhao24.NBAsalary_backend.player;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface PlayerRepository extends JpaRepository<Player, Integer> {

    // Query players by team code, ordered by salary ascending
    // SELECT p.*
    // FROM players p
    // WHERE p.team = ?
    // ORDER BY p.salary Desc;
    List<Player> findByTeamOrderBySalaryDesc(String teamCode);

    // teamEntity → the ManyToOne association to Team

    // Alternatively, by Team entity
    // SELECT p.*
    // FROM players p
    // JOIN teams t ON p.team_id = t.id
    // WHERE t.code = ?
    // ORDER BY p.salary Desc;
    List<Player> findByTeamEntity_CodeOrderBySalaryDesc(String teamCode);

    // Alternatively, by Team entity
    // SELECT p.*
    // FROM players p
    // JOIN teams t ON p.team_id = t.id
    // WHERE t.name = ?
    // ORDER BY p.salary Desc;
    List<Player> findByTeamEntity_NameOrderBySalaryDesc(String teamName);

    // find all ordered by salary Desc
    List<Player> findAllByOrderBySalaryDesc();
}
