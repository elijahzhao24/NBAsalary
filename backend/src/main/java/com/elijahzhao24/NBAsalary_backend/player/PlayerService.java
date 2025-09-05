package com.elijahzhao24.NBAsalary_backend.player;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class PlayerService {

    private final PlayerRepository playerRepository;

    @Autowired
    public PlayerService(PlayerRepository playerRepository) {
        this.playerRepository = playerRepository;
    }

    public List<Player> getPlayersByPlayerTeamCode(String teamCode) {
        return playerRepository.findByTeamOrderBySalaryDesc(teamCode);
    }

    public List<Player> getPlayersByTeamCode(String teamCode) {
        return playerRepository.findByTeamEntity_CodeOrderBySalaryDesc(teamCode);
    }

    public List<Player> getPlayersByTeamName(String teamName) {
        return playerRepository.findByTeamEntity_NameOrderBySalaryDesc(teamName);
    }

    // Get all players, and order by salary
    public List<Player> getPlayers() {
        return playerRepository.findAllByOrderBySalaryDesc();
    }

    // Get all players by search
    public List<Player> getPlayersByName(String SearchText) {
        return playerRepository.findAllByOrderBySalaryDesc().stream().filter(player ->
                player.getName().toLowerCase().contains(SearchText.toLowerCase()))
                .collect(Collectors.toList());
    }

}
