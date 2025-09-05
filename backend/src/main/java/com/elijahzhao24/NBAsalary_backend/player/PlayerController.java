package com.elijahzhao24.NBAsalary_backend.player;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/players")
public class PlayerController {

    private final PlayerService playerService;

    @Autowired
    public PlayerController(PlayerService playerService) {
        this.playerService = playerService;
    }

    @GetMapping("/team")
    public List<Player> getPlayersByTeam(
            @RequestParam(required = false) String code,
            @RequestParam(required = false) String name
    ) {
        if (code != null) {
            return playerService.getPlayersByTeamCode(code);
        } else if (name != null) {
            return playerService.getPlayersByTeamName(name);
        } else {
            throw new IllegalArgumentException("Must provide either code or name");
        }
    }

    @GetMapping()
    public List<Player> getPlayers() {
        return playerService.getPlayers();
    }

    @GetMapping("/search")
    public List<Player> getPlayersBySearch(@RequestParam String name) {
        return playerService.getPlayersByName(name);
    }
}